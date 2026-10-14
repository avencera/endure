use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::{Arc, Mutex};

use endure::{Durable, DurableCell, DurableMap, DurableVec, Error, UpdateError};
use serde::{Deserialize, Serialize};
use tempfile::TempDir;

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
struct Settings {
    max_parallel: u32,
    theme: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
struct Task {
    name: String,
    done: bool,
}

#[derive(Durable)]
struct App {
    tasks: DurableMap<u64, Task>,
    settings: DurableCell<Settings>,
    history: DurableVec<String>,
    #[durable(skip)]
    scratch: Vec<u8>,
}

fn open_at(dir: &TempDir) -> Durable<App> {
    App::builder().dir(dir.path()).open().expect("open state")
}

#[tokio::test]
async fn changes_survive_reopen() -> eyre::Result<()> {
    let dir = TempDir::new()?;

    {
        let app = open_at(&dir);
        app.update(|s| {
            s.tasks.insert(
                1,
                Task {
                    name: "build".into(),
                    done: false,
                },
            );
            s.tasks.insert(
                2,
                Task {
                    name: "test".into(),
                    done: false,
                },
            );
            s.settings.max_parallel = 4;
            s.history.push("created".into());
            s.scratch.push(7);
        })
        .await?;

        app.update(|s| {
            s.tasks.get_mut(&1).expect("task 1").done = true;
            s.tasks.remove(&2);
            s.history.push("finished build".into());
        })
        .await?;

        assert_eq!(app.read().scratch, vec![7]);
    }

    let app = open_at(&dir);
    let state = app.read();
    assert_eq!(state.tasks.len(), 1);
    assert_eq!(
        state.tasks[&1],
        Task {
            name: "build".into(),
            done: true
        }
    );
    assert_eq!(state.settings.max_parallel, 4);
    assert_eq!(
        state.history.iter().cloned().collect::<Vec<_>>(),
        ["created", "finished build"]
    );
    assert!(state.scratch.is_empty(), "skipped fields are memory only");
    Ok(())
}

#[tokio::test]
async fn rejected_update_changes_nothing() -> eyre::Result<()> {
    let dir = TempDir::new()?;
    let app = open_at(&dir);
    app.update(|s| s.settings.max_parallel = 1).await?;

    let result = app
        .try_update(|s| {
            s.settings.max_parallel = 99;
            s.tasks.insert(
                5,
                Task {
                    name: "half done".into(),
                    done: false,
                },
            );
            Err::<(), _>("not allowed")
        })
        .await;

    assert!(matches!(result, Err(UpdateError::Rejected("not allowed"))));
    assert_eq!(app.read().settings.max_parallel, 1);
    assert!(app.read().tasks.is_empty());

    drop(app);
    let app = open_at(&dir);
    assert_eq!(app.read().settings.max_parallel, 1);
    assert!(app.read().tasks.is_empty());
    Ok(())
}

#[test]
fn panicking_update_changes_nothing() -> eyre::Result<()> {
    let dir = TempDir::new()?;
    let app = open_at(&dir);
    app.update_blocking(|s| s.history.push("before".into()))?;

    let outcome = catch_unwind(AssertUnwindSafe(|| {
        app.update_blocking(|s| {
            s.history.push("lost".into());
            panic!("boom");
        })
    }));

    assert!(outcome.is_err());
    assert_eq!(app.read().history.len(), 1);

    // the handle stays usable after a panic inside an update
    app.update_blocking(|s| s.history.push("after".into()))?;
    drop(app);

    let app = open_at(&dir);
    assert_eq!(
        app.read().history.iter().cloned().collect::<Vec<_>>(),
        ["before", "after"]
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_updates_apply_in_order() -> eyre::Result<()> {
    #[derive(Durable)]
    struct Counter {
        count: DurableCell<u64>,
        log: DurableVec<u64>,
    }

    let dir = TempDir::new()?;
    let counter = Counter::builder().dir(dir.path()).open()?;

    let mut tasks = Vec::new();
    for _ in 0..50 {
        let counter = counter.clone();
        tasks.push(tokio::spawn(async move {
            counter
                .update(|s| {
                    *s.count += 1;
                    let value = *s.count.get();
                    s.log.push(value);
                })
                .await
        }));
    }

    for task in tasks {
        task.await??;
    }

    drop(counter);
    let counter = Counter::builder().dir(dir.path()).open()?;
    let state = counter.read();
    assert_eq!(*state.count.get(), 50);
    assert_eq!(
        state.log.iter().copied().collect::<Vec<_>>(),
        (1..=50).collect::<Vec<_>>()
    );
    Ok(())
}

#[test]
fn second_open_of_the_same_file_fails() -> eyre::Result<()> {
    let dir = TempDir::new()?;
    let _first = open_at(&dir);
    let second = App::builder().dir(dir.path()).open();
    assert!(matches!(second, Err(Error::Storage(_))), "got {second:?}");
    Ok(())
}

#[tokio::test]
async fn commits_notify_subscribers_and_hooks() -> eyre::Result<()> {
    let app = App::memory();
    let seen = Arc::new(Mutex::new(Vec::new()));

    let hook_seen = Arc::clone(&seen);
    app.on_commit(move |commit| hook_seen.lock().unwrap().push(commit.changed.to_vec()));

    let mut commits = app.subscribe();
    app.update(|s| s.settings.theme = "dark".into()).await?;

    commits.changed().await?;
    let commit = commits.borrow_and_update().clone();
    assert!(commit.changed("settings"));
    assert!(!commit.changed("tasks"));
    assert_eq!(commit.snapshot.settings.theme, "dark");

    // an update that only touches memory-only fields commits nothing and notifies nobody
    app.update(|s| s.scratch.push(1)).await?;
    app.update(|s| s.history.push("x".into())).await?;

    assert_eq!(
        *seen.lock().unwrap(),
        vec![vec!["settings"], vec!["history"]]
    );
    Ok(())
}

#[test]
fn snapshots_are_isolated_from_later_updates() -> eyre::Result<()> {
    let app = App::memory();
    app.update_blocking(|s| s.history.push("one".into()))?;

    let before = app.read();
    app.update_blocking(|s| s.history.push("two".into()))?;

    assert_eq!(before.history.len(), 1);
    assert_eq!(app.read().history.len(), 2);
    Ok(())
}

#[test]
fn destroy_deletes_the_file_only_when_unshared() -> eyre::Result<()> {
    let dir = TempDir::new()?;
    let app = open_at(&dir);
    let path = app.path().expect("file backed").to_path_buf();
    assert!(path.exists());

    let other = app.clone();
    assert!(matches!(app.destroy(), Err(Error::InUse)));

    other.destroy()?;
    assert!(!path.exists());
    Ok(())
}

#[test]
fn default_location_uses_crate_and_struct_names() -> eyre::Result<()> {
    let Some(path) = App::builder().resolved_path().ok().flatten() else {
        // platforms without a state directory must set one explicitly
        return Ok(());
    };

    assert!(
        path.ends_with("endure/app.redb"),
        "unexpected default path {path:?}"
    );

    let named = App::builder()
        .dir("/tmp/x")
        .name("approvals")
        .resolved_path()?;
    assert_eq!(
        named.as_deref(),
        Some(std::path::Path::new("/tmp/x/approvals.redb"))
    );

    let exact = App::builder()
        .path("/tmp/y.redb")
        .dir("/ignored")
        .resolved_path()?;
    assert_eq!(exact.as_deref(), Some(std::path::Path::new("/tmp/y.redb")));
    Ok(())
}

#[test]
fn custom_backends_work() -> eyre::Result<()> {
    let app = App::builder()
        .backend(endure::redb::backends::InMemoryBackend::new())
        .open()?;
    assert!(app.path().is_none());

    app.update_blocking(|s| {
        s.tasks.insert(
            1,
            Task {
                name: "x".into(),
                done: false,
            },
        )
    })?;
    assert_eq!(app.read().tasks.len(), 1);
    Ok(())
}

#[test]
fn map_range_and_order() -> eyre::Result<()> {
    let app = App::memory();
    app.update_blocking(|s| {
        for id in [5, 1, 3, 9, 7] {
            s.tasks.insert(
                id,
                Task {
                    name: id.to_string(),
                    done: false,
                },
            );
        }
    })?;

    let state = app.read();
    assert_eq!(
        state.tasks.keys().copied().collect::<Vec<_>>(),
        [1, 3, 5, 7, 9]
    );
    assert_eq!(
        state
            .tasks
            .range(3..8)
            .map(|(id, _)| *id)
            .collect::<Vec<_>>(),
        [3, 5, 7]
    );
    assert_eq!(state.tasks.first().map(|(id, _)| *id), Some(1));
    assert_eq!(state.tasks.last().map(|(id, _)| *id), Some(9));
    Ok(())
}

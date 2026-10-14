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

#[derive(Clone, Durable)]
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
    #[derive(Clone, Durable)]
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancelled_update_still_commits_and_publishes() -> eyre::Result<()> {
    let dir = TempDir::new()?;
    let app = open_at(&dir);

    {
        // poll once so the commit is queued, then drop the future
        let update = app.update(|s| {
            s.tasks.insert(
                1,
                Task {
                    name: "cancelled caller".into(),
                    done: false,
                },
            );
        });
        tokio::pin!(update);
        let _ = futures_poll_once(update.as_mut()).await;
    }

    app.update(|s| {
        s.tasks.insert(
            2,
            Task {
                name: "next".into(),
                done: false,
            },
        );
    })
    .await?;

    let in_memory: Vec<u64> = app.read().tasks.keys().copied().collect();
    drop(app);

    let on_disk: Vec<u64> = open_at(&dir).read().tasks.keys().copied().collect();
    assert_eq!(
        in_memory, on_disk,
        "memory and disk must agree after a cancelled update"
    );
    assert!(on_disk.contains(&2));
    Ok(())
}

// polls a future exactly once and reports whether it finished
async fn futures_poll_once<F: std::future::Future>(future: std::pin::Pin<&mut F>) -> bool {
    let mut future = Some(future);
    std::future::poll_fn(move |cx| {
        let finished = future.take().expect("polled once").poll(cx).is_ready();
        std::task::Poll::Ready(finished)
    })
    .await
}

#[tokio::test]
async fn a_panicking_hook_does_not_fail_the_commit() -> eyre::Result<()> {
    let app = App::memory();
    let later_hook_ran = Arc::new(Mutex::new(0));

    app.on_commit(|_| panic!("hook bug"));
    let counter = Arc::clone(&later_hook_ran);
    app.on_commit(move |_| *counter.lock().unwrap() += 1);

    let commits = app.subscribe();
    app.update(|s| s.settings.max_parallel = 2).await?;

    assert!(
        commits.has_changed()?,
        "subscribers still hear about the commit"
    );
    assert_eq!(*later_hook_ran.lock().unwrap(), 1);
    assert_eq!(app.read().settings.max_parallel, 2);
    Ok(())
}

#[test]
fn hooks_can_register_hooks() -> eyre::Result<()> {
    let app = App::memory();
    let inner_app = app.clone();
    let registered = Arc::new(Mutex::new(false));

    let flag = Arc::clone(&registered);
    app.on_commit(move |_| {
        let mut flag = flag.lock().unwrap();
        if !*flag {
            *flag = true;
            inner_app.on_commit(|_| {});
        }
    });

    app.update_blocking(|s| s.history.push("one".into()))?;
    app.update_blocking(|s| s.history.push("two".into()))?;
    assert!(*registered.lock().unwrap());
    Ok(())
}

#[test]
fn blocking_and_async_updates_share_a_tiny_blocking_pool() -> eyre::Result<()> {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .max_blocking_threads(1)
        .enable_all()
        .build()?;

    runtime.block_on(async {
        let app = App::memory();

        let blocking = {
            let app = app.clone();
            tokio::task::spawn_blocking(move || {
                for index in 0..20 {
                    app.update_blocking(|s| s.history.push(format!("blocking {index}")))
                        .unwrap();
                }
            })
        };

        for index in 0..20 {
            app.update(|s| s.history.push(format!("async {index}")))
                .await?;
        }

        tokio::time::timeout(std::time::Duration::from_secs(10), blocking).await??;
        assert_eq!(app.read().history.len(), 40);
        eyre::Ok(())
    })
}

#[tokio::test]
async fn values_that_cannot_round_trip_are_refused() -> eyre::Result<()> {
    #[derive(Clone, Default, Serialize, Deserialize)]
    struct Item {
        name: String,
        // without `default`, an empty list would be skipped on write and missing on read
        #[serde(skip_serializing_if = "Vec::is_empty")]
        tags: Vec<String>,
    }

    #[derive(Clone, Durable)]
    struct Items {
        item: DurableCell<Item>,
    }

    let dir = TempDir::new()?;
    let items = Items::builder().dir(dir.path()).open()?;

    let result = items.update(|s| s.item.name = "x".into()).await;
    assert!(
        matches!(result, Err(Error::Encode { .. })),
        "got {result:?}"
    );
    assert_eq!(items.read().item.name, "");

    drop(items);
    let items = Items::builder().dir(dir.path()).open()?;
    assert_eq!(items.read().item.name, "");
    Ok(())
}

#[tokio::test]
async fn changes_that_cancel_out_write_nothing() -> eyre::Result<()> {
    let app = App::memory();
    let commits = app.subscribe();

    app.update(|s| {
        s.tasks.insert(
            9,
            Task {
                name: "temporary".into(),
                done: false,
            },
        );
        s.tasks.remove(&9);
        s.history.push("temporary".into());
        s.history.pop();
    })
    .await?;

    assert!(
        !commits.has_changed()?,
        "nothing reached storage, so nothing was announced"
    );
    Ok(())
}

#[test]
fn raw_identifier_fields_use_their_plain_names() -> eyre::Result<()> {
    #[derive(Clone, Durable)]
    struct Keywords {
        r#type: DurableCell<String>,
    }

    let state = Keywords::memory();
    let mut commits = state.subscribe();
    state.update_blocking(|s| s.r#type.set("kind".into()))?;

    assert!(commits.borrow_and_update().changed("type"));
    Ok(())
}

#[cfg(unix)]
#[test]
fn new_files_are_private() -> eyre::Result<()> {
    use std::os::unix::fs::PermissionsExt;

    let dir = TempDir::new()?;
    let app = App::builder().dir(dir.path().join("nested")).open()?;
    let path = app.path().expect("file backed");

    assert_eq!(std::fs::metadata(path)?.permissions().mode() & 0o777, 0o600);
    assert_eq!(
        std::fs::metadata(path.parent().unwrap())?
            .permissions()
            .mode()
            & 0o777,
        0o700
    );
    Ok(())
}

#[tokio::test]
async fn replacing_a_whole_collection_is_saved() -> eyre::Result<()> {
    let dir = TempDir::new()?;
    let app = open_at(&dir);
    app.update(|s| {
        s.tasks.insert(
            1,
            Task {
                name: "a".into(),
                done: false,
            },
        );
        s.tasks.insert(
            2,
            Task {
                name: "b".into(),
                done: false,
            },
        );
        s.history.push("one".into());
        s.settings.max_parallel = 3;
    })
    .await?;

    let before = app.read();
    app.update(|s| {
        s.tasks.insert(
            3,
            Task {
                name: "c".into(),
                done: false,
            },
        )
    })
    .await?;

    app.update(|s| {
        // a fresh map, a taken list, a new cell, and a revert to an older snapshot
        s.tasks = DurableMap::new();
        s.tasks.insert(
            9,
            Task {
                name: "fresh".into(),
                done: false,
            },
        );
        std::mem::take(&mut s.history);
        s.settings = DurableCell::new(Settings {
            max_parallel: 8,
            theme: "x".into(),
        });
    })
    .await?;

    app.update(|s| s.tasks = before.tasks.clone()).await?;

    let in_memory: Vec<u64> = app.read().tasks.keys().copied().collect();
    drop(app);

    let reopened = open_at(&dir);
    let state = reopened.read();
    assert_eq!(state.tasks.keys().copied().collect::<Vec<_>>(), in_memory);
    assert_eq!(in_memory, [1, 2]);
    assert!(state.history.is_empty());
    assert_eq!(state.settings.max_parallel, 8);
    Ok(())
}

#[tokio::test]
async fn hash_maps_inside_values_are_stored() -> eyre::Result<()> {
    #[derive(Clone, Durable)]
    struct Tagged {
        tags: DurableCell<std::collections::HashMap<String, u32>>,
    }

    let dir = TempDir::new()?;
    let state = Tagged::builder().dir(dir.path()).open()?;
    state
        .update(|s| {
            for index in 0..32 {
                s.tags.insert(format!("tag {index}"), index);
            }
        })
        .await?;

    drop(state);
    let state = Tagged::builder().dir(dir.path()).open()?;
    assert_eq!(state.read().tags.len(), 32);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn destroy_right_after_an_update_succeeds() -> eyre::Result<()> {
    for _ in 0..100 {
        let dir = TempDir::new()?;
        let app = open_at(&dir);
        app.update(|s| s.history.push("x".into())).await?;
        app.destroy()?;
    }

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn hooks_after_async_updates_can_spawn_tasks() -> eyre::Result<()> {
    let app = App::memory();
    let (sender, mut receiver) = tokio::sync::mpsc::unbounded_channel();

    app.on_commit(move |commit| {
        let sender = sender.clone();
        let changed = commit.changed.to_vec();
        tokio::spawn(async move {
            let _ = sender.send(changed);
        });
    });

    app.update(|s| s.history.push("x".into())).await?;
    let changed = tokio::time::timeout(std::time::Duration::from_secs(5), receiver.recv()).await?;
    assert_eq!(changed, Some(vec!["history"]));
    Ok(())
}

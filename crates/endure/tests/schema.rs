use endure::{Durable, DurableCell, DurableMap, DurableVec, Error};
use serde::{Deserialize, Serialize};
use tempfile::TempDir;

mod v1 {
    use super::*;

    #[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
    pub struct Approval {
        pub tool: String,
        pub granted: bool,
    }

    #[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
    pub struct Settings {
        pub max_parallel: u32,
    }

    #[derive(Clone, Durable)]
    pub struct App {
        pub approvals: DurableMap<u32, Approval>,
        pub settings: DurableCell<Settings>,
        pub notes: DurableVec<String>,
    }
}

mod v2 {
    use super::*;

    #[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
    pub enum State {
        Pending,
        Granted,
    }

    #[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
    pub struct Approval {
        pub tool: String,
        pub state: State,
    }

    impl From<v1::Approval> for Approval {
        fn from(old: v1::Approval) -> Self {
            let state = if old.granted {
                State::Granted
            } else {
                State::Pending
            };
            Self {
                tool: old.tool,
                state,
            }
        }
    }

    #[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
    pub struct Settings {
        pub max_parallel: u32,
        // added later; serde fills it in for values written before it existed
        #[serde(default)]
        pub theme: String,
    }

    #[derive(Clone, Durable)]
    pub struct App {
        #[durable(versions(v1::Approval = 1))]
        pub approvals: DurableMap<u32, Approval>,
        pub settings: DurableCell<Settings>,
        // renamed from `notes`; the table name keeps the old data
        #[durable(table = "notes")]
        pub journal: DurableVec<String>,
    }
}

mod v3 {
    use super::*;

    #[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
    pub struct Approval {
        pub tool: String,
        pub granted: bool,
        pub uses: u32,
    }

    impl From<v2::Approval> for Approval {
        fn from(old: v2::Approval) -> Self {
            Self {
                tool: old.tool,
                granted: old.state == v2::State::Granted,
                uses: 0,
            }
        }
    }

    #[derive(Clone, Durable)]
    pub struct App {
        #[durable(versions(v1::Approval = 1, v2::Approval = 2))]
        pub approvals: DurableMap<u32, Approval>,
    }
}

fn path(dir: &TempDir) -> std::path::PathBuf {
    dir.path().join("state.redb")
}

fn write_v1(dir: &TempDir) -> eyre::Result<()> {
    let app = v1::App::builder().path(path(dir)).open()?;
    app.update_blocking(|s| {
        s.approvals.insert(
            1,
            v1::Approval {
                tool: "deploy".into(),
                granted: true,
            },
        );
        s.approvals.insert(
            2,
            v1::Approval {
                tool: "delete".into(),
                granted: false,
            },
        );
        s.settings.max_parallel = 3;
        s.notes.push("first".into());
    })?;

    Ok(())
}

#[test]
fn values_upgrade_through_listed_versions() -> eyre::Result<()> {
    let dir = TempDir::new()?;
    write_v1(&dir)?;

    let app = v2::App::builder().path(path(&dir)).open()?;
    let state = app.read();
    assert_eq!(
        state.approvals[&1],
        v2::Approval {
            tool: "deploy".into(),
            state: v2::State::Granted
        }
    );
    assert_eq!(state.approvals[&2].state, v2::State::Pending);
    assert_eq!(state.settings.max_parallel, 3);
    assert_eq!(state.settings.theme, "");
    assert_eq!(state.journal.iter().cloned().collect::<Vec<_>>(), ["first"]);
    drop(state);
    drop(app);

    // the upgrade was written back, so a second open reads version 2 directly
    let app = v2::App::builder().path(path(&dir)).open()?;
    assert_eq!(app.read().approvals[&1].state, v2::State::Granted);
    Ok(())
}

#[test]
fn chains_skip_straight_from_the_oldest_version() -> eyre::Result<()> {
    let dir = TempDir::new()?;
    write_v1(&dir)?;

    let app = v3::App::builder().path(path(&dir)).open()?;
    let state = app.read();
    assert_eq!(
        state.approvals[&1],
        v3::Approval {
            tool: "deploy".into(),
            granted: true,
            uses: 0
        }
    );
    assert!(!state.approvals[&2].granted);
    Ok(())
}

#[test]
fn older_builds_refuse_newer_files() -> eyre::Result<()> {
    let dir = TempDir::new()?;
    write_v1(&dir)?;
    drop(v2::App::builder().path(path(&dir)).open()?);

    let result = v1::App::builder().path(path(&dir)).open();
    assert!(
        matches!(&result, Err(Error::NewerSchema { table, found: 2, supported: 1 }) if table == "approvals"),
        "got {result:?}"
    );
    Ok(())
}

mod v4 {
    use super::*;

    // the oldest version was dropped from the list once no stored data needed it
    #[derive(Clone, Durable)]
    pub struct App {
        #[durable(versions(v2::Approval = 2))]
        pub approvals: DurableMap<u32, v3::Approval>,
    }
}

#[test]
fn data_older_than_the_oldest_listed_version_is_refused() -> eyre::Result<()> {
    let dir = TempDir::new()?;
    write_v1(&dir)?;

    let result = v4::App::builder().path(path(&dir)).open();
    assert!(
        matches!(&result, Err(Error::OlderSchema { table, found: 1, oldest: 2 }) if table == "approvals"),
        "got {result:?}"
    );
    Ok(())
}

mod cells_and_lists {
    use super::*;

    #[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
    pub struct Limits {
        pub parallel: u64,
    }

    impl From<v1::Settings> for Limits {
        fn from(old: v1::Settings) -> Self {
            Self {
                parallel: u64::from(old.max_parallel),
            }
        }
    }

    #[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
    pub struct Note {
        pub text: String,
    }

    impl From<String> for Note {
        fn from(text: String) -> Self {
            Self { text }
        }
    }

    #[derive(Clone, Durable)]
    pub struct App {
        #[durable(versions(v1::Settings = 1))]
        pub settings: DurableCell<Limits>,
        #[durable(versions(String = 1))]
        pub notes: DurableVec<Note>,
    }
}

#[test]
fn cells_and_lists_upgrade_too() -> eyre::Result<()> {
    let dir = TempDir::new()?;
    write_v1(&dir)?;

    let app = cells_and_lists::App::builder().path(path(&dir)).open()?;
    let state = app.read();
    assert_eq!(state.settings.parallel, 3);
    assert_eq!(
        state.notes.get(0).map(|note| note.text.as_str()),
        Some("first")
    );
    Ok(())
}

mod v5 {
    use super::*;

    // every old entry dropped, with the current number kept explicitly
    #[derive(Clone, Durable)]
    pub struct App {
        #[durable(version = 3)]
        pub approvals: DurableMap<u32, v3::Approval>,
    }
}

#[test]
fn an_explicit_version_survives_dropping_every_old_entry() -> eyre::Result<()> {
    let dir = TempDir::new()?;
    write_v1(&dir)?;
    drop(v3::App::builder().path(path(&dir)).open()?);

    let app = v5::App::builder().path(path(&dir)).open()?;
    assert!(app.read().approvals[&1].granted);
    drop(app);

    // a file still at version 1 is refused rather than misread
    let older = TempDir::new()?;
    write_v1(&older)?;
    let result = v5::App::builder().path(path(&older)).open();
    assert!(
        matches!(
            result,
            Err(Error::OlderSchema {
                found: 1,
                oldest: 3,
                ..
            })
        ),
        "got {result:?}"
    );
    Ok(())
}

mod keys {
    use super::*;

    #[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
    pub struct Thin {
        pub id: u32,
    }

    #[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
    pub struct Wide {
        pub id: u32,
        #[serde(default)]
        pub tag: String,
    }

    #[derive(Clone, Durable)]
    pub struct ThinApp {
        pub items: DurableMap<Thin, String>,
    }

    #[derive(Clone, Durable)]
    pub struct WideApp {
        pub items: DurableMap<Wide, String>,
    }
}

fn wide(id: u32) -> keys::Wide {
    keys::Wide {
        id,
        tag: String::new(),
    }
}

#[test]
fn keys_stored_before_a_key_field_was_added_can_be_updated_and_removed() -> eyre::Result<()> {
    let dir = TempDir::new()?;
    {
        let app = keys::ThinApp::builder().path(path(&dir)).open()?;
        app.update_blocking(|s| {
            s.items.insert(keys::Thin { id: 1 }, "removed".into());
            s.items.insert(keys::Thin { id: 2 }, "old".into());
        })?;
    }

    {
        let app = keys::WideApp::builder().path(path(&dir)).open()?;
        app.update_blocking(|s| {
            s.items.remove(&wide(1));
            s.items.insert(wide(2), "new".into());
        })?;
    }

    let app = keys::WideApp::builder().path(path(&dir)).open()?;
    let items: Vec<_> = app
        .read()
        .items
        .iter()
        .map(|(k, v)| (k.id, v.clone()))
        .collect();
    assert_eq!(items, [(2, "new".to_owned())]);
    Ok(())
}

#[test]
fn keys_stored_before_a_key_field_was_removed_can_be_updated() -> eyre::Result<()> {
    let dir = TempDir::new()?;
    {
        let app = keys::WideApp::builder().path(path(&dir)).open()?;
        app.update_blocking(|s| {
            s.items.insert(
                keys::Wide {
                    id: 7,
                    tag: "x".into(),
                },
                "old".into(),
            );
        })?;
    }

    {
        let app = keys::ThinApp::builder().path(path(&dir)).open()?;
        app.update_blocking(|s| s.items.insert(keys::Thin { id: 7 }, "new".into()))?;
    }

    let app = keys::ThinApp::builder().path(path(&dir)).open()?;
    let items: Vec<_> = app
        .read()
        .items
        .iter()
        .map(|(k, v)| (k.id, v.clone()))
        .collect();
    assert_eq!(items, [(7, "new".to_owned())]);
    Ok(())
}

mod kinds {
    use super::*;

    #[derive(Clone, Durable)]
    pub struct AsList {
        pub history: DurableVec<String>,
    }

    #[derive(Clone, Durable)]
    pub struct AsCell {
        pub history: DurableCell<String>,
    }
}

#[test]
fn a_cell_refuses_to_open_over_a_list() -> eyre::Result<()> {
    let dir = TempDir::new()?;
    {
        let app = kinds::AsList::builder().path(path(&dir)).open()?;
        app.update_blocking(|s| s.history.push("kept".into()))?;
    }

    let result = kinds::AsCell::builder().path(path(&dir)).open();
    assert!(
        matches!(result, Err(Error::Decode { .. })),
        "got {result:?}"
    );

    // the list is untouched
    let app = kinds::AsList::builder().path(path(&dir)).open()?;
    assert_eq!(app.read().history.iter().collect::<Vec<_>>(), ["kept"]);
    Ok(())
}

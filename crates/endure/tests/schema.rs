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

    #[derive(Durable)]
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

    #[derive(Durable)]
    pub struct App {
        #[durable(versions(v1::Approval))]
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

    #[derive(Durable)]
    pub struct App {
        #[durable(versions(v1::Approval, v2::Approval))]
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

//! Changes the process working directory, so it runs in its own test binary

use endure::{Durable, DurableCell};
use tempfile::TempDir;

#[derive(Clone, Durable)]
struct Counter {
    count: DurableCell<u32>,
}

#[test]
fn a_relative_path_stays_tied_to_the_file_that_was_opened() -> eyre::Result<()> {
    let first = TempDir::new()?;
    let second = TempDir::new()?;
    std::fs::write(second.path().join("state.redb"), b"unrelated")?;

    std::env::set_current_dir(first.path())?;
    let counter = Counter::builder().path("state.redb").open()?;
    counter.update_blocking(|s| *s.count += 1)?;

    std::env::set_current_dir(second.path())?;
    counter.destroy()?;

    assert!(
        !first.path().join("state.redb").exists(),
        "the opened file is deleted"
    );

    assert_eq!(
        std::fs::read(second.path().join("state.redb"))?,
        b"unrelated"
    );

    Ok(())
}

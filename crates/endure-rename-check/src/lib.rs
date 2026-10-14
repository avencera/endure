//! Compiles only if `#[derive(Durable)]` finds endure under the name the manifest gives it

#[derive(Clone, storage::Durable)]
pub struct Renamed {
    pub count: storage::DurableCell<u32>,
}

#[cfg(test)]
mod tests {
    use super::Renamed;

    #[test]
    fn renamed_dependency_works() {
        let state = Renamed::memory();
        state.update_blocking(|s| *s.count += 1).unwrap();
        assert_eq!(*state.read().count.get(), 1);
    }
}

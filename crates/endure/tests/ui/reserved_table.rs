use endure::{Durable, DurableCell};

#[derive(Clone, Durable)]
struct App {
    #[durable(table = "__endure_meta")]
    count: DurableCell<u32>,
}

fn main() {}

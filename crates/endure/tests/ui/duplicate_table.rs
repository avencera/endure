use endure::{Durable, DurableCell};

#[derive(Clone, Durable)]
struct App {
    count: DurableCell<u32>,
    #[durable(table = "count")]
    other: DurableCell<u32>,
}

fn main() {}

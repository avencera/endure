use endure::{Durable, DurableCell};

#[derive(Clone, Durable)]
struct App {
    #[durable(table = "")]
    count: DurableCell<u32>,
}

fn main() {}

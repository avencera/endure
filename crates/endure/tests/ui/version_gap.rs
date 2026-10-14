use endure::{Durable, DurableCell};

#[derive(Clone, Durable)]
struct App {
    #[durable(versions(u8 = 1, u16 = 3))]
    count: DurableCell<u32>,
}

fn main() {}

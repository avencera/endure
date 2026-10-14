//! A tiny todo list that remembers its items between runs
//!
//! Run it a few times: `cargo run --example todo -- add "write docs"`, then
//! `cargo run --example todo -- done 1`, then `cargo run --example todo`

use endure::{Durable, DurableCell, DurableMap};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Todo {
    title: String,
    done: bool,
}

#[derive(Durable)]
struct Todos {
    items: DurableMap<u64, Todo>,
    next_id: DurableCell<u64>,
}

#[derive(Debug)]
struct NotFound(u64);

impl std::fmt::Display for NotFound {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "no todo with id {}", self.0)
    }
}

impl std::error::Error for NotFound {}

#[tokio::main]
async fn main() -> eyre::Result<()> {
    let dir = std::env::temp_dir().join("endure-todo-example");
    let todos = Todos::builder().dir(&dir).open()?;

    let args: Vec<String> = std::env::args().skip(1).collect();
    match args
        .iter()
        .map(String::as_str)
        .collect::<Vec<_>>()
        .as_slice()
    {
        ["add", title] => {
            let id = todos
                .update(|s| {
                    *s.next_id += 1;
                    let id = *s.next_id.get();
                    s.items.insert(
                        id,
                        Todo {
                            title: title.to_string(),
                            done: false,
                        },
                    );
                    id
                })
                .await?;

            println!("added #{id}");
        }
        ["done", id] => {
            let id: u64 = id.parse()?;
            todos
                .try_update(|s| {
                    s.items.get_mut(&id).ok_or(NotFound(id))?.done = true;
                    Ok::<_, NotFound>(())
                })
                .await?;
        }
        _ => {}
    }

    for (id, todo) in todos.read().items.iter() {
        let mark = if todo.done { "x" } else { " " };
        println!("[{mark}] #{id} {}", todo.title);
    }

    println!(
        "stored in {}",
        todos
            .path()
            .map(|p| p.display().to_string())
            .unwrap_or_default()
    );
    Ok(())
}

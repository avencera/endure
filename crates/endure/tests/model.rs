//! Random operation sequences checked against std collections, with reopens in between, so
//! the stored rows always rebuild exactly what memory held

use std::collections::BTreeMap;

use endure::{Durable, DurableMap, DurableVec};
use proptest::prelude::*;
use tempfile::TempDir;

#[derive(Durable)]
struct Model {
    list: DurableVec<u32>,
    map: DurableMap<u16, u32>,
}

#[derive(Debug, Clone)]
enum VecOp {
    Push(u32),
    PushFront(u32),
    Pop,
    PopFront,
    Insert(usize, u32),
    Remove(usize),
    Set(usize, u32),
    Truncate(usize),
    RetainEven,
    Sort,
}

#[derive(Debug, Clone)]
enum MapOp {
    Insert(u16, u32),
    Remove(u16),
    Bump(u16),
    RetainOdd,
    Clear,
}

fn vec_op() -> impl Strategy<Value = VecOp> {
    prop_oneof![
        4 => any::<u32>().prop_map(VecOp::Push),
        2 => any::<u32>().prop_map(VecOp::PushFront),
        1 => Just(VecOp::Pop),
        1 => Just(VecOp::PopFront),
        // inserting near the front again and again exhausts gaps and forces renumbering
        4 => (0usize..3, any::<u32>()).prop_map(|(index, value)| VecOp::Insert(index, value)),
        2 => (0usize..64, any::<u32>()).prop_map(|(index, value)| VecOp::Insert(index, value)),
        2 => (0usize..64).prop_map(VecOp::Remove),
        2 => (0usize..64, any::<u32>()).prop_map(|(index, value)| VecOp::Set(index, value)),
        1 => (0usize..64).prop_map(VecOp::Truncate),
        1 => Just(VecOp::RetainEven),
        1 => Just(VecOp::Sort),
    ]
}

fn map_op() -> impl Strategy<Value = MapOp> {
    prop_oneof![
        5 => (0u16..40, any::<u32>()).prop_map(|(key, value)| MapOp::Insert(key, value)),
        3 => (0u16..40).prop_map(MapOp::Remove),
        3 => (0u16..40).prop_map(MapOp::Bump),
        1 => Just(MapOp::RetainOdd),
        1 => Just(MapOp::Clear),
    ]
}

fn apply_vec(list: &mut DurableVec<u32>, expected: &mut Vec<u32>, op: &VecOp) {
    match *op {
        VecOp::Push(value) => {
            list.push(value);
            expected.push(value);
        }
        VecOp::PushFront(value) => {
            list.push_front(value);
            expected.insert(0, value);
        }
        VecOp::Pop => assert_eq!(list.pop(), expected.pop()),
        VecOp::PopFront => {
            let removed = (!expected.is_empty()).then(|| expected.remove(0));
            assert_eq!(list.pop_front(), removed);
        }
        VecOp::Insert(index, value) => {
            let index = index.min(expected.len());
            list.insert(index, value);
            expected.insert(index, value);
        }
        VecOp::Remove(index) if index < expected.len() => {
            assert_eq!(list.remove(index), expected.remove(index));
        }
        VecOp::Set(index, value) if index < expected.len() => {
            *list.get_mut(index).expect("in bounds") = value;
            expected[index] = value;
        }
        VecOp::Truncate(len) => {
            list.truncate(len);
            expected.truncate(len);
        }
        VecOp::RetainEven => {
            list.retain(|value| value % 2 == 0);
            expected.retain(|value| value % 2 == 0);
        }
        VecOp::Sort => {
            list.sort();
            expected.sort();
        }
        VecOp::Remove(_) | VecOp::Set(..) => {}
    }
}

fn apply_map(map: &mut DurableMap<u16, u32>, expected: &mut BTreeMap<u16, u32>, op: &MapOp) {
    match *op {
        MapOp::Insert(key, value) => {
            assert_eq!(map.insert(key, value), expected.insert(key, value))
        }
        MapOp::Remove(key) => assert_eq!(map.remove(&key), expected.remove(&key)),
        MapOp::Bump(key) => {
            if let Some(value) = map.get_mut(&key) {
                *value = value.wrapping_add(1);
            }

            if let Some(value) = expected.get_mut(&key) {
                *value = value.wrapping_add(1);
            }
        }
        MapOp::RetainOdd => {
            map.retain(|_, value| value % 2 == 1);
            expected.retain(|_, value| *value % 2 == 1);
        }
        MapOp::Clear => {
            map.clear();
            expected.clear();
        }
    }
}

fn open(dir: &TempDir) -> Durable<Model> {
    Model::builder().dir(dir.path()).open().expect("open model")
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]

    #[test]
    fn vec_matches_std_across_reopens(batches in prop::collection::vec(prop::collection::vec(vec_op(), 1..40), 1..8)) {
        let dir = TempDir::new().unwrap();
        let mut expected = Vec::new();
        let mut state = open(&dir);

        for (round, batch) in batches.iter().enumerate() {
            state.update_blocking(|s| {
                for op in batch {
                    apply_vec(&mut s.list, &mut expected, op);
                }
            }).unwrap();

            prop_assert_eq!(state.read().list.iter().copied().collect::<Vec<_>>(), expected.clone());

            if round % 2 == 1 {
                drop(state);
                state = open(&dir);
                prop_assert_eq!(state.read().list.iter().copied().collect::<Vec<_>>(), expected.clone());
            }
        }

        drop(state);
        let state = open(&dir);
        prop_assert_eq!(state.read().list.iter().copied().collect::<Vec<_>>(), expected);
    }

    #[test]
    fn map_matches_std_across_reopens(batches in prop::collection::vec(prop::collection::vec(map_op(), 1..30), 1..8)) {
        let dir = TempDir::new().unwrap();
        let mut expected = BTreeMap::new();
        let mut state = open(&dir);

        for (round, batch) in batches.iter().enumerate() {
            state.update_blocking(|s| {
                for op in batch {
                    apply_map(&mut s.map, &mut expected, op);
                }
            }).unwrap();

            if round % 2 == 1 {
                drop(state);
                state = open(&dir);
            }

            let stored: BTreeMap<u16, u32> = state.read().map.iter().map(|(key, value)| (*key, *value)).collect();
            prop_assert_eq!(stored, expected.clone());
        }
    }
}

#[test]
fn repeated_front_inserts_renumber_without_losing_order() -> eyre::Result<()> {
    let dir = TempDir::new()?;
    let state = open(&dir);

    // each insert at index 1 halves the gap after the first element, so this renumbers often
    state.update_blocking(|s| {
        s.list.push(0);
        s.list.push(u32::MAX);
        for value in 1..200 {
            s.list.insert(1, value);
        }
    })?;

    let expected: Vec<u32> = std::iter::once(0)
        .chain((1..200).rev())
        .chain(std::iter::once(u32::MAX))
        .collect();
    assert_eq!(
        state.read().list.iter().copied().collect::<Vec<_>>(),
        expected
    );

    drop(state);
    let state = open(&dir);
    assert_eq!(
        state.read().list.iter().copied().collect::<Vec<_>>(),
        expected
    );
    Ok(())
}

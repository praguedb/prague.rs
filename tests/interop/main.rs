//! The generated contracts against fixtures Prague (.NET) wrote.
mod contracts;

mod catalog;
mod shop;
mod showfeed;
mod tenant;

use prague::wire::{self, Decode};

/// The bytes of `testdata/<dir>/<name>`.
pub fn fixture(dir: &str, name: &str) -> Vec<u8> {
    let path = format!("{}/testdata/{dir}/{name}", env!("CARGO_MANIFEST_DIR"));
    std::fs::read(&path).unwrap_or_else(|e| panic!("{path}: {e}"))
}

/// Decodes `testdata/<dir>/<name>` as a `T`.
pub fn decode<T: Decode>(dir: &str, name: &str) -> T {
    wire::decode(&fixture(dir, name)).unwrap_or_else(|e| panic!("{dir}/{name}: {e}"))
}

use crate::dataset::load_dataset;

mod dataset;
mod query;
mod dataset_test;
mod errors;

fn main() {
    let courses = load_dataset("pair.zip").unwrap();
    println!("{}", courses.len());
}

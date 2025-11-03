use crate::dataset::load_dataset;
use crate::query::{Query, execute_query};
use prompted::input;

mod dataset;
mod dataset_test;
mod query;
mod testing;

fn main() -> ! {
    let courses = load_dataset("pair.zip").unwrap();
    println!("{}", courses.len());
    loop {
        let _ = input!();
        let json = std::fs::read_to_string("test.json").unwrap();
        match serde_json::from_str::<Query>(&json) {
            Ok(query) => {
                let result = execute_query(&query, &courses);
                println!("{:#?}", result);
            }
            Err(e) => eprintln!("{}", e),
        }
    }
}

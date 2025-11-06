use crate::types::Dataset;
use crate::types::Value;
use macros::Dataset;
use ordered_float::OrderedFloat;
use serde::Deserialize;
use std::fs::File;
use std::io;
use std::io::Read;
use zip::ZipArchive;

pub const EPSILON: f32 = 1e-4;

#[derive(Debug, Deserialize)]
pub(crate) struct SectionFile {
    result: Vec<SectionJson>,
}

#[derive(Debug, Dataset, Clone)]
#[field_prefix("sections_")]
pub struct Section {
    pub uuid: String,
    pub id: String,
    pub title: String,
    pub instructor: String,
    pub dept: String,
    pub year: OrderedFloat<f32>,
    pub avg: OrderedFloat<f32>,
    pub pass: OrderedFloat<f32>,
    pub fail: OrderedFloat<f32>,
    pub audit: OrderedFloat<f32>,
}

#[derive(Deserialize, Debug)]
struct SectionJson {
    #[serde(rename = "id")]
    uuid: i32,
    #[serde(rename = "Course")]
    id: String,
    #[serde(rename = "Title")]
    title: String,
    #[serde(rename = "Professor")]
    instructor: String,
    #[serde(rename = "Subject")]
    dept: String,
    #[serde(rename = "Year")]
    year: String,
    #[serde(rename = "Avg")]
    avg: f32,
    #[serde(rename = "Pass")]
    pass: f32,
    #[serde(rename = "Fail")]
    fail: f32,
    #[serde(rename = "Audit")]
    audit: f32,
}

pub fn load_dataset(file_name: &str) -> io::Result<Vec<Section>> {
    let file = File::open(file_name)?;
    let mut archive = ZipArchive::new(file)?;
    let mut dataset = vec![];

    for i in 0..archive.len() {
        let mut file = archive.by_index(i)?;
        let mut json = String::new();
        file.read_to_string(&mut json)?;
        let section_file: SectionFile = match serde_json::from_str(&json) {
            Ok(c) => c,
            Err(e) => {
                println!("Error while parsing {}, {}", file.name(), e);
                continue;
            }
        };
        for section in section_file.result {
            dataset.push(section);
        }
    }

    Ok(dataset
        .into_iter()
        .map(|course| Section {
            uuid: course.uuid.to_string(),
            id: course.id,
            title: course.title,
            instructor: course.instructor,
            dept: course.dept,
            year: course.year.parse().unwrap(),
            avg: OrderedFloat::from(course.avg),
            pass: OrderedFloat::from(course.pass),
            fail: OrderedFloat::from(course.fail),
            audit: OrderedFloat::from(course.audit),
        })
        .collect())
}

#[cfg(test)]
#[path = "dataset_test.rs"]
mod dataset_test;

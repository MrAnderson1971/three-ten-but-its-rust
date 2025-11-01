use macros::FieldGetter;
use serde::{Deserialize, Serialize};
use std::fs::File;
use std::io;
use std::io::Read;
use zip::ZipArchive;

#[derive(Serialize, Debug)]
pub enum Value {
    Num(f32),
    Str(String),
}

#[derive(Debug, Deserialize)]
pub(crate) struct CourseFile {
    result: Vec<CourseJson>,
}

#[derive(Debug, FieldGetter, Clone)]
pub struct Course {
    pub uuid: String,
    pub id: String,
    pub title: String,
    pub instructor: String,
    pub dept: String,
    pub year: f32,
    pub avg: f32,
    pub pass: f32,
    pub fail: f32,
    pub audit: f32,
}

#[derive(Deserialize, Debug)]
struct CourseJson {
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

pub fn load_dataset(file: &str) -> Result<Vec<Course>, io::Error> {
    let file = File::open("pair.zip")?;
    let mut archive = ZipArchive::new(file)?;
    let mut dataset = vec![];

    for i in 0..archive.len() {
        let mut file = archive.by_index(i)?;
        let mut json = String::new();
        file.read_to_string(&mut json)?;
        let mut course_file: CourseFile = match serde_json::from_str(&json) {
            Ok(c) => c,
            Err(e) => {
                println!("Error while parsing {}, {}", file.name(), e);
                continue;
            }
        };
        for course in course_file.result.drain(..) {
            dataset.push(course);
        }
    }

    Ok(dataset
        .iter()
        .map(|course| Course {
            uuid: course.uuid.to_string(),
            id: course.id.clone(),
            title: course.title.clone(),
            instructor: course.instructor.clone(),
            dept: course.dept.clone(),
            year: course.year.parse().unwrap(),
            avg: course.avg,
            pass: course.pass,
            fail: course.fail,
            audit: course.audit,
        })
        .collect())
}

#[cfg(test)]
#[path = "dataset_test.rs"]
mod dataset_test;

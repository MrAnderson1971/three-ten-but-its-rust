mod dataset {
    use serde::Deserialize;

    #[derive(Debug, Deserialize)]
    pub struct CourseDataset {
        pub result: Vec<Course>,
    }

    #[derive(Deserialize, Debug)]
    pub struct Course {
        #[serde(rename = "id")]
        pub uuid: String,
        #[serde(rename = "Course")]
        pub id: String,
        #[serde(rename = "Title")]
        pub title: String,
        #[serde(rename = "Professor")]
        pub instructor: String,
        #[serde(rename = "Subject")]
        pub dept: String,
        #[serde(rename = "Year")]
        pub year: i32,
        #[serde(rename = "Avg")]
        pub avg: f32,
        #[serde(rename = "Pass")]
        pub pass: i32,
        #[serde(rename = "Fail")]
        pub fail: i32,
        #[serde(rename = "Audit")]
        pub audit: i32,
    }
}

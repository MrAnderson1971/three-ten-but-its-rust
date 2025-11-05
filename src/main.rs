use crate::dataset::{Course, load_dataset};
use crate::query::{Query, execute_query};
use crate::types::QueryResult;
use axum::http::StatusCode;
use axum::routing::get;
use axum::{Json, Router};
use std::sync::LazyLock;

mod dataset;
mod dataset_test;
mod query;
mod testing;
mod types;

static COURSES: LazyLock<Vec<Course>, fn() -> Vec<Course>> =
    LazyLock::new(|| load_dataset("pair.zip").unwrap());

const PORT: i32 = 310;

async fn query_courses(
    axum::extract::Query(params): axum::extract::Query<std::collections::HashMap<String, String>>,
) -> Result<Json<QueryResult>, StatusCode> {
    let json = params.get("q").ok_or(StatusCode::BAD_REQUEST)?;
    println!("Received query from URL param: {}", json);

    match serde_json::from_str::<Query>(&json) {
        Ok(query) => {
            let result = execute_query(&query, &COURSES);
            println!("{:#?}", result);
            let query_result = match result {
                Ok(ok) => QueryResult::OK { result: ok },
                Err(error) => QueryResult::ERROR {
                    error: error.to_string(),
                },
            };
            Ok(Json(query_result))
        }
        Err(e) => {
            eprintln!("{}", e);
            Ok(Json(QueryResult::ERROR {
                error: e.to_string(),
            }))
        }
    }
}

#[tokio::main]
async fn main() {
    let app = Router::new()
        .route("/", get(|| async { "Hello, world!" }))
        .route("/courses", get(query_courses));

    let listener = tokio::net::TcpListener::bind(format!("127.0.0.1:{}", PORT))
        .await
        .unwrap();

    println!("Waiting on port {}", PORT);
    axum::serve(listener, app).await.unwrap();
}

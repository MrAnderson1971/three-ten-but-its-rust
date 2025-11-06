use crate::dataset::{Section, load_dataset};
use crate::query::{Query, execute_query};
use crate::rooms_dataset::{Room, load_rooms_dataset};
use crate::types::QueryResult;
use axum::http::StatusCode;
use axum::routing::get;
use axum::{Json, Router};
use prompted::input;
use std::sync::LazyLock;
use tower_http::cors::CorsLayer;

mod dataset;
mod dataset_test;
mod query;
mod rooms_dataset;
mod testing;
mod types;

enum DS {
    SECTION,
    ROOM,
}

static SECTIONS: LazyLock<Vec<Section>, fn() -> Vec<Section>> =
    LazyLock::new(|| load_dataset("pair.zip").unwrap());

static ROOMS: LazyLock<Vec<Room>, fn() -> Vec<Room>> =
    LazyLock::new(|| load_rooms_dataset("campus.zip").unwrap());

const PORT: i32 = 310;

async fn query_courses(
    dataset: DS,
    axum::extract::Query(params): axum::extract::Query<std::collections::HashMap<String, String>>,
) -> Result<Json<QueryResult>, StatusCode> {
    let json = params.get("q").ok_or(StatusCode::BAD_REQUEST)?;
    println!("Received query from URL param: {}", json);

    match serde_json::from_str::<Query>(&json) {
        Ok(query) => {
            let result = match dataset {
                DS::SECTION => execute_query(&query, &SECTIONS),
                DS::ROOM => execute_query(&query, &ROOMS),
            };
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

fn console_ui() -> ! {
    loop {
        println!(r#"Type "section" or "room""#);
        let which = input!();
        let json = std::fs::read_to_string("test.json").unwrap();
        match serde_json::from_str::<Query>(&json) {
            Ok(query) => {
                let result = match which.to_ascii_lowercase().as_str() {
                    "section" => execute_query(&query, &SECTIONS),
                    "room" => execute_query(&query, &ROOMS),
                    _ => continue,
                };
                println!("{:#?}", result);
            }
            Err(e) => eprintln!("{}", e),
        }
    }
}

#[tokio::main]
async fn main() {
    let app = Router::new()
        .route("/", get(|| async { "Hello, world!" }))
        .route("/sections", get(|param| query_courses(DS::SECTION, param)))
        .route("/rooms", get(|param| query_courses(DS::ROOM, param)))
        .layer(CorsLayer::new().allow_origin("*".parse::<axum::http::HeaderValue>().unwrap()));

    let listener = tokio::net::TcpListener::bind(format!("127.0.0.1:{}", PORT))
        .await
        .unwrap();

    std::thread::spawn(console_ui);

    println!("Waiting on port {}", PORT);
    axum::serve(listener, app).await.unwrap();
}

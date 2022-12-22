#[macro_use]
extern crate lazy_static;

lazy_static! {
    static ref REVERSED_INDEX: BTreeMap<String, Vec<[u8; 12]>> =
        ReversedIndexAPI::load_from_file("../indexer/reversed_index.bin");
}

use std::collections::BTreeMap;

use index_api::{reversed_index::ReversedIndexAPI, forward_index};
use serde::{Deserialize, Serialize};

pub mod boolean_search;

#[derive(Serialize, Deserialize, Debug)]
struct RequestSchema {
    words: Vec<String>,
}

#[derive(Serialize, Deserialize, Debug)]
struct ResponseSchema {
    doc_ids: Vec<[u8; 12]>,
}

use actix_web::{get, post, App, HttpResponse, HttpServer, Responder};

#[get("/")]
async fn hello() -> impl Responder {
    HttpResponse::Ok().body("Hello world!")
}

#[post("/search")]
async fn search(req_body: String) -> impl Responder {
    println!("{req_body}");
    let data: RequestSchema = serde_json::from_str(&req_body).unwrap();
    // println!("{data:?}");
    // println!("{:?}", REVERSED_INDEX.keys().collect::<Vec<&String>>());
    // println!("{:?}", REVERSED_INDEX.get("урук"));
    let result_ids = boolean_search::boolean_search(&data.words, &REVERSED_INDEX);
    let forward_metadata = forward_index::ForwardIndexApi::new("../indexer/forward_index.bin", "../indexer/forward_index_metadata.bin").load_metadata();


    println!("{:?}", forward_metadata.get(&result_ids[0]));
    let result = ResponseSchema { doc_ids: result_ids};

    return HttpResponse::Ok().body(serde_json::to_string(&result).unwrap());
}

#[tokio::main]
async fn main() -> std::io::Result<()> {
    HttpServer::new(|| App::new().service(hello).service(search))
        .bind(("localhost", 8080))?
        .run()
        .await
}

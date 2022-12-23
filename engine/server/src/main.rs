#[macro_use]
extern crate lazy_static;

lazy_static! {
    static ref REVERSED_INDEX: BTreeMap<String, Vec<[u8; 12]>> =
        ReversedIndexAPI::load_from_file("../indexer/reversed_index.bin").0;
    static ref WORDS_IDF: HashMap<String, f64> =
        ReversedIndexAPI::load_from_file("../indexer/reversed_index.bin").1;
}

use std::collections::{BTreeMap, HashMap};

use index_api::{forward_index, reversed_index::ReversedIndexAPI};
use serde::{Deserialize, Serialize};

pub mod boolean_search;

#[derive(Serialize, Deserialize, Debug)]
struct RequestSchema {
    words: Vec<String>,
}

#[derive(Serialize, Deserialize, Debug)]
struct ResponseSchema {
    doc_ids: Vec<mongodb::bson::oid::ObjectId>,
}

use actix_web::{get, post, App, HttpResponse, HttpServer, Responder};

#[get("/")]
async fn hello() -> impl Responder {
    HttpResponse::Ok().body("Hello world!")
}

use itertools::Itertools;
use std::cmp::Ordering;


fn calculate_cos(v1: &Vec<f64>, v2: &Vec<f64>) -> f64 {
    let numerator = v1.iter().zip(v2.iter()).map(|(a, b)| a * b).sum::<f64>();
    let denumerator =
        f64::sqrt(v1.iter().map(|a| a * a).sum()) * f64::sqrt(v2.iter().map(|b| b * b).sum());

    return numerator / denumerator;
}

fn rank_docs(request: &Vec<String>, docs: &Vec<[u8; 12]>) -> Vec<[u8; 12]> {
    let mut forward_api = forward_index::ForwardIndexApi::new(
        "../indexer/forward_index.bin",
        "../indexer/forward_index_metadata.bin",
    );

    let request_words_count = request.len() as f64;
    let request_vector = request
        .iter()
        .map(|word| WORDS_IDF.get(word).unwrap_or(&0f64) * 1f64 / request_words_count)
        .collect_vec();

    let result = docs.iter().cloned().sorted_by_key( |doc| {
        let forward_record = forward_api.load_record_by(doc);
        let searching_words: Vec<forward_index::Lemma> = forward_record
            .lemmas
            .into_iter()
            .filter(|lemma| request.contains(&lemma.text))
            .sorted_by_key(|lemma| lemma.text.clone())
            .collect_vec();

        let document_vector = searching_words
            .iter()
            .map(|lemma| lemma.frequency * WORDS_IDF.get(&lemma.text).unwrap())
            .collect();

        return (calculate_cos(&request_vector, &document_vector) * 1000f64) as i64;  
    }
    ).rev().collect();

    return result;
}

#[post("/search")]
async fn search(req_body: String) -> impl Responder {
    println!("{req_body}");
    let data: RequestSchema = serde_json::from_str(&req_body).unwrap();
    let result_ids = boolean_search::boolean_search(&data.words, &REVERSED_INDEX);
    let ranked_results = rank_docs(&data.words, &result_ids);
    let result = ResponseSchema {
        doc_ids: ranked_results.into_iter().map(|doc_id| mongodb::bson::oid::ObjectId::from_bytes(doc_id)).collect(),
    };

    return HttpResponse::Ok().body(serde_json::to_string(&result).unwrap());
}

#[tokio::main]
async fn main() -> std::io::Result<()> {
    HttpServer::new(|| App::new().service(hello).service(search))
        .bind(("localhost", 8080))?
        .run()
        .await
}

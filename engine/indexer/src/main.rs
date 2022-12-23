use serde::{Deserialize, Serialize};

use mongodb::bson::doc;
use mongodb::bson::oid::ObjectId;

#[derive(Debug, Serialize, Deserialize)]
struct Page {
    _id: ObjectId,
    path: String,
    lemmas: Vec<Lemma>,
}

use index_api::forward_index::*;
use index_api::reversed_index::*;

#[derive(Debug, Serialize, Deserialize)]
struct Lemma {
    text: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = mongodb::Client::with_uri_str("mongodb://localhost").await?;

    let db = client.database("test");

    let test_collection = db.collection::<Page>("docs");

    let aggregate_pipeline = vec![doc! {
       "$sort": {
          "_id": 1
       }
    }];
    let mut cursor = test_collection.aggregate(aggregate_pipeline, None).await?;

    build_forward_index(&mut cursor).await;

    let mut fapi = ForwardIndexApi::new("./forward_index.bin", "./forward_index_metadata.bin");

    spimi_invert(&mut fapi).await?;

    Ok(())
}

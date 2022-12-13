extern crate sort;

use futures::stream::TryStreamExt;
use futures::{Stream, StreamExt, TryStream};
use std::collections::HashMap;

use std::io::Cursor;
use std::mem::size_of_val;

use serde::{Deserialize, Serialize};

use mongodb::bson::oid::ObjectId;
use mongodb::bson::{doc, Document};
use mongodb::{bson, Client};

#[derive(Debug, Serialize, Deserialize)]
struct Page {
    _id: ObjectId,
    path: String,
    lemmas: Vec<Lemma>,
}

#[derive(Debug, Serialize, Deserialize)]
struct Lemma {
    text: String,
}

const MEMORY_LIMIT: usize = 1024 * 1024 * 8;

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
    let cursor = test_collection.aggregate(aggregate_pipeline, None).await?;

    let mut chunked_cursor = cursor.try_chunks(5);

    spimi_invert(&mut chunked_cursor, db.clone()).await?;

    Ok(())
}


use futures::stream::TryChunks;

async fn spimi_invert(
    stream: &mut TryChunks<mongodb::Cursor<Document>>,
    database: mongodb::Database,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut count_pages_without_dump = 0;
    let mut dictionary: HashMap<String, Vec<_>> = HashMap::new();

    let mut block_api = BlockApi::new(database);

    while let Some(docs) = stream.try_next().await? {
        let pages = docs
            .into_iter()
            .map(|document| bson::de::from_document::<Page>(document).unwrap());

        for page in pages {
            for lemma in page.lemmas.into_iter() {
                dictionary.entry(lemma.text).or_default().push(page._id);
            }
            count_pages_without_dump += 1;

            if size_of_dictionary(&dictionary, count_pages_without_dump) > MEMORY_LIMIT {
                block_api.dump_dictionary_to_mongo(&dictionary).await?;
                dictionary.clear();
                count_pages_without_dump = 0;
            }
        }
        break;
    }

    block_api.dump_dictionary_to_mongo(&dictionary).await?;

    Ok(())
}

struct BlockApi {
    mongo_database: mongodb::Database,
    count_upload_blocs: i64,
}

impl BlockApi {
    fn new(database: mongodb::Database) -> Self {
        return Self {
            mongo_database: database,
            count_upload_blocs: 0,
        };
    }

    async fn dump_dictionary_to_mongo(
        &mut self,
        dict: &HashMap<String, Vec<ObjectId>>,
    ) -> Result<(), mongodb::error::Error> {
        let block = doc! {
            "block_id": self.count_upload_blocs,
            "dictionary": bson::to_bson(dict)?,
        };

        self.mongo_database
            .collection("block")
            .insert_one(block, None)
            .await?;

        self.count_upload_blocs += 1;

        Ok(())
    }
}

fn size_of_dictionary(
    dict: &HashMap<String, Vec<ObjectId>>,
    count_pages_processed: usize,
) -> usize {
    let average_russian_word_len = 7;

    let keys_size = dict.keys().len() * average_russian_word_len;
    let values_size = dict.keys().len() * count_pages_processed * std::mem::size_of::<[u8; 12]>();

    return keys_size + values_size;
}

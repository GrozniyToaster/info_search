use std::{
    io::{Read, Write, Seek},
    mem::size_of, collections::HashMap,
};



lazy_static! {
    static ref FORWARD_INDEX_RECORD_METADATA_SIZE: usize = bincode::serialized_size(&ForwardIndexRecordMetadata::default()).unwrap() as usize;
}


use futures::TryStreamExt;
use mongodb::bson::Document;

use std::fs::OpenOptions;

use serde::{Deserialize, Serialize, de::IntoDeserializer};

#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub struct ForwardIdexRecord {
    pub id: [u8; 12],
    pub lemmas: Vec<String>,
}

impl ForwardIdexRecord {
    fn from_document(document: Document) -> Self {
        Self {
            id: document.get_object_id("_id").unwrap().bytes(),
            lemmas: document
                .get_array("lemmas")
                .unwrap()
                .into_iter()
                .map(|lemma_info| {
                    lemma_info
                        .as_document()
                        .unwrap()
                        .get_str("text")
                        .unwrap()
                        .to_string()
                })
                .collect(),
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Default)]
pub struct ForwardIndexRecordMetadata {
    id: [u8; 12],
    offset_in_index_file: u64,
    size_of_record: u64,
}

pub async fn build_forward_index(cursor: &mut mongodb::Cursor<Document>) {
    let mut index_file = OpenOptions::new()
        .truncate(true)
        .create(true)
        .write(true)
        .open("./forward_index.bin")
        .unwrap();

    let mut index_metadata_file = OpenOptions::new()
        .truncate(true)
        .create(true)
        .write(true)
        .open("./forward_index_metadata.bin")
        .unwrap();

    while let Some(document) = cursor.try_next().await.unwrap() {
        let record = ForwardIdexRecord::from_document(document);
        let record_as_bytes = bincode::serialize(&record).unwrap();

        let current_metadata = ForwardIndexRecordMetadata {
            id: record.id,
            offset_in_index_file: index_file.metadata().unwrap().len(),
            size_of_record: record_as_bytes.len() as u64,
        };

        index_file.write(&record_as_bytes[..]).unwrap();

        let record_as_bytes = bincode::serialize(&current_metadata).unwrap();
        index_metadata_file.write(&record_as_bytes[..]).unwrap();
    }
}

use std::fs::File;
use std::io::SeekFrom;

pub struct ForwardIndexApi {
    index_file: File,
    index_metadata: File,
}

impl ForwardIndexApi {
    pub fn new(index_path: &str, index_metadata_path: &str) -> Self {
        Self {
            index_file: File::open(&index_path).unwrap(),
            index_metadata: File::open(&index_metadata_path).unwrap(),
        }
    }

    pub fn next(&mut self) -> Result<ForwardIdexRecord, Box<dyn std::error::Error>> {
        let mut metadata_buf = vec![0; *FORWARD_INDEX_RECORD_METADATA_SIZE];
        self.index_metadata.read_exact(&mut metadata_buf)?;

        let next_record_info: ForwardIndexRecordMetadata = bincode::deserialize(&metadata_buf).unwrap();

        let mut record_buf = vec![0u8; next_record_info.size_of_record as usize];
        self.index_file.read_exact(&mut record_buf.as_mut_slice())?;

        let next_record: ForwardIdexRecord = bincode::deserialize(&record_buf.as_slice()).unwrap();

        return Ok(next_record);
    }

    fn _load_metadata_record(&mut self) ->  Result<ForwardIndexRecordMetadata, Box<dyn std::error::Error>>  {
        let mut metadata_buf = vec![0; *FORWARD_INDEX_RECORD_METADATA_SIZE];
        self.index_metadata.read_exact(&mut metadata_buf)?;
        let next_record_info: ForwardIndexRecordMetadata = bincode::deserialize(&metadata_buf)?;

        return Ok(next_record_info);
        

    }

    pub fn load_metadata(&mut self) -> HashMap<[u8;12], (u64, u64)> {
        let mut metadata = HashMap::new();
        
        while let Ok(next_record) = self._load_metadata_record() {
            
            metadata.insert(next_record.id, (next_record.offset_in_index_file, next_record.size_of_record));
        } 
        return metadata;

    }
}

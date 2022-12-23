use std::{
    collections::HashMap,
    io::{Read, Seek, Write},
    mem::size_of,
    ops::AddAssign,
};

lazy_static! {
    static ref FORWARD_INDEX_RECORD_METADATA_SIZE: usize =
        bincode::serialized_size(&ForwardIndexRecordMetadata::default()).unwrap() as usize;
}

use futures::TryStreamExt;
use mongodb::bson::Document;

use std::fs::OpenOptions;

use serde::{de::IntoDeserializer, Deserialize, Serialize};

#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub struct ForwardIdexRecord {
    pub id: [u8; 12],
    pub lemmas: Vec<Lemma>,
}

#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub struct Lemma {
    pub frequency: f64,
    pub text: String,
}

impl ForwardIdexRecord {
    fn from_document(document: Document) -> Self {
        let lemmas: Vec<String> = document
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
            .collect();

        let mut lemmas_count = HashMap::new();
        let count_words = lemmas.len() as f64;
        for lemma in lemmas {
            lemmas_count.entry(lemma).or_insert(0).add_assign(1);
        }

        return Self {
            id: document.get_object_id("_id").unwrap().bytes(),
            lemmas: lemmas_count
                .into_iter()
                .map(|(lemma, count)| Lemma {
                    frequency: count as f64 / count_words,
                    text: lemma,
                })
                .collect(),
        };
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

type DocId = [u8; 12];
pub struct ForwardIndexApi {
    index_file: File,
    index_metadata: File,

    loaded_metadata: Option<HashMap<DocId, ForwardIndexMetaRecord>>,
}

struct ForwardIndexMetaRecord {
    offset_in_index_file: u64,
    size_of_record: u64,
}

impl ForwardIndexApi {
    pub fn new(index_path: &str, index_metadata_path: &str) -> Self {
        Self {
            index_file: File::open(&index_path).unwrap(),
            index_metadata: File::open(&index_metadata_path).unwrap(),

            loaded_metadata: None,
        }
    }

    pub fn next(&mut self) -> Result<ForwardIdexRecord, Box<dyn std::error::Error>> {
        let mut metadata_buf = vec![0; *FORWARD_INDEX_RECORD_METADATA_SIZE];
        self.index_metadata.read_exact(&mut metadata_buf)?;

        let next_record_info: ForwardIndexRecordMetadata =
            bincode::deserialize(&metadata_buf).unwrap();

        let mut record_buf = vec![0u8; next_record_info.size_of_record as usize];
        self.index_file.read_exact(&mut record_buf.as_mut_slice())?;

        let next_record: ForwardIdexRecord = bincode::deserialize(&record_buf.as_slice()).unwrap();

        return Ok(next_record);
    }

    fn _load_metadata_record(
        &mut self,
    ) -> Result<ForwardIndexRecordMetadata, Box<dyn std::error::Error>> {
        let mut metadata_buf = vec![0; *FORWARD_INDEX_RECORD_METADATA_SIZE];
        self.index_metadata.read_exact(&mut metadata_buf)?;
        let next_record_info: ForwardIndexRecordMetadata = bincode::deserialize(&metadata_buf)?;

        return Ok(next_record_info);
    }

    pub fn load_metadata(&mut self) {
        if let Some(_) = self.loaded_metadata {
            return;
        }

        let mut metadata = HashMap::new();

        while let Ok(next_record) = self._load_metadata_record() {
            metadata.insert(
                next_record.id,
                ForwardIndexMetaRecord {
                    offset_in_index_file: next_record.offset_in_index_file,
                    size_of_record: next_record.size_of_record,
                },
            );
        }
        self.loaded_metadata = Some(metadata);
    }

    pub fn load_record_by(&mut self, id: &DocId) -> ForwardIdexRecord {
        self.load_metadata();
        let loaded_meta = self.loaded_metadata.as_ref().unwrap();
        let meta_info = loaded_meta.get(id).unwrap();
        self.index_file
            .seek(SeekFrom::Start(meta_info.offset_in_index_file))
            .unwrap();
        let mut buf = vec![0u8; meta_info.size_of_record as usize];
        self.index_file.read_exact(&mut buf).unwrap();
        let record: ForwardIdexRecord = bincode::deserialize(&buf).unwrap();

        return record;
    }
}

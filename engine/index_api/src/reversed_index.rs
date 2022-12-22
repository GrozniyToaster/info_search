use std::{
    collections::BTreeMap,
    io::{Read, Write},
};

use crate::forward_index;

pub async fn spimi_invert(
    stream: &mut forward_index::ForwardIndexApi,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut dictionary = BTreeMap::new();
    let mut i = 0;
    while let Ok(docs) = stream.next() {
        println!("{i}");
        i +=1;
        for lemma in docs.lemmas {
            dictionary
                .entry(lemma.clone())
                .or_insert(Vec::default())
                .push(docs.id);
        
        }

        //     if size_of_dictionary(&dictionary, count_pages_without_dump) > MEMORY_LIMIT {
        //         block_api.dump_dictionary_to_mongo(&dictionary).await?;
        //         dictionary.clear();
        //         count_pages_without_dump = 0;
        //     }
        // }
        // break;
    }

    ReversedIndexAPI::dump_to_file(dictionary)?;

    Ok(())
}

#[derive(serde::Serialize, serde::Deserialize)]
struct ReversedIndexRecord {
    word: String,
    doc_ids: Vec<[u8; 12]>,
}

#[derive(serde::Serialize, serde::Deserialize)]
struct ReVersedIndex {
    index: Vec<ReversedIndexRecord>,
}

pub struct ReversedIndexAPI {
    file: std::fs::File,
}

impl ReversedIndexAPI {
    fn new(path: &str) -> Self {
        Self {
            file: std::fs::File::open(&path).unwrap(),
        }
    }

    fn dump_to_file(
        dictionary: BTreeMap<String, Vec<[u8; 12]>>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let mut file = std::fs::OpenOptions::new()
            .truncate(true)
            .create_new(true)
            .write(true)
            .open("./reversed_index.bin")?;

        let index = dictionary
            .into_iter()
            .map(|(word, doc_ids)| ReversedIndexRecord {
                word: word,
                doc_ids: doc_ids,
            })
            .collect();

        let buf = bincode::serialize(&ReVersedIndex { index: index })?;

        file.write(&buf.as_slice())?;

        Ok(())
    }

    pub fn load_from_file(path: &str) -> BTreeMap<String, Vec<[u8; 12]>> {
        let mut file = std::fs::File::open(path).unwrap();
        let buf_size = file.metadata().unwrap().len();

        let mut buf = vec![0u8; buf_size as usize];

        file.read_exact(&mut buf.as_mut_slice()).unwrap();
        let index: ReVersedIndex = bincode::deserialize(&buf).unwrap();

        return BTreeMap::from_iter(
            index
                .index
                .into_iter()
                .map(|record| (record.word, record.doc_ids)),
        );
    }
}

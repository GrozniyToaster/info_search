use std::collections::BTreeMap;

// use futures::stream::Iter;

use std::cmp::Ordering;

#[derive(Debug)]
struct SearchState<'a> {
    ids: &'a Vec<[u8; 12]>,
    position: usize,
}

pub fn boolean_search(
    request: &Vec<String>,
    reversed_index: &BTreeMap<String, Vec<[u8; 12]>>,
) -> Vec<[u8; 12]> {
    let mut docs_containing_words = Vec::with_capacity(request.len());
    let mut maximum_doc_id = [u8::MIN; 12];

    for word in request {
        match reversed_index.get(word) {
            Some(docs) => {
                docs_containing_words.push(SearchState {
                    ids: docs,
                    position: 0,
                });
                maximum_doc_id = std::cmp::max(maximum_doc_id, docs[0])
            }
            None => continue,
        }
    }
    let mut result = Vec::new();

    'main_loop: loop {
        let mut some_cheked = false;
        let mut all_previous_equal = true;

        'word_cycle: for state in &mut docs_containing_words {
            'doc_id_cycle: while let Some(next_doc_id) = state.ids.get(state.position) {
                some_cheked = some_cheked || true;

                match next_doc_id.cmp(&maximum_doc_id) {
                    Ordering::Less => {
                        state.position += 1;
                        continue 'doc_id_cycle;
                    }
                    Ordering::Equal => {
                        all_previous_equal = all_previous_equal && true;
                        break 'doc_id_cycle;
                    }
                    Ordering::Greater => {
                        all_previous_equal = false;
                        maximum_doc_id = next_doc_id.clone();
                        continue 'main_loop;
                    }
                }
            }
        }
        if all_previous_equal {
            result.push(maximum_doc_id);
            docs_containing_words
                .iter_mut()
                .for_each(|state| state.position += 1)
        }

        if !some_cheked {
            break 'main_loop;
        }
    }

    return result;
}

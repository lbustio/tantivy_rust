#[macro_use]
extern crate tantivy;
use tantivy::Index;
use tantivy::collector::TopDocs;
use tantivy::directory::MmapDirectory;
use tantivy::ReloadPolicy;
use tantivy::Result;
use tantivy::query::QueryParser;
use tantivy::schema::Field;
use tantivy::schema::Schema;
use tantivy::TantivyError;

use std::env;
use std::fs;

/// Get the current directory.
///
/// This function returns a `String` representing the current directory.
fn get_current_dir() -> String {
    let current_dir = env::current_dir().expect("Failed to get current directory");
    let current_dir_str = current_dir.to_string_lossy().into_owned();
    current_dir_str
}

fn index_exists(index_path: &str) -> bool {
    let index_directory = Index::open_in_dir(index_path).is_ok();
    index_directory
}

fn count_documents_in_index(index_location: &str) -> u64 {
    let directory = MmapDirectory::open(index_location).unwrap();
    let index = Index::open(directory).unwrap();
    
    let reader = index.reader().unwrap();
    let searcher = reader.searcher();
    
    searcher.num_docs()
}

fn get_index_size(index_location: &str) -> f64 {
    let metadata = fs::metadata(index_location).unwrap();
    let size = metadata.len();
    let size_in_megabytes = (size as f64) / (1024.0 * 1024.0);

    size_in_megabytes
}

fn read_index(index_path: &str) -> Result<Index> {
    let index = Index::open_in_dir(index_path)?;
    Ok(index)
}

fn query_index(index: &Index) -> Result<()> {
    let reader = index
        .reader_builder()
        .reload_policy(ReloadPolicy::OnCommit)
        .try_into()?;
    let searcher = reader.searcher();

    let schema: &Schema = &index.schema();
    let title_field: Field = schema.get_field("title").unwrap();
    let body_field: Field = schema.get_field("body").unwrap();
    let state_field: Field = schema.get_field("state").unwrap();

    let query_parser = QueryParser::for_index(&index, vec![title_field, body_field, state_field]);
    let query = query_parser.parse_query("united states")?;

    let top_docs = searcher.search(&query, &TopDocs::with_limit(10))?;
    for (_score, doc_address) in top_docs {
        let retrieved_doc = searcher.doc(doc_address)?;
        println!("{}", schema.to_json(&retrieved_doc));
    }

    Ok(())
}

fn main(){
    // Set the index directory in the project's root folder
    let current_path = get_current_dir();
    // Concatenar la carpeta "index"
    let index_path = format!("{}/index", current_path);
    println!("The current directory is: {:?}", index_path);

    let exists = index_exists(&index_path);
    if exists {
        println!("An index already exists at the location: {}", index_path);
        
        let document_count = count_documents_in_index(&index_path);
        println!("Número de documentos en el índice: {}", document_count);

        let index_size = get_index_size(&index_path);
        println!("Tamaño del índice: {} megabytes", index_size);

        let index_result = read_index(&index_path);
    
        match index_result {
            Ok(index) => {
                // Hacer algo con el índice leído
                println!("Index read: {:?}", index);
        
                if let Err(err) = query_index(&index) {
                    // Maneja el error si ocurrió
                    println!("Error en la consulta del índice: {:?}", err);
                }
            }
            Err(err) => {
                // Maneja el error si ocurrió al leer el índice
                println!("Error al leer el índice: {:?}", err);
            }
        }

    } else {
        println!("Index does not exist...");
    }
}
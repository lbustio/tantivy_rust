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
use tantivy::tokenizer::TokenizerManager;


use std::env;
use std::fs;

/// Get the current directory as a String.
fn get_current_dir() -> String {
    env::current_dir()
        .expect("Failed to get current directory")
        .to_string_lossy()
        .into_owned()
}

/// Check if an index exists at the given path.
fn index_exists(index_path: &str) -> bool {
    Index::open_in_dir(index_path).is_ok()
}

/// Count the number of documents in an index located at the given path.
fn count_documents_in_index(index_location: &str) -> Result<u64> {
    let directory = MmapDirectory::open(index_location)?;
    let index = Index::open(directory)?;
    
    let reader = index.reader()?;
    let searcher = reader.searcher();
    
    Ok(searcher.num_docs())
}

/// Get the size of the index located at the given path in megabytes.
fn get_index_size(index_location: &str) -> Result<f64> {
    let metadata = fs::metadata(index_location)?;
    let size = metadata.len();
    let size_in_megabytes = (size as f64) / (1024.0 * 1024.0);

    Ok(size_in_megabytes)
}

/// Read an index located at the given path and return the Index object.
fn read_index(index_path: &str) -> Result<Index> {
    Index::open_in_dir(index_path)
}

/// Perform a query on the given Index and print the results.
fn query_index(index: &Index) -> Result<()> {
    println!("Querying the index.....");

    let reader = index
        .reader_builder()
        .reload_policy(ReloadPolicy::OnCommit)
        .try_into()?;
    let searcher = reader.searcher();
    let schema: &Schema = &index.schema();

    let title_field: Field = schema.get_field("title").unwrap();
    let body_field: Field = schema.get_field("body").unwrap();
    let state_field: Field = schema.get_field("state").unwrap();

    // Crea un TokenizerManager para el análisis de texto
    let tokenizer_manager = TokenizerManager::default();

    // Crea un QueryParser con el esquema, el TokenizerManager y los campos definidos
    let query_parser = QueryParser::new(schema.clone(),  vec![title_field, body_field, state_field], tokenizer_manager);

    // Parsea la consulta para buscar la cadena "pepe"
    let query = query_parser.parse_query("i")?;

    // Realiza la búsqueda y obtiene los documentos más relevantes
    let reader = index.reader()?;
    let searcher = reader.searcher();
    let top_docs = searcher.search(&query, &TopDocs::with_limit(10))?;

     // Imprime la cantidad de documentos en top_docs
     println!("Cantidad de documentos encontrados: {}", top_docs.len());

    // Imprime los documentos encontrados
    for (_score, doc_address) in top_docs {
        let retrieved_doc = searcher.doc(doc_address)?;
        println!("{}", schema.to_json(&retrieved_doc));
    }

    Ok(())

    
}

fn main() {
    // Set the index directory in the project's root folder
    let current_path = get_current_dir();
    // Concatenate the "index" folder
    let index_path = format!("{}/index", current_path);
    println!("The current directory is: {:?}", index_path);

    let index_exists = index_exists(&index_path);
    if index_exists {
        println!("An index already exists at the location: {}", index_path);
        
        if let Ok(document_count) = count_documents_in_index(&index_path) {
            println!("Number of documents in the index: {}", document_count);
        }

        if let Ok(index_size) = get_index_size(&index_path) {
            println!("Index size: {} megabytes", index_size);
        }

        if let Ok(index) = read_index(&index_path) {
            // Do something with the read index
            println!("Index read: {:?}", index);
            
            if let Err(err) = query_index(&index) {
                // Handle the error if it occurred
                println!("Error querying the index: {:?}", err);
            }
        } else {
            // Handle the error if it occurred while reading the index
            println!("Error reading the index");
        }
    } else {
        println!("Index does not exist...");
    }
}
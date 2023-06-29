extern crate tantivy;
use tantivy::Document;
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
use std::time::Instant;

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
fn query_index(index: &Index, query: &str, limit: usize) -> tantivy::Result<Vec<Document>> {
    println!("Querying the index searching for '{:?}'", query);

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
    let query_parser = QueryParser::new(schema.clone(), vec![title_field, body_field, state_field], tokenizer_manager);
    //let query_parser = QueryParser::new(schema.clone(), vec![state_field], tokenizer_manager);

    // Parsea la consulta
    let query = query_parser.parse_query(query)?;

    // Realiza la búsqueda y obtiene los documentos más relevantes
    let top_docs: Vec<(f32, tantivy::DocAddress)> = searcher.search(&query, &TopDocs::with_limit(limit))?;

    let mut retrieved_docs: Vec<Document> = Vec::new();

    // Recorre los documentos encontrados
    for (_score, doc_address) in top_docs {
        let retrieved_doc = searcher.doc(doc_address)?;
        retrieved_docs.push(retrieved_doc);
    }

    Ok(retrieved_docs)
}


fn print_results(retrieved_docs_result: tantivy::Result<Vec<tantivy::Document>>, index: &tantivy::Index) {
    // Verifica si la consulta tuvo éxito
    if let Ok(retrieved_docs) = retrieved_docs_result {
        // Obtén el esquema del índice
        let schema = index.schema();

        // Recorre los documentos encontrados
        let mut counter = 0;
        for retrieved_doc in retrieved_docs {
            // Trabaja con cada documento según sea necesario
            println!("Result: {:?} - {}", counter, schema.to_json(&retrieved_doc));
            counter += 1;
            println!("-----------------------------------------------------------------");
        }
    } else {
        // Manejo del error en caso de que la consulta falle
        println!("Error en la consulta: {:?}", retrieved_docs_result.err());
    }
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
            println!("Index read");
            
            let query = "Amazon";
            let search_limit: usize = 20000;

            let start_time = Instant::now();
            let retrieved_docs_result = query_index(&index, query, search_limit);
            let elapsed_time = start_time.elapsed();
            println!("La consulta tomó: {:?} en ejecutarse", elapsed_time);

            if let Ok(retrieved_docs) = retrieved_docs_result.clone() {
                // Imprime la cantidad de documentos encontrados
                let num_docs = retrieved_docs.len();
                println!("Cantidad de documentos encontrados: {}", num_docs);

                //print_results(retrieved_docs_result, &index);
            } else {
                // Manejo del error en caso de que la consulta falle
                println!("Error en la consulta: {:?}", retrieved_docs_result.err());
            }
        } else {
            // Handle the error if it occurred while reading the index
            println!("Error reading the index");
        }
    } else {
        println!("Index does not exist...");
    }
}
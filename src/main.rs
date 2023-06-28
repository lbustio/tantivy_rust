#[macro_use]
extern crate tantivy;
use tantivy::collector::TopDocs;
use tantivy::directory::error::OpenReadError;
use tantivy::directory::MmapDirectory;
use tantivy::query::QueryParser;
use tantivy::schema::*;
use tantivy::Index;
use tantivy::IndexWriter;
use tantivy::ReloadPolicy;
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

/// Creates the schema for the Tantivy index.
/// Returns the created schema.
fn create_schema() -> Schema {
    // DEFINING THE SCHEMA:
    // The Tantivy index requires a very strict schema. The schema declares which fields are in the index,
    // and for each field, its type and "the way it should be indexed".
    // First, we need to define a schema...
    let mut schema_builder = Schema::builder();

    // title;url;body;state
    // Our first field is the title of the web page.
    // We want full-text search for it, and we also want to be able to retrieve the document after the search.
    // TEXT | STORED is some syntactic sugar to describe that.
    // TEXT means the field should be tokenized and indexed, along with its term frequency and term positions.
    // STORED means that the field will also be saved in a compressed, row-oriented key-value store.
    // This store is useful to reconstruct the documents that were selected during the search phase.
    schema_builder.add_text_field("title", TEXT | STORED);

    // The second field is the URL of the web page.
    // This field is non-searchable but used as metadata.
    schema_builder.add_text_field("url", STORED);

    // Our third field is the body of the web page.
    // We want full-text search for it, but we do not need to be able to retrieve it for our application.
    // We can make our index lighter by omitting the STORED flag.
    schema_builder.add_text_field("body", TEXT | STORED);

    // The fourth field is the state (if it exists) where the company that owns the URL is located.
    // This field is searchable.
    schema_builder.add_text_field("state", TEXT | STORED);

    // Create the schema
    let schema = schema_builder.build();

    schema
}

/// Creates a new index with the provided schema.
///
/// # Arguments
///
/// * `index_path` - The path where the index will be created.
/// * `schema` - The schema to be used for the index.
///
/// # Returns
///
/// Returns the created index if successful, or an error if the index creation fails.
fn create_index(index_path: &str, schema: Schema) -> Index {
    // INDEXING DOCUMENTS
    // Let's create a brand new index.
    // This will actually just save a meta.json with our schema in the directory.
    // Crear el nuevo índice

    let index = Index::create_in_dir(index_path, schema.clone()).expect("Failed to create index");

    index
}

/// Indexes the contents of a CSV file into a Tantivy index.
///
/// # Arguments
///
/// * `file_path` - The path to the CSV file.
/// * `schema` - The Tantivy schema.
/// * `index_writer` - The Tantivy index writer.
///
/// # Errors
///
/// Returns an error if there is any issue reading the CSV file or indexing the documents.
fn index_data(file_path: &str, schema: &Schema, index_writer: &mut Result<IndexWriter, TantivyError>) -> Result<(), Box<dyn std::error::Error>> {
    // Open the CSV file
    let file = std::fs::File::open(file_path)?;
    let mut reader = csv::Reader::from_reader(file);

    let mut counter = 0;

    // Iterate over the rows of the CSV
    for result in reader.records() {
        let record = result?;
        
        // Create a new document
        let mut doc = Document::default();

        // Add fields to the document
        // Check for title
        let mut  _title: String = String::new();
        if schema.get_field("title").is_ok() {
            _title = record.get(0).unwrap_or("NA").to_string();
        }
        else {
            _title = "NA".to_string();
        }
        doc.add_text(schema.get_field("title").unwrap(),_title);

        // Check for url
        let mut _url: String = String::new();
        if schema.get_field("url").is_ok() {
            _url = record.get(1).unwrap_or("NA").to_string();
        }
        else {
            _url = "NA".to_string();
        }
        doc.add_text(schema.get_field("url").unwrap(),_url);

        // Check for body
        let mut _body: String = String::new();
        if schema.get_field("body").is_ok() {
            _body = record.get(2).unwrap_or("NA").to_string();
        }
        else {
            _body = "NA".to_string();
        }
        doc.add_text(schema.get_field("body").unwrap(), _body);

        // Check for states
        let mut _state: String = String::new();
        if schema.get_field("state").is_ok() {
            _state = record.get(3).unwrap_or("NA").to_string();
        }
        else {
            let _state = "NA".to_string();
        }
        doc.add_text(schema.get_field("state").unwrap(), _state);
        
        // Add the document to the index writer
        if let Ok(ref mut writer) = *index_writer {
            writer.add_document(doc)?;
        } else {
            // Manejar el error en caso de que sea un Err
            if let Err(err) = index_writer {
                // Manejar el error
                println!("Error: {:?}", err);
            }
        }

        counter += 1;

        // Commit the changes every 1000 documents
        if counter % 1000 == 0 {
            println!("Indexing document {:?}", counter);
            // Extraer el valor del IndexWriter
            if let Ok(ref mut writer) = *index_writer {
                writer.commit()?;
            } else {
            // Manejar el error en caso de que sea un Err
                if let Err(err) = index_writer {
                    // Manejar el error
                    println!("Error: {:?}", err);
                }
            }
        }
    }

    // Commit any remaining changes
    // Extraer el valor del IndexWriter
    if let Ok(ref mut writer) = *index_writer {
        // Aquí puedes usar el index_writer
         writer.commit()?;
    } else {
        // Manejar el error en caso de que sea un Err
        if let Err(err) = index_writer {
            // Manejar el error
            println!("Error: {:?}", err);
        }
    }     

    Ok(())
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

fn main() {
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
    } else {
        println!("Creating the schema for the index...");
        let schema = create_schema();

        println!("Creating the index...");
        let index = create_index(&index_path, schema.clone());
        println!("Index created successfully!");

        let mut index_writer = index.writer(50_000_000);
        let data_path = "data/Libro1.csv";
        match index_data(data_path, &schema, &mut index_writer) {
            Ok(()) => println!("CSV file indexed successfully!"),
            Err(err) => eprintln!("Error indexing CSV file: {:?}", err),
        }
    }
}

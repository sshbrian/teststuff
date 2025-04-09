use std::fs::File;
use std::io::{self, BufRead, Cursor};
use std::path::Path;
use std::sync::{Arc, atomic::{AtomicUsize, Ordering}};
use std::error::Error;

use apache_avro::{Schema, Reader};
use futures::stream::{self, StreamExt};
use reqwest::Client;
use tokio::task;

// Avro schema definition
const AVRO_SCHEMA: &str = r#"
{
  "type": "record",
  "name": "Example",
  "fields": [
    {"name": "field1", "type": "string"},
    {"name": "field2", "type": "int"}
  ]
}
"#;

// Read list of URLs from a file
fn read_urls_from_file<P: AsRef<Path>>(path: P) -> io::Result<Vec<String>> {
    let file = File::open(path)?;
    let reader = io::BufReader::new(file);
    reader.lines().collect()
}

// Download and decode Avro data from a URL using Reader
async fn process_avro_url(
    url: String,
    schema: Arc<Schema>,
    global_counter: Arc<AtomicUsize>,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let client = Client::new();
    let response = client.get(&url).send().await?;

    if !response.status().is_success() {
        return Err(format!("HTTP error: {}", response.status()).into());
    }

    let bytes = response.bytes().await?;

    let schema_clone = schema.clone();
    let url_clone = url.clone();
    let global_counter_clone = global_counter.clone();

    task::spawn_blocking(move || -> Result<(), Box<dyn Error + Send + Sync>> {
        let cursor = Cursor::new(bytes.to_vec());
        let reader = Reader::with_schema(&schema_clone, cursor)?;

        let count = reader.count();
        global_counter_clone.fetch_add(count, Ordering::Relaxed);

        println!("✅ URL: {} - Records: {}", url_clone, count);

        Ok(())
    }).await??;

    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let url_file_path = "urls.txt";
    let urls = read_urls_from_file(url_file_path)?;

    let schema = Arc::new(Schema::parse_str(AVRO_SCHEMA)?);
    let total_counter = Arc::new(AtomicUsize::new(0));

    let concurrency_limit = 100;
    let tasks: Vec<_> = urls.into_iter().map(|url| {
        let schema = Arc::clone(&schema);
        let global_counter = Arc::clone(&total_counter);
        task::spawn(async move {
            if let Err(e) = process_avro_url(url.clone(), schema, global_counter).await {
                eprintln!("❌ Error processing URL {}: {}", url, e);
            }
        })
    }).collect();

    let mut stream = stream::iter(tasks).buffer_unordered(concurrency_limit);
    while let Some(result) = stream.next().await {
        if let Err(e) = result {
            eprintln!("❌ Task error: {}", e);
        }
    }

    let final_total = total_counter.load(Ordering::Relaxed);
    println!("\n🎉 Total records across all files: {}", final_total);

    Ok(())
}

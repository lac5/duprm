extern crate glob;
extern crate md5;
extern crate trash;

mod remove_tags;

use glob::glob;
use md5::Digest;
use remove_tags::remove_tags_from_buffer;
use std::error::Error;
use std::fs;
use std::fs::File;
use std::io::prelude::*;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::SystemTime;

struct Row {
    time: SystemTime,
    md5: Digest,
    path: PathBuf,
}

fn main() {
    let found: Arc<Mutex<Vec<Row>>> = Arc::new(Mutex::new(vec![]));

    for entry in glob("**/*.mp3").unwrap() {
        let found = Arc::clone(&found);
        thread::spawn(move || {
            let row = make_row(entry).unwrap();
            let mut found = found.lock().unwrap();
            check_found(&mut found, row).unwrap();
        });
    }
}

fn make_row(entry: Result<PathBuf, glob::GlobError>) -> Result<Row, Box<dyn Error>> {
    let path = entry?;
    let mut f = File::open(path.clone())?;
    let mut buffer = Vec::new();
    f.read_to_end(&mut buffer)?;
    let buffer = remove_tags_from_buffer(buffer).ok_or("Tags could not be removed")?;
    let md5 = md5::compute(buffer);
    let metadata = fs::metadata(path.clone())?;
    let time = metadata.created()?;

    println!("{:x} {}", md5, path.display());

    return Ok(Row { time, md5, path });
}

fn check_found(found: &mut Vec<Row>, row1: Row) -> Result<(), Box<dyn Error>> {
    let mut deleted = false;
    for (i, row2) in found.iter().enumerate() {
        if row1.md5 == row2.md5 {
            println!("match ({:x}):", row1.md5);
            if row1.time > row2.time {
                println!("trash#2 -> {}", row2.path.display());
                trash::delete(row2.path.clone())?;
                found.remove(i);
            } else {
                println!("trash#1 -> {}", row1.path.display());
                trash::delete(row1.path.clone())?;
                deleted = true;
            }
            break;
        }
    }
    if !deleted {
        found.push(row1);
    }
    Ok(())
}

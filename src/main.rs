mod remove_tags;

use glob::glob;
use md5::Digest;
use owo_colors::OwoColorize;
use remove_tags::remove_tags_from_buffer;
use std::error::Error;
use std::fs;
use std::fs::File;
use std::io::prelude::*;
use std::path::PathBuf;
use std::sync::mpsc::channel;
use std::sync::{Arc, Mutex};
use std::time::SystemTime;
use threadpool::ThreadPool;

struct Row {
    time: SystemTime,
    md5: Digest,
    path: PathBuf,
}

fn main() {
    let found: Arc<Mutex<Vec<Row>>> = Arc::new(Mutex::new(vec![]));
    let pool = ThreadPool::default();
    let mut jobs = 0;
    let (tx, rx) = channel();

    for entry in glob("**/*.mp3").unwrap() {
        let found = Arc::clone(&found);
        let tx = tx.clone();
        jobs += 1;
        pool.execute(move || {
            let do_stuff = || -> Result<(), Box<dyn Error>> {
                let row = make_row(entry)?;
                let mut found = found.lock()?;
                check_found(&mut found, row)?;
                Ok(())
            };
            if let Err(e) = do_stuff() {
                eprintln!("ERR: {:?}", e);
            }
            if let Err(e) = tx.send(1) {
                eprintln!("ERR: {:?}", e);
            }
        });
    }

    if jobs != rx.iter().take(jobs).fold(0, |a, b| a + b) {
        panic!("some jobs didn't finish");
    } else {
        println!("done");
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

    println!("{:x} {}", md5.yellow(), path.display().green());

    return Ok(Row { time, md5, path });
}

fn check_found(found: &mut Vec<Row>, row1: Row) -> Result<(), Box<dyn Error>> {
    let mut deleted = false;
    for (i, row2) in found.iter().enumerate() {
        if row1.md5 == row2.md5 {
            println!("match ({:x}):", row1.md5.yellow());
            if row1.time > row2.time {
                println!("trash#2 -> {}", row2.path.display().blue());
                trash::delete(row2.path.clone())?;
                found.remove(i);
            } else {
                println!("trash#1 -> {}", row1.path.display().green());
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

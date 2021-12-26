mod remove_tags;

use glob::glob;
use md5::Digest;
use owo_colors::OwoColorize;
use remove_tags::remove_tags_from_buffer;
use std::collections::HashMap;
use std::error::Error;
use std::fs;
use std::fs::File;
use std::io::prelude::*;
use std::path::PathBuf;
use std::sync::mpsc::channel;
use std::sync::{Arc, Mutex};
use std::time::SystemTime;
use threadpool::ThreadPool;

fn main() {
    let mut found = 0;
    let md5_map = Arc::new(Mutex::new(HashMap::<Digest, (SystemTime, PathBuf)>::new()));
    let pool = ThreadPool::default();
    let (tx, rx) = channel();

    for entry in glob("**/*.mp3").unwrap() {
        found += 1;
        let index = found.clone();
        let md5_map = Arc::clone(&md5_map);
        let tx = tx.clone();
        pool.execute(move || {
            let (md5, time, path) = make_row(entry).unwrap();
            let mut md5_map = md5_map.lock().unwrap();
            println!("{}. {:x} {}", index, md5.yellow(), path.display().green());
            if let Some((time2, path2)) = md5_map.get(&md5) {
                println!("{}. match ({:x}):", index, md5.yellow());
                if time > *time2 {
                    println!("{}. trash -> {}", index, path2.display().blue());
                    if let Err(e) = trash::delete(path2.clone()) {
                        eprintln!("{}. {}{:?}", index, "ERR: ".red(), e);
                    }
                    md5_map.insert(md5, (time, path));
                } else {
                    println!("{} trash -> {}", index, path.display().green());
                    if let Err(e) = trash::delete(path.clone()) {
                        eprintln!("{} {}{:?}", index, "ERR: ".red(), e);
                    }
                }
            } else {
                md5_map.insert(md5, (time, path));
            }
            tx.send(1).unwrap();
        });
    }

    println!("...{}", rx.iter().take(found).fold(0, |a, b| a + b));
    println!("done");
}

fn make_row(
    entry: Result<PathBuf, glob::GlobError>,
) -> Result<(Digest, SystemTime, PathBuf), Box<dyn Error>> {
    let path = entry?;
    let mut f = File::open(path.clone())?;
    let mut buffer = Vec::new();
    f.read_to_end(&mut buffer)?;
    let buffer = remove_tags_from_buffer(buffer).ok_or("Tags could not be removed")?;
    let md5 = md5::compute(buffer);
    let metadata = fs::metadata(path.clone())?;
    let time = metadata.created()?;

    return Ok((md5, time, path));
}

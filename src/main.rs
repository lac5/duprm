macro_rules! try_or_continue {
    ($x:expr) => {
        match $x {
            Ok(x) => x,
            Err(e) => {
                eprintln!("ERR {:?}", e);
                continue;
            }
        }
    };
}

macro_rules! try_or_log {
    ($x:expr) => {
        match $x {
            Ok(x) => x,
            Err(e) => {
                eprintln!("ERR {:?}", e);
            }
        }
    };
}

macro_rules! some_or_continue {
    ($x:expr) => {
        match $x {
            Some(x) => x,
            None => continue,
        }
    };
}

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
use std::time::SystemTime;

struct Row {
    time: SystemTime,
    md5: Digest,
    path: PathBuf,
}

fn main() -> Result<(), Box<dyn Error>> {
    let mut found: Vec<Row> = vec![];

    for entry in glob("**/*.mp3")? {
        let path = try_or_continue!(entry);
        let mut f = File::open(path.clone())?;
        let mut buffer = Vec::new();
        f.read_to_end(&mut buffer)?;
        let buffer = some_or_continue!(remove_tags_from_buffer(buffer));
        let md5 = md5::compute(buffer);
        let metadata = try_or_continue!(fs::metadata(path.clone()));
        let time = try_or_continue!(metadata.created());

        let row1 = Row { time, md5, path };
        let mut deleted = false;
        for (i, row2) in found.iter().enumerate() {
            if row1.md5 == row2.md5 {
                // if file1 is newer than file2:
                if row1.time > row2.time {
                    try_or_log!(trash::delete(row2.path.clone()));
                    found.remove(i);
                } else {
                    try_or_log!(trash::delete(row1.path.clone()));
                    deleted = true;
                }
                break;
            }
        }
        if !deleted {
            found.push(row1);
        }
    }

    Ok(())
}

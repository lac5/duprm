macro_rules! etry {
    ($x:expr) => {
        match $x {
            Ok(x) => x,
            Err(e) => {
                eprintln!("ERR {:?}", e);
            }
        }
    };
    ($x:expr; $y:expr) => {
        match $x {
            Ok(x) => x,
            Err(e) => {
                eprintln!("ERR {:?}", e);
                $y
            }
        }
    };
}

mod remove_tags;

use async_std::fs;
use async_std::fs::Metadata;
use futures::executor::block_on;
use futures::future::join_all;
use futures::join;
use glob::glob;
use glob::GlobResult;
use md5::Digest;
use owo_colors::OwoColorize;
use remove_tags::remove_tags_from_buffer;
use std::cmp;
use std::collections::HashMap;
use std::error::Error;
use std::path::PathBuf;
use std::sync::mpsc::channel;
use std::sync::mpsc::Sender;
use std::time::SystemTime;
use threadpool::ThreadPool;

type FileData = (
    PathBuf,
    async_std::io::Result<Vec<u8>>,
    async_std::io::Result<Metadata>,
);
type Data = (Digest, SystemTime, PathBuf);
type Md5Map = HashMap<Digest, (SystemTime, PathBuf)>;

fn main() {
    println!("searching for mp3 files");
    let results: Vec<GlobResult> = glob("**/*.mp3").unwrap().collect();
    println!("search complete");

    let pool = ThreadPool::default();
    let thread_count = pool.max_count();
    let per_row = cmp::max(1, 128 / thread_count);

    let (tx1, rx1) = channel::<Option<Data>>();

    let mut row = vec![];
    let mut total = 0;
    for entry in results {
        let path = etry!(entry; continue);
        row.push(path);
        total += 1;
        if total % per_row == 0 {
            println!("{}...", total);
            let tx1 = tx1.clone();
            pool.execute(move || crunch_data(row, tx1));
            row = vec![];
        }
    }
    pool.execute(move || crunch_data(row, tx1));
    let total = total;
    println!("{} files found", total);

    let (tx2, rx2) = channel::<i32>();
    let mut md5_map = Md5Map::new();
    let mut index = 0;
    let mut jobs = 0;
    println!("reading files");
    for entry in rx1.iter().take(total) {
        if let Some((md5, time, path)) = entry {
            index += 1;
            if let Some(path) = etry!(insert_md5(index, &mut md5_map, md5, time, path); continue) {
                jobs += 1;
                let tx2 = tx2.clone();
                let index = jobs.clone();
                pool.execute(move || {
                    println!("trash#{} -> {}", index.cyan(), path.display().dimmed());
                    etry!(trash::delete(path); {
                        etry!(tx2.send(0));
                        return;
                    });
                    etry!(tx2.send(1));
                });
            }
        }
    }

    println!(
        "moved {} files to the trash",
        rx2.iter().take(jobs).fold(0, |a, b| a + b)
    );

    println!("done");
}

fn crunch_data(row: Vec<PathBuf>, tx: Sender<Option<Data>>) {
    let mut futures = vec![];
    for path in row {
        futures.push(fetch_data(path));
    }
    for (path, buffer, metadata) in block_on(join_all(futures)) {
        let buffer = etry!(buffer; {
            etry!(tx.send(None));
            continue;
        });
        let metadata = etry!(metadata; {
            etry!(tx.send(None));
            continue;
        });
        let time = etry!(metadata.created(); {
            etry!(tx.send(None));
            continue;
        });
        let buffer = etry!(remove_tags_from_buffer(buffer).ok_or("Tags could not be removed"); {
            etry!(tx.send(None));
            continue;
        });
        let md5 = md5::compute(buffer);
        etry!(tx.send(Some((md5, time, path))));
    }
}

async fn fetch_data(path: PathBuf) -> FileData {
    let f_read = fs::read(path.clone());
    let f_metadata = fs::metadata(path.clone());
    let (r_read, r_metadata) = join!(f_read, f_metadata);
    (path, r_read, r_metadata)
}

fn insert_md5(
    index: usize,
    md5_map: &mut Md5Map,
    md5: Digest,
    time: SystemTime,
    path: PathBuf,
) -> Result<Option<PathBuf>, Box<dyn Error>> {
    if let Some((time2, path2)) = md5_map.get(&md5) {
        println!("{} match {:x}:", index.cyan(), md5.yellow());
        if time > *time2 {
            println!("{} keep -> {}", index.cyan(), path.display().purple());
            let path2 = path2.clone();
            md5_map.insert(md5, (time, path));
            Ok(Some(path2))
        } else {
            println!("{} keep -> {}", index.cyan(), path2.display().blue());
            Ok(Some(path))
        }
    } else {
        println!(
            "{} {:x} {}",
            index.cyan(),
            md5.yellow(),
            path.display().dimmed()
        );
        md5_map.insert(md5, (time, path));
        Ok(None)
    }
}

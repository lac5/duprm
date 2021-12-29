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
use async_std::fs::remove_file;
use async_std::fs::Metadata;
use clap::App;
use futures::executor::block_on;
use futures::join;
use futures_polling::{FuturePollingExt, Polling};
use glob::glob;
use glob::GlobResult;
use md5::Digest;
use num_integer::Integer;
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
    let _ = App::new("duprm")
        .version("1.0")
        .about("Removes duplicate MP3 files.")
        .get_matches();

    println!("searching for mp3 files");
    let results: Vec<GlobResult> = glob("**/*.mp3").unwrap().collect();
    println!("search complete");

    let pool = ThreadPool::default();
    let thread_count = pool.max_count();
    let max_files = cmp::max(1, 64.div_ceil(&thread_count));

    let (tx1, rx1) = channel::<Option<Data>>();

    let mut files = vec![vec![]; thread_count];
    let mut total = 0;
    for entry in results {
        let path = etry!(entry; continue);
        files[total % thread_count].push(path);
        total += 1;
        if total % thread_count == 0 {
            println!("{}...", total);
        }
    }
    for mut row in files {
        let tx1 = tx1.clone();
        pool.execute(move || {
            row.reverse();
            crunch_data(row, max_files, tx1);
        });
    }
    let total = total;
    println!("{} files found", total);

    let mut md5_map = Md5Map::new();
    let mut index = 0;
    let mut trashed = 0;
    let mut trash_polls = vec![];
    let mut poll_tash = |trash_polls: Vec<(usize, PathBuf, Polling<_>)>| {
        let mut new_trash_polls = vec![];
        for (index, path, mut poll) in trash_polls {
            block_on(poll.polling_once());
            if poll.is_pending() {
                new_trash_polls.push((index, path, poll));
            } else {
                etry!(block_on(poll); continue);
                trashed += 1;
                println!(
                    "{} trash#{} -> {}",
                    index.cyan(),
                    trashed.cyan(),
                    path.display().dimmed()
                );
            }
        }
        return new_trash_polls;
    };

    println!("reading files");
    for entry in rx1.iter().take(total) {
        if let Some((md5, time, path)) = entry {
            index += 1;
            if let Some(path) = etry!(insert_md5(index, &mut md5_map, md5, time, path); continue) {
                let poll = remove_file(path.clone()).polling();
                trash_polls.push((index, path, poll));
            }
        }
        if trash_polls.len() > 0 {
            trash_polls = poll_tash(trash_polls);
        }
    }
    while trash_polls.len() > 0 {
        trash_polls = poll_tash(trash_polls);
    }

    println!("deleted {} files", trashed);

    println!("done");
}

fn crunch_data(mut files: Vec<PathBuf>, max_files: usize, tx: Sender<Option<Data>>) {
    let mut polls: Vec<Polling<_>> = vec![];
    macro_rules! next {
        ($x:expr) => {
            match files.pop() {
                Some(path) => fetch_data(path).polling(),
                None => $x,
            }
        };
    }
    while polls.len() < max_files {
        polls.push(next!(break));
    }
    while polls.len() > 0 {
        let mut next_polls = vec![];
        for mut poll in polls {
            block_on(poll.polling_once());
            match poll {
                Polling::Pending(f) => {
                    next_polls.push(f.polling());
                }
                Polling::Ready((path, buffer, metadata)) => {
                    let buffer = etry!(buffer; {
                        etry!(tx.send(None));
                        next_polls.push(next!(continue));
                        continue;
                    });
                    let metadata = etry!(metadata; {
                        etry!(tx.send(None));
                        next_polls.push(next!(continue));
                        continue;
                    });
                    let time = etry!(metadata.created(); {
                        etry!(tx.send(None));
                        next_polls.push(next!(continue));
                        continue;
                    });
                    let buffer = etry!(remove_tags_from_buffer(buffer).ok_or("Tags could not be removed"); {
                        etry!(tx.send(None));
                        next_polls.push(next!(continue));
                        continue;
                    });
                    let md5 = md5::compute(buffer);
                    etry!(tx.send(Some((md5, time, path))));
                    next_polls.push(next!(continue));
                }
                Polling::Done => {}
            }
        }
        polls = next_polls;
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
            println!("{} insert -> {}", index.cyan(), path.display().purple());
            let path2 = path2.clone();
            md5_map.insert(md5, (time, path));
            Ok(Some(path2))
        } else {
            println!("{} keep -> {}", index.cyan(), path2.display().blue());
            Ok(Some(path))
        }
    } else {
        println!(
            "{} insert {:x}: {}",
            index.cyan(),
            md5.yellow(),
            path.display().dimmed()
        );
        md5_map.insert(md5, (time, path));
        Ok(None)
    }
}

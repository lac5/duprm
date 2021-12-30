macro_rules! etry {
    (?$x:expr) => {
        match $x {
            Ok(x) => Some(x),
            Err(e) => {
                eprintln!("ERR {}", e);
                None
            }
        }
    };
    ($x:expr) => {
        match $x {
            Ok(x) => x,
            Err(e) => {
                eprintln!("ERR {}", e);
            }
        }
    };
    ($x:expr; $y:expr) => {
        match $x {
            Ok(x) => x,
            Err(e) => {
                eprintln!("ERR {}", e);
                $y
            }
        }
    };
}

mod remove_tags;

use async_std::channel::{unbounded as async_channel, Sender};
use async_std::fs::{self, remove_file, Metadata};
use clap::{App, Arg};
use futures::executor::block_on;
use futures::future::join_all;
use futures::join;
use futures_polling::{FuturePollingExt, Polling};
use glob::glob;
use glob::GlobResult;
use md5::Digest;
use num_integer::Integer;
use remove_tags::remove_tags_from_buffer;
use std::cmp;
use std::collections::HashMap;
use std::error::Error;
use std::io::Write;
use std::path::PathBuf;
use std::sync::mpsc::channel;
use std::time::SystemTime;
use termcolor::{Color, ColorChoice, ColorSpec, StandardStream, WriteColor};
use threadpool::ThreadPool;

type FileData = (
    PathBuf,
    async_std::io::Result<Vec<u8>>,
    async_std::io::Result<Metadata>,
);
type Data = (Digest, SystemTime, PathBuf);
type Md5Map = HashMap<Digest, (SystemTime, PathBuf)>;

fn main() {
    let matches = App::new("duprm")
        .version("1.0")
        .about("Removes duplicate MP3 files.")
        .arg(
            Arg::with_name("oldest")
                .short("o")
                .help("Keep older files and delete newer files"),
        )
        .get_matches();

    let keep_older = matches.is_present("oldest");

    let pool = ThreadPool::default();
    let thread_count = pool.max_count();
    let max_files = cmp::max(1, 64.div_ceil(&thread_count));

    let (tx1, rx1) = channel::<(Sender<_>, Option<Data>)>();
    let mut channels = vec![];
    for _ in 0..thread_count {
        channels.push(async_channel::<Option<GlobResult>>());
    }

    let mut files = glob("**/*.mp3").unwrap();
    let mut files_total = 0;

    {
        let mut senders = vec![];
        for (tx, rx) in channels.into_iter() {
            let tx1 = tx1.clone();
            let tx_clone = tx.clone();
            pool.execute(move || {
                let mut paths = vec![];
                let mut next = rx.recv().polling();
                let mut polls = vec![];
                let mut done = false;
                while !done || polls.len() > 0 || paths.len() > 0 {
                    if next.is_pending() {
                        block_on(next.polling_once());
                        if next.is_ready() {
                            if let Some(result) = next.take_ready() {
                                if let Some(data) = etry!(?result) {
                                    if let Some(glob_result) = data {
                                        if let Some(path) = etry!(?glob_result) {
                                            done = false;
                                            if polls.len() < max_files {
                                                polls.push(crunch_data(path).polling());
                                            } else {
                                                paths.push(path);
                                            }
                                        }
                                    } else {
                                        done = true;
                                    }
                                }
                            }
                            next = rx.recv().polling();
                        }
                    }
                    for poll in polls.iter_mut() {
                        if poll.is_pending() {
                            block_on(poll.polling_once());
                            if poll.is_ready() {
                                if let Some(result) = poll.take_ready() {
                                    let tx = tx.clone();
                                    etry!(tx1.send((tx, etry!(?result))));
                                }
                                if let Some(path) = paths.pop() {
                                    *poll = crunch_data(path).polling();
                                }
                            }
                        }
                    }
                    polls.retain(|poll| !poll.is_done());
                }
                println!("thread done");
            });
            senders.push(tx_clone);
        }

        let mut futures = vec![];
        for i in 0..(max_files * thread_count) {
            let send_data = files.next();
            if let Some(_) = send_data {
                files_total += 1;
            }
            futures.push(senders[i % thread_count].send(send_data));
        }
        block_on(join_all(futures));
    }

    let mut md5_map = Md5Map::new();
    let mut index = 0usize;
    let mut trashed = 0usize;
    let mut trash_polls = vec![];

    let mut stdout = StandardStream::stdout(ColorChoice::Always);
    let mut poll_tash = |trash_polls: &mut Vec<(usize, PathBuf, Polling<_>)>| {
        for (index, path, poll) in trash_polls.iter_mut() {
            block_on(poll.polling_once());
            if poll.is_ready() {
                let _ = poll.take_ready();
                trashed += 1;
                etry!(stdout.set_color(ColorSpec::new().set_fg(Some(Color::Cyan))));
                etry!(write!(&mut stdout, "{}", index));
                etry!(stdout.reset());
                etry!(write!(&mut stdout, " trash#"));
                etry!(stdout.set_color(ColorSpec::new().set_fg(Some(Color::Cyan))));
                etry!(write!(&mut stdout, "{}", trashed));
                etry!(stdout.reset());
                etry!(write!(&mut stdout, " -> "));
                etry!(stdout.set_color(ColorSpec::new().set_dimmed(true)));
                etry!(write!(&mut stdout, "{}", path.display()));
                etry!(stdout.reset());
                etry!(writeln!(&mut stdout, ""));
            }
        }
        trash_polls.retain(|(_, _, poll)| !poll.is_done());
    };

    let mut stdout = StandardStream::stdout(ColorChoice::Always);
    let mut insert_md5 = |index: usize,
                          md5_map: &mut Md5Map,
                          md5: Digest,
                          time: SystemTime,
                          path: PathBuf|
     -> Result<Option<PathBuf>, Box<dyn Error>> {
        etry!(stdout.set_color(ColorSpec::new().set_fg(Some(Color::Cyan))));
        etry!(write!(&mut stdout, "{}", index));
        etry!(stdout.reset());
        if let Some((time2, path2)) = md5_map.get(&md5) {
            etry!(write!(&mut stdout, " match "));
            etry!(stdout.set_color(ColorSpec::new().set_fg(Some(Color::Yellow))));
            etry!(write!(&mut stdout, "{:x}", md5));
            etry!(stdout.reset());
            etry!(write!(&mut stdout, ": "));
            let result = if keep_older == (time > *time2) {
                etry!(write!(&mut stdout, "keep -> "));
                etry!(stdout.set_color(ColorSpec::new().set_fg(Some(Color::Magenta))));
                etry!(write!(&mut stdout, "{}", path2.display()));
                Ok(Some(path))
            } else {
                etry!(write!(&mut stdout, "insert -> "));
                etry!(stdout.set_color(ColorSpec::new().set_fg(Some(Color::Blue))));
                etry!(write!(&mut stdout, "{}", path.display()));

                let path2 = path2.clone();
                md5_map.insert(md5, (time, path));

                Ok(Some(path2))
            };

            etry!(stdout.reset());
            etry!(writeln!(&mut stdout, ""));
            result
        } else {
            etry!(write!(&mut stdout, " insert "));
            etry!(stdout.set_color(ColorSpec::new().set_fg(Some(Color::Yellow))));
            etry!(write!(&mut stdout, "{:x}", md5));
            etry!(stdout.reset());
            etry!(write!(&mut stdout, ": "));
            etry!(stdout.set_color(ColorSpec::new().set_dimmed(true)));
            etry!(write!(&mut stdout, "{}", path.display()));
            etry!(stdout.reset());
            etry!(writeln!(&mut stdout, ""));
            md5_map.insert(md5, (time, path));
            Ok(None)
        }
    };

    println!("reading files");
    let mut read_total = 0;
    while read_total < files_total {
        read_total += 1;
        let entry = rx1.recv().unwrap();
        let (tx, entry) = entry;
        if let Some((md5, time, path)) = entry {
            index += 1;
            if let Some(path) = etry!(insert_md5(index, &mut md5_map, md5, time, path); continue) {
                let poll = remove_file(path.clone()).polling();
                trash_polls.push((index, path, poll));
            }
        }
        if trash_polls.len() > 0 {
            poll_tash(&mut trash_polls);
        }
        if !tx.is_closed() {
            let send_data = files.next();
            if let Some(_) = send_data {
                files_total += 1;
            }
            etry!(block_on(tx.send(send_data)));
        }
    }
    while trash_polls.len() > 0 {
        poll_tash(&mut trash_polls);
    }

    println!("deleted {} out of {} files", trashed, files_total);

    println!("done");
}

async fn crunch_data(path: PathBuf) -> Result<Data, Box<dyn Error>> {
    let file_data = fetch_data(path).await;
    let data = try_data(file_data)?;
    Ok(data)
}

async fn fetch_data(path: PathBuf) -> FileData {
    let f_read = fs::read(path.clone());
    let f_metadata = fs::metadata(path.clone());
    let (r_read, r_metadata) = join!(f_read, f_metadata);
    (path, r_read, r_metadata)
}

fn try_data(data: FileData) -> Result<Data, Box<dyn Error>> {
    let (path, buffer, metadata) = data;
    let buffer = buffer?;
    let metadata = metadata?;
    let time = metadata.created()?;
    let buffer = remove_tags_from_buffer(buffer).ok_or("Tags could not be removed")?;
    let md5 = md5::compute(buffer);
    Ok((md5, time, path))
}

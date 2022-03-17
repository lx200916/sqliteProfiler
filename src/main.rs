use std::fs;
use std::process::Command;
use std::sync::atomic::{AtomicI64, AtomicU64};
use std::time::Duration;
use rusqlite::{Connection};
use fake::{Dummy, Fake, Faker};
use fake::faker::name::zh_cn::*;
use fake::faker::lorem::en::*;
use fake::faker::phone_number::en::*;
use flexi_logger::{FileSpec, Logger, WriteMode};
use log::{info};
use rusqlite::DatabaseName::Main;
use structopt::StructOpt;

static TIMES: AtomicU64 = AtomicU64::new(0);
static TOTAL_TIME: AtomicU64 = AtomicU64::new(0);

type ProfileFn = fn(&str, Duration);

#[derive(StructOpt, Debug)]
#[structopt(name = "sqliteProfiler")]
struct Opt {
    /// Start Logger.
    #[structopt(short, long)]
    debug: bool,
    /// Do NOT Free CACHE.
    #[structopt(short, long)]
    free: bool,
    /// Benchmark Time. Default is 5s.
    #[structopt(short, long, default_value = "5")]
    time: u32,

}

fn profile(s: &str, dur: Duration) {
    info!("'{}' took {} m seconds", s, dur.as_micros());
    if dur.as_micros() > 0 {
        TIMES.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        TOTAL_TIME.fetch_add(dur.as_micros() as u64, std::sync::atomic::Ordering::SeqCst);
    }
}

#[cfg(target_os = "macos")]
fn free() {
    Command::new("sync").output().unwrap();
    Command::new("purge").output().unwrap();

    info!("free!");
}

#[cfg(all(not(target_os = "windows"), not(target_os = "macos")))]
fn free() {
    Command::new("sync").output().unwrap();
    fs::write("/proc/sys/vm/drop_caches", "3").unwrap();
    info!("free!");
}

#[cfg(target_os = "windows")]
fn free() {}


fn main() {
    let opt: Opt = Opt::from_args();
    if opt.debug {
        let _logger = Logger::try_with_str("info, my::critical::module=trace").unwrap()
            .log_to_file(FileSpec::default())
            .write_mode(WriteMode::BufferAndFlush)
            .start().unwrap();
    }
    // set_logger(&_logger).unwrap();
    let times = opt.time as u64;

    let mut conn = rusqlite::Connection::open("test.db").unwrap();
    let p_fn: ProfileFn = profile;
    conn.profile(Option::from(p_fn));

    conn.execute("CREATE TABLE IF NOT EXISTS test (id INTEGER PRIMARY KEY, name TEXT NOT NULL,phone TEXT,uid INTEGER,description TEXT )", []).unwrap();
    conn.execute("PRAGMA cache_size=-0", []).unwrap();
    let version: i32 = conn.pragma_query_value(Some(Main), "cache_size", |r| r.get(0)).unwrap();
    println!("cache_size: {}", version);


//  INSERT TEST
    TIMES.store(0, std::sync::atomic::Ordering::SeqCst);
    TOTAL_TIME.store(0, std::sync::atomic::Ordering::SeqCst);
    let instant = std::time::Instant::now();
    let stmt = conn.prepare_cached("INSERT INTO test (name,phone,uid,description) VALUES (?,?,?,?)").unwrap();

    loop {
        if instant.elapsed().as_secs() >= times {
            break;
        }
        conn.cache_flush();
        let mut stmt = conn.prepare_cached("INSERT INTO test (name,phone,uid,description) VALUES (?1,?2,?3,?4)").unwrap();
        let params = &[&Name().fake::<String>(), &PhoneNumber().fake::<String>(), &(1000000000..4000000000).fake::<u32>().to_string(), &Paragraph(100..300).fake()];
        // println!("{:?}",params);
        stmt.execute(params).unwrap();
        // println!("u32 {:?}", );
        if !opt.free {
            free();
        }
    }

    println!("INSERT {} {} {}", TIMES.load(std::sync::atomic::Ordering::SeqCst), TOTAL_TIME.load(std::sync::atomic::Ordering::SeqCst), TOTAL_TIME.load(std::sync::atomic::Ordering::SeqCst) / TIMES.load(std::sync::atomic::Ordering::SeqCst));
    //Insert 10k records
    for _ in 1..10000 {
        let mut stmt = conn.prepare_cached("INSERT INTO test (name,phone,uid,description) VALUES (?1,?2,?3,?4)").unwrap();
        let params = &[&Name().fake::<String>(), &PhoneNumber().fake::<String>(), &(1000000000..4000000000).fake::<u32>().to_string(), &Paragraph(100..300).fake()];
        // println!("{:?}",params);
        stmt.execute(params).unwrap();
    }
//  UPDATE TEST
    TIMES.store(0, std::sync::atomic::Ordering::SeqCst);
    TOTAL_TIME.store(0, std::sync::atomic::Ordering::SeqCst);
    let instant = std::time::Instant::now();
    let stmt = conn.prepare_cached("UPDATE test SET description= ?1 where id = ?2").unwrap();

    loop {
        if instant.elapsed().as_secs() >= times {
            break;
        }
        if !opt.free {
            free();
        }
        conn.cache_flush();
        let mut stmt = conn.prepare_cached("UPDATE test SET description= ?1 where id = ?2").unwrap();
        let params = &[&Paragraph(300..400).fake(), &(1..10000).fake::<i16>().to_string()];

        // println!("{:?}",params.last());
        stmt.execute(params).unwrap();

        // println!("u32 {:?}", );
    }
    println!("UPDATE {} {} {}", TIMES.load(std::sync::atomic::Ordering::SeqCst), TOTAL_TIME.load(std::sync::atomic::Ordering::SeqCst), TOTAL_TIME.load(std::sync::atomic::Ordering::SeqCst) / TIMES.load(std::sync::atomic::Ordering::SeqCst));
//  SELECT TEST
    TIMES.store(0, std::sync::atomic::Ordering::SeqCst);
    TOTAL_TIME.store(0, std::sync::atomic::Ordering::SeqCst);
    let instant = std::time::Instant::now();
    let stmt = conn.prepare_cached("SELECT * FROM test where id = ?1").unwrap();

    loop {
        if instant.elapsed().as_secs() >= times {
            break;
        }

        if !opt.free {
            free();
        }


        conn.cache_flush();
        let mut stmt = conn.prepare_cached("SELECT * FROM test WHERE id =?1").unwrap();
        let params = &[&(1..10000).fake::<i16>().to_string()];
        // println!("{:?}",params);
        stmt.query(params).unwrap().next().is_ok();
        // println!("u32 {:?}", );
    }
    println!("SELECT {} {} {}", TIMES.load(std::sync::atomic::Ordering::SeqCst), TOTAL_TIME.load(std::sync::atomic::Ordering::SeqCst), TOTAL_TIME.load(std::sync::atomic::Ordering::SeqCst) / TIMES.load(std::sync::atomic::Ordering::SeqCst));

//  DELETE TEST
    TIMES.store(0, std::sync::atomic::Ordering::SeqCst);
    TOTAL_TIME.store(0, std::sync::atomic::Ordering::SeqCst);
    let instant = std::time::Instant::now();
    let stmt = conn.prepare_cached("DELETE FROM test WHERE id = ?1").unwrap();

    loop {
        if instant.elapsed().as_secs() >= times {
            break;
        }
        if !opt.free {
            free();
        }
        conn.cache_flush();
        let mut stmt = conn.prepare_cached("DELETE FROM test WHERE id = ?1").unwrap();
        let params = &[&(1..10000).fake::<i16>().to_string()];
        // println!("{:?}",params);
        stmt.execute(params).unwrap();
        // println!("u32 {:?}", );
    }
    println!("DELETE {} {} {}", TIMES.load(std::sync::atomic::Ordering::SeqCst), TOTAL_TIME.load(std::sync::atomic::Ordering::SeqCst), TOTAL_TIME.load(std::sync::atomic::Ordering::SeqCst) / TIMES.load(std::sync::atomic::Ordering::SeqCst));
}

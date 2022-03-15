use std::sync::atomic::{AtomicI64, AtomicU64};
use std::time::Duration;
use rusqlite::{Connection};
use fake::{Dummy, Fake, Faker};
use fake::faker::name::zh_cn::*;
use fake::faker::lorem::en::*;
use fake::faker::phone_number::en::*;

static TIMES: AtomicU64 = AtomicU64::new(0);
static TOTAL_TIME: AtomicU64 = AtomicU64::new(0);

type ProfileFn = fn(&str, Duration);

fn profile(s: &str, dur: Duration) {
    // println!("{} took {} m seconds", s, dur.as_nanos());
    if dur.as_micros() > 0 {
        TIMES.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        TOTAL_TIME.fetch_add(dur.as_micros() as u64, std::sync::atomic::Ordering::SeqCst);
    }

}

fn main() {
    let mut conn = rusqlite::Connection::open("test.db").unwrap();
    let p_fn: ProfileFn = profile;
    conn.profile(Option::from(p_fn));

    conn.execute("CREATE TABLE IF NOT EXISTS test (id INTEGER PRIMARY KEY, name TEXT NOT NULL,phone TEXT,uid INTEGER,description TEXT )", []).unwrap();
//  INSERT TEST
    TIMES.store(0, std::sync::atomic::Ordering::SeqCst);
    TOTAL_TIME.store(0, std::sync::atomic::Ordering::SeqCst);
    let instant = std::time::Instant::now();
    let stmt = conn.prepare_cached("INSERT INTO test (name,phone,uid,description) VALUES (?,?,?,?)").unwrap();

    loop {
        if instant.elapsed().as_secs() >= 5 {
            break;
        }
        conn.cache_flush();
        let mut stmt = conn.prepare_cached("INSERT INTO test (name,phone,uid,description) VALUES (?1,?2,?3,?4)").unwrap();
        let params = &[&Name().fake::<String>(), &PhoneNumber().fake::<String>(), &(1000000000..4000000000).fake::<u32>().to_string(), &Paragraph(100..300).fake()];
        // println!("{:?}",params);
        stmt.execute(params).unwrap();
        // println!("u32 {:?}", );
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
        if instant.elapsed().as_secs() >= 5 {
            break;
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
    let stmt = conn.prepare_cached("SELECT * FROM test where id = ?1",).unwrap();

    loop {
        if instant.elapsed().as_secs() >= 5 {
            break;
        }
        conn.cache_flush();
        let mut stmt = conn.prepare_cached("SELECT * FROM test WHERE id =?1").unwrap();
        let params = &[ &(1..10000).fake::<i16>().to_string()];
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
        if instant.elapsed().as_secs() >= 5 {
            break;
        }
        conn.cache_flush();
        let mut stmt = conn.prepare_cached("DELETE FROM test WHERE id = ?1").unwrap();
        let params = &[ &(1..10000).fake::<i16>().to_string()];
        // println!("{:?}",params);
        stmt.execute(params).unwrap();
        // println!("u32 {:?}", );
    }
    println!("DELETE {} {} {}", TIMES.load(std::sync::atomic::Ordering::SeqCst), TOTAL_TIME.load(std::sync::atomic::Ordering::SeqCst), TOTAL_TIME.load(std::sync::atomic::Ordering::SeqCst) / TIMES.load(std::sync::atomic::Ordering::SeqCst));

}

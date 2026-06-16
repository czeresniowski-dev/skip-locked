//! Migration compute benchmark — Python (Celery-era) vs Rust on the SAME
//! `webhook.normalize` hot path the migration essay describes.
//!
//! The essay says ~70% of the old worker's wall time was in *parse and validate*;
//! this measures exactly that hot path — parse JSON, validate, normalize a
//! carrier tracking webhook — with NO database, so it isolates the compute that
//! the Python→Rust move actually changed. The companion `bench/celery_normalize.py`
//! runs the identical work over the identical payloads (this bin dumps them with
//! `-dump <dir>`; Python reads them).
//!
//! IMPORTANT: this is a *compute* comparison. The essay's 900ms→34ms p99 and
//! 280MB→40MB are production-tail figures under a retry storm with a loaded
//! Django/Celery process — not reproducible by a microbench. What IS reproducible
//! is the per-task compute ratio, the parse/validate share, and the RSS delta
//! between a bare Rust process and a bare Python one (measured by the wrapper
//! script via `/usr/bin/time -l`).
//!
//!   cargo run --release --example bench_migration -- -dump /tmp/mig
//!
//! Tune with MIG_N (default 200000).

use std::time::Instant;

use serde::Deserialize;

fn env_usize(key: &str, default: usize) -> usize {
    std::env::var(key).ok().and_then(|v| v.parse().ok()).unwrap_or(default)
}

// ── the webhook shape we parse+validate ────────────────────────────────────
#[derive(Deserialize)]
struct Raw {
    carrier_code: String,
    tracking_number: String,
    event: RawEvent,
    package: RawPackage,
}
#[derive(Deserialize)]
struct RawEvent {
    status: String,
    timestamp: String,
    location: RawLoc,
}
#[derive(Deserialize)]
struct RawLoc {
    city: String,
    country: String,
    postal: String,
}
#[derive(Deserialize)]
struct RawPackage {
    weight_grams: i64,
    pieces: i64,
}

const STATUSES: [&str; 6] = [
    "created", "in_transit", "out_for_delivery", "delivered", "exception", "returned",
];

// canonical status mapping (lowercase carrier term -> canonical UPPER)
fn canonical_status(s: &str) -> Option<&'static str> {
    match s {
        "created" => Some("CREATED"),
        "in_transit" => Some("IN_TRANSIT"),
        "out_for_delivery" => Some("OUT_FOR_DELIVERY"),
        "delivered" => Some("DELIVERED"),
        "exception" => Some("EXCEPTION"),
        "returned" => Some("RETURNED"),
        _ => None,
    }
}

#[allow(dead_code)] // built to model the work; fields aren't read in the bench
struct Normalized {
    carrier: String,
    tracking: String,
    status: &'static str,
    ts_millis: i64,
    country: String,
    billable_grams: i64,
}

// parse+validate+normalize one raw payload. Returns Ok(Normalized) or Err on a
// validation failure. The timing split is returned via the out-params.
fn process(raw: &str, pv_nanos: &mut u128, norm_nanos: &mut u128) -> Result<Normalized, ()> {
    let t0 = Instant::now();
    // parse
    let r: Raw = serde_json::from_str(raw).map_err(|_| ())?;
    // validate
    if r.carrier_code.is_empty() || r.tracking_number.len() < 6 {
        return Err(());
    }
    if !STATUSES.contains(&r.event.status.as_str()) {
        return Err(());
    }
    if r.package.weight_grams <= 0 || r.package.pieces <= 0 {
        return Err(());
    }
    let t1 = Instant::now();
    // normalize
    let status = canonical_status(&r.event.status).ok_or(())?;
    let ts = chrono::DateTime::parse_from_rfc3339(&r.event.timestamp)
        .map_err(|_| ())?
        .timestamp_millis();
    let norm = Normalized {
        carrier: r.carrier_code.to_uppercase(),
        tracking: r.tracking_number.clone(),
        status,
        ts_millis: ts,
        country: r.event.location.country.to_uppercase(),
        billable_grams: r.package.weight_grams.max(r.package.pieces * 500),
    };
    let _ = (&r.event.location.city, &r.event.location.postal); // touched fields
    let t2 = Instant::now();
    *pv_nanos += (t1 - t0).as_nanos();
    *norm_nanos += (t2 - t1).as_nanos();
    Ok(norm)
}

fn pct(sorted: &[f64], p: f64) -> f64 {
    if sorted.is_empty() {
        return 0.0;
    }
    let mut i = (p / 100.0 * sorted.len() as f64) as usize;
    if i >= sorted.len() {
        i = sorted.len() - 1;
    }
    sorted[i]
}

// deterministic synthetic payloads (raw JSON strings), seeded.
fn build_payloads(n: usize) -> Vec<String> {
    // tiny LCG so Rust and the dumped JSON are deterministic without rand deps
    let mut state: u64 = 0xC0FFEE;
    let mut next = || {
        state = state.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
        (state >> 33) as u32
    };
    let cities = ["Hamburg", "Lyon", "Turin", "Gdansk", "Porto", "Brno"];
    let countries = ["DE", "FR", "IT", "PL", "PT", "CZ"];
    let carriers = ["uxz", "dhl", "gls", "dpd", "inpost", "ups"];
    let mut out = Vec::with_capacity(n);
    for i in 0..n {
        let c = (next() as usize) % carriers.len();
        let s = STATUSES[(next() as usize) % STATUSES.len()];
        let loc = (next() as usize) % cities.len();
        let w = 500 + (next() % 40000) as i64;
        let p = 1 + (next() % 5) as i64;
        let hour = (next() % 24) as i64;
        let min = (next() % 60) as i64;
        out.push(format!(
            "{{\"carrier_code\":\"{carrier}\",\"tracking_number\":\"1Z999AA{seq:010}\",\
             \"event\":{{\"status\":\"{status}\",\"timestamp\":\"2026-06-16T{hh:02}:{mm:02}:00Z\",\
             \"location\":{{\"city\":\"{city}\",\"country\":\"{country}\",\"postal\":\"{postal:05}\"}}}},\
             \"package\":{{\"weight_grams\":{w},\"pieces\":{p}}},\
             \"meta\":{{\"source\":\"carrier-push\",\"attempt\":1}}}}",
            carrier = carriers[c],
            seq = i,
            status = s,
            hh = hour,
            mm = min,
            city = cities[loc],
            country = countries[loc],
            postal = (next() % 100000),
            w = w,
            p = p,
        ));
    }
    out
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let dump = args.iter().position(|a| a == "-dump").and_then(|i| args.get(i + 1)).cloned();
    let n = env_usize("MIG_N", 200_000);

    let payloads = build_payloads(n);

    if let Some(dir) = &dump {
        let path = std::path::Path::new(dir).join("payloads.jsonl");
        std::fs::create_dir_all(dir).ok();
        std::fs::write(&path, payloads.join("\n")).expect("write payloads");
        eprintln!("dumped {} payloads to {}", n, path.display());
    }

    let mut lat = Vec::with_capacity(n);
    let mut pv_nanos: u128 = 0;
    let mut norm_nanos: u128 = 0;
    let mut ok = 0usize;
    for raw in &payloads {
        let t0 = Instant::now();
        if process(raw, &mut pv_nanos, &mut norm_nanos).is_ok() {
            ok += 1;
        }
        lat.push((Instant::now() - t0).as_nanos() as f64 / 1000.0); // microseconds
    }
    lat.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let pv_pct = 100.0 * pv_nanos as f64 / (pv_nanos + norm_nanos) as f64;
    println!(
        "RUST   n={n} ok={ok} p50={:.2}us p99={:.2}us p999={:.2}us parse_validate_pct={:.0}",
        pct(&lat, 50.0),
        pct(&lat, 99.0),
        pct(&lat, 99.9),
        pv_pct,
    );
}

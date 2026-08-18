//! Benchmark the C++ vgi-rpc worker with a native client.
//!
//! `scripts/benchmark_transports.py` measures a round trip through the Python
//! reference client, which is what an application sees. It is the wrong
//! instrument for measuring the *server*: a call that does nothing costs ~22
//! microseconds there, nearly all of it client-side, so any server change
//! smaller than that is invisible. This drives the same worker from Rust so
//! the client is no longer the thing being measured.
//!
//! Reports the **best** sample. Interference on a shared machine is one-sided
//! — it can only make a sample slower — so the minimum is the closest
//! available estimate of the real cost, and it is what makes two runs on a
//! busy laptop comparable at all.

use std::sync::Arc;
use std::time::Instant;

use arrow_array::{Float64Array, Int64Array, LargeBinaryArray, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use vgi_rpc_client::{HttpClient, RpcClient};

/// One measurable call: a name, the method to invoke, and its params batch.
struct Workload {
    label: &'static str,
    method: &'static str,
    params: RecordBatch,
}

fn workloads() -> Vec<Workload> {
    let empty = RecordBatch::new_empty(Arc::new(Schema::empty()));

    let int_schema = Arc::new(Schema::new(vec![Field::new("value", DataType::Int64, true)]));
    let echo_int = RecordBatch::try_new(int_schema, vec![Arc::new(Int64Array::from(vec![7]))])
        .expect("echo_int params");

    let add_schema = Arc::new(Schema::new(vec![
        Field::new("a", DataType::Float64, true),
        Field::new("b", DataType::Float64, true),
    ]));
    let add_floats = RecordBatch::try_new(
        add_schema,
        vec![
            Arc::new(Float64Array::from(vec![1.0])),
            Arc::new(Float64Array::from(vec![2.0])),
        ],
    )
    .expect("add_floats params");

    let str_schema = Arc::new(Schema::new(vec![Field::new("value", DataType::Utf8, true)]));
    let echo_string = RecordBatch::try_new(
        str_schema,
        vec![Arc::new(StringArray::from(vec!["hello world"]))],
    )
    .expect("echo_string params");

    vec![
        // No parameters, no result: the per-call framing floor.
        Workload { label: "void_noop()", method: "void_noop", params: empty },
        Workload { label: "echo_int(7)", method: "echo_int", params: echo_int },
        Workload { label: "add_floats(a,b)", method: "add_floats", params: add_floats },
        Workload { label: "echo_string(11B)", method: "echo_string", params: echo_string },
    ]
}

/// Payload sizes bracketing the 128 KiB shared-memory routing threshold, so
/// the table shows both where the channel does nothing and where it earns its
/// keep.
fn payload_sizes() -> [usize; 4] {
    [1 << 10, 64 << 10, 1 << 20, 16 << 20]
}

/// An `echo_large_binary` params batch carrying `size` bytes.
fn payload_batch(size: usize) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::LargeBinary,
        true,
    )]));
    let blob = vec![0xa5u8; size];
    RecordBatch::try_new(
        schema,
        vec![Arc::new(LargeBinaryArray::from(vec![Some(blob.as_slice())]))],
    )
    .expect("echo_large_binary params")
}

/// Time `f` and return the best of `iters` samples, in microseconds.
fn best_us(warmup: usize, iters: usize, mut f: impl FnMut()) -> f64 {
    for _ in 0..warmup {
        f();
    }
    let mut lo = f64::MAX;
    for _ in 0..iters {
        let t0 = Instant::now();
        f();
        lo = lo.min(t0.elapsed().as_secs_f64());
    }
    lo * 1e6
}

/// A connection under test, uniform across transports.
enum Conn {
    Bytes(RpcClient),
    Http(HttpClient),
}

impl Conn {
    fn call(&mut self, method: &str, params: &RecordBatch) {
        let r = match self {
            Conn::Bytes(c) => c.call_unary(method, params, None).map(|_| ()),
            Conn::Http(c) => c.call_unary(method, params, None).map(|_| ()),
        };
        // A failure here means the numbers would be measuring an error path,
        // so stop rather than report them.
        if let Err(e) = r {
            eprintln!("call to {method} failed: {e}");
            std::process::exit(1);
        }
    }
}

fn main() {
    let mut worker = String::from("build-release/conformance/conformance_worker");
    let mut iters = 20_000usize;
    let mut warmup = 2_000usize;
    let mut rounds = 3usize;

    let argv: Vec<String> = std::env::args().skip(1).collect();
    let mut i = 0;
    while i < argv.len() {
        match argv[i].as_str() {
            "--worker" => { worker = argv[i + 1].clone(); i += 2; }
            "--iterations" => { iters = argv[i + 1].parse().expect("--iterations"); i += 2; }
            "--warmup" => { warmup = argv[i + 1].parse().expect("--warmup"); i += 2; }
            "--rounds" => { rounds = argv[i + 1].parse().expect("--rounds"); i += 2; }
            other => { eprintln!("unknown argument: {other}"); std::process::exit(2); }
        }
    }

    if !std::path::Path::new(&worker).is_file() {
        eprintln!("worker not found: {worker}");
        eprintln!("build one with: cmake --preset release && cmake --build build-release");
        std::process::exit(2);
    }

    let loads = workloads();
    let sizes = payload_sizes();
    let payloads: Vec<RecordBatch> = sizes.iter().map(|s| payload_batch(*s)).collect();
    let transports = ["pipe", "unix", "tcp", "http", "pipe+shm"];
    // transport -> workload -> best microseconds
    let mut best: Vec<Vec<f64>> = vec![vec![f64::MAX; loads.len()]; transports.len()];
    let mut best_payload: Vec<Vec<f64>> = vec![vec![f64::MAX; sizes.len()]; transports.len()];

    // Interleaved: measuring each transport once, in sequence, compares
    // whatever the machine was doing at the time as much as the transports.
    for round in 0..rounds {
        eprintln!("round {}/{rounds}", round + 1);
        for (ti, transport) in transports.iter().enumerate() {
            let mut conn = match connect(transport, &worker) {
                Some(c) => c,
                None => continue,
            };
            for (wi, w) in loads.iter().enumerate() {
                let us = best_us(warmup, iters, || conn.call(w.method, &w.params));
                best[ti][wi] = best[ti][wi].min(us);
            }
            // Far fewer iterations: 16 MiB twice over the wire dominates, and
            // a thousand of those measures patience, not throughput.
            for (pi, params) in payloads.iter().enumerate() {
                let us = best_us(3, 15, || conn.call("echo_large_binary", params));
                best_payload[ti][pi] = best_payload[ti][pi].min(us);
            }
        }
    }

    println!("\n## Unary round trip, native Rust client");
    println!("_best of {rounds} rounds x {iters} calls, us — lower is better_\n");
    print!("| {:<18} |", "");
    for t in &transports {
        print!(" {t:>9} |");
    }
    println!();
    print!("| {:-<18} |", "");
    for _ in &transports {
        print!(" {:-<9} |", "");
    }
    println!();
    for (wi, w) in loads.iter().enumerate() {
        print!("| {:<18} |", w.label);
        for ti in 0..transports.len() {
            if best[ti][wi] == f64::MAX {
                print!(" {:>9} |", "—");
            } else {
                print!(" {:>9.2} |", best[ti][wi]);
            }
        }
        println!();
    }

    println!("\n## Echo throughput");
    println!("_MB/s round trip at best time — higher is better_\n");
    print!("| {:<18} |", "");
    for t in &transports {
        print!(" {t:>9} |");
    }
    println!();
    print!("| {:-<18} |", "");
    for _ in &transports {
        print!(" {:-<9} |", "");
    }
    println!();
    for (pi, size) in sizes.iter().enumerate() {
        let label = if *size < (1 << 20) {
            format!("{} KiB", size >> 10)
        } else {
            format!("{} MiB", size >> 20)
        };
        print!("| {label:<18} |");
        for ti in 0..transports.len() {
            if best_payload[ti][pi] == f64::MAX {
                print!(" {:>9} |", "—");
            } else {
                // Both directions cross the wire, hence 2x.
                let mb = (2.0 * *size as f64) / (1024.0 * 1024.0);
                print!(" {:>9.0} |", mb / (best_payload[ti][pi] / 1e6));
            }
        }
        println!();
    }

    println!("\n## Unary rate\n_calls/sec at best round trip — higher is better_\n");
    print!("| {:<18} |", "");
    for t in &transports {
        print!(" {t:>9} |");
    }
    println!();
    print!("| {:-<18} |", "");
    for _ in &transports {
        print!(" {:-<9} |", "");
    }
    println!();
    for (wi, w) in loads.iter().enumerate() {
        print!("| {:<18} |", w.label);
        for ti in 0..transports.len() {
            if best[ti][wi] == f64::MAX {
                print!(" {:>9} |", "—");
            } else {
                print!(" {:>9.0} |", 1e6 / best[ti][wi]);
            }
        }
        println!();
    }
}

/// Open one connection of the named transport, or `None` when this platform
/// or build cannot offer it — a missing transport skips its column rather
/// than failing the run.
fn connect(transport: &str, worker: &str) -> Option<Conn> {
    match transport {
        "pipe" => RpcClient::connect(&[worker]).ok().map(Conn::Bytes),
        "pipe+shm" => RpcClient::shm_connect(&[worker], 256 << 20).ok().map(Conn::Bytes),
        "unix" => {
            let path = spawn_listener(worker, "--unix", &unique_sock(), "UNIX:")?;
            RpcClient::unix_connect(&path).ok().map(Conn::Bytes)
        }
        "tcp" => {
            let addr = spawn_listener(worker, "--tcp", "127.0.0.1:0", "TCP:")?;
            let (host, port) = addr.rsplit_once(':')?;
            RpcClient::tcp_connect(host, port.parse().ok()?).ok().map(Conn::Bytes)
        }
        "http" => {
            let port = spawn_listener(worker, "--port", "0", "PORT:")?;
            HttpClient::connect(format!("http://127.0.0.1:{port}"))
                .build()
                .ok()
                .map(Conn::Http)
        }
        _ => None,
    }
}

/// A short socket path: sockaddr_un.sun_path is 104 bytes on macOS, which the
/// usual temp directory plus a generated name overruns.
fn unique_sock() -> String {
    let pid = std::process::id();
    format!("/tmp/vgib{pid}.sock")
}

/// Spawn the worker as a listener and return the payload of its discovery
/// line. The child is deliberately leaked: it must outlive this function, and
/// the process is about to exit anyway.
fn spawn_listener(worker: &str, flag: &str, value: &str, prefix: &str) -> Option<String> {
    use std::io::{BufRead, BufReader};
    use std::process::{Command, Stdio};

    let mut cmd = Command::new(worker);
    if flag == "--port" {
        cmd.arg("--http").arg("--port").arg(value);
    } else {
        cmd.arg(flag).arg(value);
    }
    let mut child = cmd.stdout(Stdio::piped()).stderr(Stdio::null()).spawn().ok()?;
    let stdout = child.stdout.take()?;
    let mut line = String::new();
    BufReader::new(stdout).read_line(&mut line).ok()?;
    std::mem::forget(child);
    line.trim().strip_prefix(prefix).map(str::to_string)
}

//! A coverage-guided differential fuzzer for resonate-pg, on LibAFL.
//!
//! The point of using LibAFL here is the separation the framework makes and a
//! hand-rolled loop does not:
//!
//!   FEEDBACK  decides what earns a corpus slot — here, a custom bitmap whose
//!             entries are BEHAVIOURAL signatures rather than code edges. No
//!             instrumentation is involved: the harness writes the map itself.
//!
//!   OBJECTIVE decides what counts as a finding. For a differential that is not
//!             a crash, it is disagreement — so the harness returns
//!             `ExitKind::Crash` when the two schema layouts answer differently,
//!             or when either raises, and `CrashFeedback` collects it.
//!
//! WHAT IS UNDER TEST
//! ------------------
//! Two databases carrying the same protocol under different schemas: the
//! two-table `resonate.sql` and the merged `resonate-single.sql`. Every request
//! goes to both at the same instant, and their answers must match.
//!
//! The merged store also carries `constraints.sql` — 38 entries of the
//! specification's conformance catalogue, as CHECK/UNIQUE/FOREIGN KEY. That
//! makes a second property oracle free: a catalogue violation raises inside
//! `resonate_rpc`, whose exception arm turns it into a 500. So "the constrained
//! store returned 500" IS "a catalogue property was violated", and it is an
//! objective on a SINGLE store — the kind of finding cross-checking two
//! implementations can never produce, because two stores that share a bug agree.
//!
//! THE INPUT IS A DECISION TAPE, NOT A REQUEST
//! -------------------------------------------
//! Mutating request bytes is hopeless here: `task.acquire` needs a task that
//! exists, is pending, and is at exactly the right version, and blind mutation
//! essentially never produces one — the Python generator in `test/` is the
//! control experiment, sitting at 0.7% success on that call.
//!
//! So the input is a tape of decision bytes, and a planner consumes it while
//! reading live state out of the database: the tape chooses WHICH pending task
//! to acquire, the database supplies its id and version. Every request is
//! well-formed by construction, mutating the tape mutates the plan, and the
//! fuzzer climbs the signature space instead of the syntax space. Zest calls
//! this generator-based fuzzing; it is the reason the corpus is worth having.
//!
//! Run:
//!   createdb res_fuzz_two && psql -d res_fuzz_two -f resonate.sql
//!   createdb res_fuzz_one && psql -d res_fuzz_one -f resonate-single.sql -f constraints-all.sql
//!   cargo run --release

use std::{path::PathBuf, ptr::write};

use libafl::{
    corpus::{InMemoryCorpus, OnDiskCorpus},
    events::SimpleEventManager,
    executors::{ExitKind, InProcessExecutor},
    feedbacks::{CrashFeedback, MaxMapFeedback},
    fuzzer::{Fuzzer, StdFuzzer},
    generators::RandBytesGenerator,
    inputs::{BytesInput, HasTargetBytes},
    monitors::SimpleMonitor,
    mutators::{havoc_mutations::havoc_mutations, scheduled::HavocScheduledMutator},
    observers::StdMapObserver,
    schedulers::QueueScheduler,
    stages::mutational::StdMutationalStage,
    state::StdState,
};
use libafl_bolts::{current_nanos, nonzero, rands::StdRand, tuples::tuple_list};
use postgres::{Client, NoTls};
use serde_json::{json, Value};

// ─── the bitmap ──────────────────────────────────────────────────────────────
// 64KB, the classic AFL size, but every entry is a behavioural signature:
// (operation, response status, bucketed shape of the store). Nothing here comes
// from instrumentation — `signal` is called by the harness.

const MAP_SIZE: usize = 65536;
static mut SIGNALS: [u8; MAP_SIZE] = [0; MAP_SIZE];

fn signal(idx: usize) {
    unsafe {
        let p = (&raw mut SIGNALS) as *mut u8;
        let cur = *p.add(idx % MAP_SIZE);
        write(p.add(idx % MAP_SIZE), cur.saturating_add(1));
    }
}

fn clear_map() {
    unsafe {
        let p = (&raw mut SIGNALS) as *mut u8;
        for i in 0..MAP_SIZE {
            write(p.add(i), 0);
        }
    }
}

/// AFL's hit-count bucketing, applied to a cardinality.
///
/// Eight buckets rather than "is it non-empty": a store with three suspended
/// tasks is a different shape from one with thirty, and a feedback that cannot
/// tell them apart has nothing to climb.
fn bucket(n: i64) -> u64 {
    match n {
        0 => 0,
        1 => 1,
        2 => 2,
        3 => 3,
        4..=7 => 4,
        8..=15 => 5,
        16..=31 => 6,
        _ => 7,
    }
}

fn mix(mut h: u64, v: u64) -> u64 {
    h ^= v.wrapping_add(0x9e37_79b9_7f4a_7c15).wrapping_add(h << 6).wrapping_add(h >> 2);
    h
}

// ─── the decision tape ───────────────────────────────────────────────────────

struct Tape<'a> {
    b: &'a [u8],
    i: usize,
}

impl<'a> Tape<'a> {
    fn new(b: &'a [u8]) -> Self {
        Tape { b, i: 0 }
    }
    fn done(&self) -> bool {
        self.i >= self.b.len()
    }
    fn byte(&mut self) -> u8 {
        let v = self.b.get(self.i).copied().unwrap_or(0);
        self.i += 1;
        v
    }
    fn upto(&mut self, n: usize) -> usize {
        if n == 0 {
            0
        } else {
            self.byte() as usize % n
        }
    }
    /// Two bytes, for a value that wants more range than 0..255.
    fn word(&mut self) -> u32 {
        (self.byte() as u32) << 8 | self.byte() as u32
    }
}

// ─── operations ──────────────────────────────────────────────────────────────

const OPS: &[&str] = &[
    "promise.create",
    "promise.get",
    "promise.settle",
    "promise.register_callback",
    "promise.register_listener",
    "task.create",
    "task.get",
    "task.acquire",
    "task.release",
    "task.fulfill",
    "task.suspend",
    "task.fence",
    "task.heartbeat",
    "task.halt",
    "task.continue",
    "schedule.create",
    "schedule.get",
    "schedule.delete",
    "tick",
];

/// What the planner needs to know to build a request that will actually land.
struct Live {
    pending_tasks: Vec<(String, i32)>,
    acquired_tasks: Vec<(String, i32, String)>,
    suspended: Vec<String>,
    halted: Vec<String>,
    pending_promises: Vec<String>,
    pending_targeted: Vec<String>,
    schedules: Vec<String>,
    all_promises: Vec<String>,
}

fn read_live(c: &mut Client) -> Result<Live, postgres::Error> {
    let mut l = Live {
        pending_tasks: vec![],
        acquired_tasks: vec![],
        suspended: vec![],
        halted: vec![],
        pending_promises: vec![],
        pending_targeted: vec![],
        schedules: vec![],
        all_promises: vec![],
    };
    for r in c.query(
        "SELECT id, task_state, task_version, pid, state, target IS NOT NULL
           FROM resonate.promises ORDER BY id LIMIT 200",
        &[],
    )? {
        let id: String = r.get(0);
        let ts: Option<String> = r.get(1);
        let tv: Option<i32> = r.get(2);
        let pid: Option<String> = r.get(3);
        let ps: String = r.get(4);
        let targeted: bool = r.get(5);
        l.all_promises.push(id.clone());
        if ps == "pending" {
            l.pending_promises.push(id.clone());
            if targeted {
                l.pending_targeted.push(id.clone());
            }
        }
        match ts.as_deref() {
            Some("pending") => l.pending_tasks.push((id, tv.unwrap_or(0))),
            Some("acquired") => {
                l.acquired_tasks.push((id, tv.unwrap_or(0), pid.unwrap_or_default()))
            }
            Some("suspended") => l.suspended.push(id),
            Some("halted") => l.halted.push(id),
            _ => {}
        }
    }
    for r in c.query("SELECT id FROM resonate.schedules ORDER BY id LIMIT 50", &[])? {
        l.schedules.push(r.get(0));
    }
    Ok(l)
}

fn shape(c: &mut Client) -> Result<u64, postgres::Error> {
    let r = c.query_one(
        "SELECT count(*) FILTER (WHERE state = 'pending'),
                count(*) FILTER (WHERE state <> 'pending'),
                count(*) FILTER (WHERE task_state = 'pending'),
                count(*) FILTER (WHERE task_state = 'acquired'),
                count(*) FILTER (WHERE task_state = 'suspended'),
                count(*) FILTER (WHERE task_state = 'halted'),
                COALESCE(sum(cardinality(awaiters)), 0),
                COALESCE(sum(cardinality(listeners)), 0),
                COALESCE(sum(cardinality(resumes)), 0),
                (SELECT count(*) FROM resonate.outbox),
                (SELECT count(*) FROM resonate.schedules)
           FROM resonate.promises",
        &[],
    )?;
    let mut h = 0u64;
    for i in 0..11 {
        let v: i64 = r.get(i);
        h = mix(h, bucket(v));
    }
    Ok(h)
}

/// Which operations can actually land against this state.
///
/// Choosing the operation first and hunting for an operand afterwards is what
/// keeps a blind generator at 0.7% on `task.acquire`: with no pending task it
/// invents an id and collects a 404. Filtering first — the shape `pick_op` in
/// the Rust differential uses — means the tape only ever chooses among things
/// the store can currently do.
fn eligible(l: &Live) -> Vec<&'static str> {
    let mut v: Vec<&'static str> = vec![
        // always available: they create their own preconditions
        "promise.create", "task.create", "schedule.create", "tick",
    ];
    if !l.all_promises.is_empty() {
        v.push("promise.get");
        v.push("task.get");
    }
    if !l.pending_promises.is_empty() {
        v.push("promise.settle");
        v.push("promise.register_listener");
    }
    if !l.pending_targeted.is_empty() {
        v.push("promise.register_callback");
    }
    if !l.pending_tasks.is_empty() {
        v.push("task.acquire");
    }
    if !l.acquired_tasks.is_empty() {
        v.push("task.release");
        v.push("task.fulfill");
        v.push("task.fence");
        v.push("task.heartbeat");
        if !l.pending_targeted.is_empty() {
            v.push("task.suspend");
        }
    }
    if !l.halted.is_empty() {
        v.push("task.continue");
    }
    if !l.pending_tasks.is_empty() || !l.acquired_tasks.is_empty() || !l.suspended.is_empty() {
        v.push("task.halt");
    }
    if !l.schedules.is_empty() {
        v.push("schedule.get");
        v.push("schedule.delete");
    }
    v
}

/// Build one request from the tape, planned against live state.
fn plan(t: &mut Tape, l: &Live, now: i64) -> Option<(String, Value)> {
    let target = if t.byte() % 4 == 0 { "g2" } else { "g1" };
    let elig = eligible(l);
    // One byte in eight ignores eligibility, so the reject paths stay covered —
    // a fuzzer that only ever sends valid requests never tests a guard.
    let op = if t.byte() % 8 == 0 {
        OPS[t.upto(OPS.len())]
    } else {
        elig[t.upto(elig.len())]
    };
    let new_id = || -> String {
        // `foo.N` is a root and its own origin, `foo.N:M` a child of it.
        format!("foo.{}", 0)
    };
    let d = match op {
        "promise.create" => {
            let root = format!("foo.{}", t.upto(6));
            let id = if t.byte() % 3 == 0 {
                format!("{}:{}", root, t.upto(4))
            } else {
                root.clone()
            };
            // Each tag is rolled INDEPENDENTLY. An earlier version used a
            // match, which made timer and target mutually exclusive — and a
            // deliberately broken store that accepted `Tags.timerTargeted` then
            // survived a full run, because the generator could not express the
            // input that refutes it. A fuzzer that cannot produce the witness
            // passes vacuously, which is the failure mode the specification
            // repo's own fuzzer guards against by mutating known-good traces.
            let mut tags = json!({ "resonate:origin": id.split(':').next().unwrap() });
            if t.byte() % 3 == 0 {
                tags["resonate:timer"] = json!("true");
            }
            if t.byte() % 2 == 0 {
                tags["resonate:target"] = json!(target);
            }
            if t.byte() % 5 == 0 {
                tags["resonate:external"] = json!("true");
            }
            if t.byte() % 7 == 0 {
                tags["resonate:delay"] = json!((now + 5000).to_string());
            }
            json!({ "id": id, "timeoutAt": now + (t.word() as i64) * 10,
                    "param": { "headers": {}, "data": null }, "tags": tags })
        }
        "promise.get" => json!({ "id": pick(t, &l.all_promises).unwrap_or_else(new_id) }),
        "promise.settle" => json!({
            "id": pick(t, &l.pending_promises).unwrap_or_else(new_id),
            "state": one_of(t, SETTLE),
            "value": { "headers": {}, "data": "v" } }),
        "promise.register_callback" => {
            let awaited = pick(t, &l.pending_targeted).unwrap_or_else(new_id);
            let awaiter = pick(t, &l.pending_targeted).unwrap_or_else(new_id);
            json!({ "awaited": awaited, "awaiter": awaiter })
        }
        "promise.register_listener" => json!({
            "awaited": pick(t, &l.pending_promises).unwrap_or_else(new_id),
            "address": one_of(t, ADDRS) }),
        "task.create" => {
            let root = format!("foo.{}", t.upto(6));
            let ttl = one_i64(t, TTLS);
            json!({ "pid": format!("w{}", t.upto(3)), "ttl": ttl,
                    "action": { "data": {
                        "id": root, "timeoutAt": now + (t.word() as i64) * 10,
                        "param": { "headers": {}, "data": null },
                        "tags": if t.byte() % 4 == 0 {
                            json!({ "resonate:target": target, "resonate:origin": root,
                                    "resonate:timer": "true" })
                        } else {
                            json!({ "resonate:target": target, "resonate:origin": root })
                        } } } })
        }
        "task.get" => json!({ "id": pick(t, &l.all_promises).unwrap_or_else(new_id) }),
        "task.acquire" => {
            // The whole reason for planning: id AND its current version.
            let (id, v) = pick(t, &l.pending_tasks).unwrap_or_else(|| (new_id(), 0));
            let ttl = one_i64(t, TTLS);
            json!({ "id": id, "version": v, "pid": format!("w{}", t.upto(3)),
                    "ttl": ttl })
        }
        "task.release" => {
            let (id, v, _) = pick(t, &l.acquired_tasks).unwrap_or_else(|| (new_id(), 0, String::new()));
            json!({ "id": id, "version": v })
        }
        "task.fulfill" => {
            let (id, v, _) = pick(t, &l.acquired_tasks).unwrap_or_else(|| (new_id(), 0, String::new()));
            let st = one_of(t, SETTLE);
            json!({ "id": id, "version": v, "action": { "data": {
                "id": id, "state": st,
                "value": { "headers": {}, "data": "r" } } } })
        }
        "task.suspend" => {
            let (id, v, _) = pick(t, &l.acquired_tasks).unwrap_or_else(|| (new_id(), 0, String::new()));
            let n = 1 + t.upto(3);
            let mut actions = vec![];
            let mut seen: Vec<String> = vec![];
            for _ in 0..n {
                if let Some(a) = pick(t, &l.pending_targeted) {
                    if a != id && !seen.contains(&a) {
                        seen.push(a.clone());
                        actions.push(json!({ "data": { "awaited": a } }));
                    }
                }
            }
            json!({ "id": id, "version": v, "actions": actions })
        }
        "task.fence" => {
            let (id, v, _) = pick(t, &l.acquired_tasks).unwrap_or_else(|| (new_id(), 0, String::new()));
            let other = pick(t, &l.pending_promises).unwrap_or_else(new_id);
            if t.byte() % 2 == 0 {
                json!({ "id": id, "version": v, "action": { "kind": "promise.settle",
                    "data": { "id": other, "state": "resolved",
                              "value": { "headers": {}, "data": "f" } } } })
            } else {
                let root = format!("foo.{}", t.upto(6));
                json!({ "id": id, "version": v, "action": { "kind": "promise.create",
                    "data": { "id": root, "timeoutAt": now + 60000,
                              "param": { "headers": {}, "data": null },
                              "tags": { "resonate:origin": root } } } })
            }
        }
        "task.heartbeat" => {
            let (id, v, pid) = pick(t, &l.acquired_tasks)
                .unwrap_or_else(|| (new_id(), 0, "w0".into()));
            json!({ "pid": pid, "tasks": [{ "id": id, "version": v }] })
        }
        "task.halt" => {
            let mut c: Vec<String> = l.suspended.clone();
            c.extend(l.pending_tasks.iter().map(|(i, _)| i.clone()));
            c.extend(l.acquired_tasks.iter().map(|(i, _, _)| i.clone()));
            json!({ "id": pick(t, &c).unwrap_or_else(new_id) })
        }
        "task.continue" => json!({ "id": pick(t, &l.halted).unwrap_or_else(new_id) }),
        "schedule.create" => json!({
            "id": format!("s{}", t.upto(3)),
            "cron": one_of(t, CRONS),
            "promiseId": "sp.{{.id}}.{{.timestamp}}", "promiseTimeout": 60000,
            "promiseParam": { "headers": {}, "data": null }, "promiseTags": {} }),
        "schedule.get" => json!({ "id": pick(t, &l.schedules)
            .unwrap_or_else(|| format!("s{}", t.upto(3))) }),
        "schedule.delete" => json!({ "id": pick(t, &l.schedules)
            .unwrap_or_else(|| format!("s{}", t.upto(3))) }),
        "tick" => json!({}),
        _ => unreachable!(),
    };
    Some((op.to_string(), d))
}

const SETTLE: &[&str] = &["resolved", "rejected", "rejected_canceled"];
const ADDRS: &[&str] = &["http://a/1", "https://b/2", "poll://c@d"];
const TTLS: &[i64] = &[1, 5000, 60000];
const CRONS: &[&str] = &["* * * * *", "0 * * * *", "*/5 * * * *"];

fn one_of<'a>(t: &mut Tape, v: &[&'a str]) -> &'a str {
    v[t.upto(v.len())]
}

fn one_i64(t: &mut Tape, v: &[i64]) -> i64 {
    v[t.upto(v.len())]
}

fn pick<T: Clone>(t: &mut Tape, v: &[T]) -> Option<T> {
    if v.is_empty() {
        None
    } else {
        Some(v[t.upto(v.len())].clone())
    }
}

// ─── productivity counters ───────────────────────────────────────────────────
// The planner exists to make requests LAND. Counting how often each operation
// reaches a 2xx is the check on whether it does — the blind generator in
// `test/differential.py` is the control, and it sits at 0.7% on task.acquire.

static mut OP_TOTAL: [u64; 32] = [0; 32];
static mut OP_OK: [u64; 32] = [0; 32];
static mut PROGRAMS: u64 = 0;

fn count(op_idx: usize, ok: bool) {
    unsafe {
        let t = (&raw mut OP_TOTAL) as *mut u64;
        *t.add(op_idx) += 1;
        if ok {
            let k = (&raw mut OP_OK) as *mut u64;
            *k.add(op_idx) += 1;
        }
    }
}

fn dump_counts() {
    unsafe {
        let p = (&raw mut PROGRAMS) as *mut u64;
        *p += 1;
        if *p % 250 != 0 {
            return;
        }
        let t = (&raw const OP_TOTAL) as *const u64;
        let k = (&raw const OP_OK) as *const u64;
        eprintln!("\n[planner] after {} programs — share of each op reaching 2xx/3xx:", *p);
        for (i, op) in OPS.iter().enumerate() {
            let tot = *t.add(i);
            if tot == 0 {
                continue;
            }
            eprintln!("  {:<28} {:5.1}%   n={}", op, *k.add(i) as f64 / tot as f64 * 100.0, tot);
        }
    }
}

// ─── the harness ─────────────────────────────────────────────────────────────

fn rpc(c: &mut Client, kind: &str, data: &Value, now: i64) -> Result<Value, postgres::Error> {
    let env = json!({ "kind": kind,
                      "head": { "corrId": "f", "version": "1",
                                "resonate:debug_time": now.to_string() },
                      "data": data });
    let row = c.query_one("SELECT resonate.resonate_rpc($1::jsonb)", &[&env])?;
    Ok(row.get(0))
}

/// Status is the contract; the error phrase is not — the two stores word their
/// 4xx differently and always have.
fn canon(v: &Value) -> Value {
    let status = v.pointer("/head/status").cloned().unwrap_or(Value::Null);
    let d = v.get("data");
    let s = status.as_i64().unwrap_or(0);
    if s != 200 && s != 300 {
        return json!({ "status": status });
    }
    let mut out = json!({ "status": status });
    if let Some(d) = d.and_then(|d| d.as_object()) {
        for k in ["promise", "task", "schedule", "action"] {
            if let Some(v) = d.get(k) {
                out[k] = v.clone();
            }
        }
    }
    out
}

struct Finding {
    step: usize,
    kind: String,
    data: Value,
    two: Value,
    one: Value,
    why: &'static str,
}

fn run_program(two: &mut Client, one: &mut Client, tape: &[u8]) -> Option<Finding> {
    const RESET_TWO: &str = "TRUNCATE resonate.outbox, resonate.listeners, resonate.callbacks, \
         resonate.task_resumes, resonate.tasks, resonate.schedules, resonate.promises CASCADE";
    const RESET_ONE: &str =
        "TRUNCATE resonate.outbox, resonate.schedules, resonate.promises CASCADE";
    if two.execute(RESET_TWO, &[]).is_err() || one.execute(RESET_ONE, &[]).is_err() {
        return None;
    }

    dump_counts();
    let mut t = Tape::new(tape);
    let mut now: i64 = 1_000_000_000;
    let mut step = 0usize;

    while !t.done() && step < 128 {
        now += [0i64, 1, 10, 250, 3_000, 7_000][t.upto(6)];
        let live = match read_live(one) {
            Ok(l) => l,
            Err(_) => return None,
        };
        let (kind, data) = match plan(&mut t, &live, now) {
            Some(x) => x,
            None => break,
        };
        step += 1;

        let (r2, r1) = if kind == "tick" {
            let a = two.execute("SELECT resonate.process_timeouts($1)", &[&now]);
            let b = one.execute("SELECT resonate.process_timeouts($1)", &[&now]);
            match (a, b) {
                (Ok(_), Ok(_)) => (json!({"head":{"status":200}}), json!({"head":{"status":200}})),
                // A raise from the sweep is a finding: the constrained store
                // reached a state its catalogue forbids.
                _ => {
                    return Some(Finding { step, kind, data, two: json!("sweep"),
                                          one: json!("sweep raised"), why: "tick raised" })
                }
            }
        } else {
            match (rpc(two, &kind, &data, now), rpc(one, &kind, &data, now)) {
                (Ok(a), Ok(b)) => (a, b),
                _ => return None,
            }
        };

        let s2 = r2.pointer("/head/status").and_then(|v| v.as_i64()).unwrap_or(0);
        let s1 = r1.pointer("/head/status").and_then(|v| v.as_i64()).unwrap_or(0);
        let op_i = OPS.iter().position(|o| *o == kind).unwrap_or(0);
        count(op_i, s1 == 200 || s1 == 300);

        // ── FEEDBACK: one signature per step into the custom bitmap ──────────
        let sh = shape(one).unwrap_or(0);
        let op_idx = OPS.iter().position(|o| *o == kind).unwrap_or(0) as u64;
        let sig = mix(mix(mix(0, op_idx), s1 as u64), sh);
        signal((sig % MAP_SIZE as u64) as usize);

        // ── OBJECTIVE ───────────────────────────────────────────────────────
        // A 500 from the constrained store is a catalogue violation: the CHECK
        // raised and resonate_rpc's exception arm turned it into a 500. This is
        // the finding two agreeing stores could never give you.
        if s1 == 500 || s2 == 500 {
            return Some(Finding { step, kind, data, two: r2, one: r1,
                                  why: "internal error / constraint violation" });
        }
        if canon(&r2) != canon(&r1) {
            return Some(Finding { step, kind, data, two: r2, one: r1,
                                  why: "layouts disagree" });
        }
    }
    None
}

fn main() {
    let two_dsn = std::env::var("FUZZ_TWO")
        .unwrap_or_else(|_| "host=/tmp port=5433 user=postgres dbname=res_fuzz_two".into());
    let one_dsn = std::env::var("FUZZ_ONE")
        .unwrap_or_else(|_| "host=/tmp port=5433 user=postgres dbname=res_fuzz_one".into());

    let mut two = Client::connect(&two_dsn, NoTls).expect("connect two-table store");
    let mut one = Client::connect(&one_dsn, NoTls).expect("connect single-table store");

    let findings_dir = PathBuf::from("./findings");
    std::fs::create_dir_all(&findings_dir).ok();
    let mut found = 0usize;

    let mut harness = |input: &BytesInput| {
        let bytes = input.target_bytes();
        clear_map();
        match run_program(&mut two, &mut one, &bytes) {
            None => ExitKind::Ok,
            Some(f) => {
                found += 1;
                let report = json!({
                    "why": f.why, "step": f.step, "op": f.kind, "request": f.data,
                    "two_table": f.two, "single_table": f.one,
                    "tape": bytes.iter().map(|b| *b as u32).collect::<Vec<_>>(),
                });
                let path = findings_dir.join(format!("finding-{found:04}.json"));
                std::fs::write(&path, serde_json::to_string_pretty(&report).unwrap()).ok();
                eprintln!("\n[FINDING {found}] {} at step {} on {}\n  {}\n",
                          f.why, f.step, f.kind, path.display());
                ExitKind::Crash
            }
        }
    };

    let observer = unsafe { StdMapObserver::new("signatures", &mut *(&raw mut SIGNALS)) };

    // FEEDBACK — corpus membership. A new maximum in any signature bucket.
    let mut feedback = MaxMapFeedback::new(&observer);
    // OBJECTIVE — findings. Divergence, reported as a crash by the harness.
    let mut objective = CrashFeedback::new();

    let mut state = StdState::new(
        StdRand::with_seed(current_nanos()),
        InMemoryCorpus::new(),
        OnDiskCorpus::new(findings_dir.join("tapes")).unwrap(),
        &mut feedback,
        &mut objective,
    )
    .unwrap();

    let mon = SimpleMonitor::new(|s| println!("{s}"));
    let mut mgr = SimpleEventManager::new(mon);
    let scheduler = QueueScheduler::new();
    let mut fuzzer = StdFuzzer::new(scheduler, feedback, objective);

    let mut executor = InProcessExecutor::new(
        &mut harness,
        tuple_list!(observer),
        &mut fuzzer,
        &mut state,
        &mut mgr,
    )
    .expect("executor");

    let mut generator = RandBytesGenerator::new(nonzero!(256));
    state
        .generate_initial_inputs(&mut fuzzer, &mut executor, &mut generator, &mut mgr, 16)
        .expect("initial corpus");

    let mutator = HavocScheduledMutator::new(havoc_mutations());
    let mut stages = tuple_list!(StdMutationalStage::new(mutator));

    fuzzer
        .fuzz_loop(&mut stages, &mut executor, &mut state, &mut mgr)
        .expect("fuzz loop");
}

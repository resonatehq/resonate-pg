//! A coverage-guided differential fuzzer for resonate-pg, on LibAFL.
//!
//!   FEEDBACK  a custom bitmap whose entries are BEHAVIOURAL signatures rather
//!             than code edges. No instrumentation: the harness writes the map.
//!             What goes into a signature is selectable — see `Feedback` — so
//!             the choice can be measured rather than asserted.
//!
//!   OBJECTIVE divergence. The harness returns `ExitKind::Crash` when the two
//!             schema layouts answer differently or either raises, and
//!             `CrashFeedback` collects it. The merged store carries the
//!             catalogue as CHECK constraints, so a violation raises and
//!             `resonate_rpc`'s exception arm makes it a 500 — which turns a
//!             catalogue violation into an objective on a SINGLE store, the
//!             finding two agreeing implementations can never produce.
//!
//! The input is a decision TAPE, not a request: a planner consumes tape bytes
//! while reading live state, so the tape chooses WHICH pending task to acquire
//! and the database supplies its id and version. Mutation moves the plan, not
//! the syntax.
//!
//! Environment:
//!   FUZZ_TWO / FUZZ_ONE   connection strings for the two stores
//!   FUZZ_FEEDBACK         points | edges | preconds | shapes   (default: shapes)
//!   FUZZ_MAX_EXECS        stop after N executions with no finding (0 = forever)
//!   FUZZ_QUIET            suppress the per-finding report

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

// ─── feedback configuration ──────────────────────────────────────────────────

/// What a signature is made of. Each level adds one signal to the one before,
/// so the battery can attribute a change in bug-finding to a single addition.
#[derive(Clone, Copy, PartialEq)]
enum Feedback {
    /// (operation, status, aggregate shape of the store). Points, not edges.
    Points,
    /// Points, plus AFL's edge encoding: the signature of the PREVIOUS step is
    /// mixed in, so a new ORDER of the same steps is novel. This is most of
    /// why AFL's bitmap works and it was missing here.
    Edges,
    /// Edges, plus the preconditions that held on the operand this step names —
    /// a proxy for which guard fired, since the guards are tests on exactly
    /// these predicates. Distinguishes "409 because the version was wrong" from
    /// "409 because the task was not pending", which a status code cannot.
    Preconds,
    /// Preconds, but the store's shape is measured PER OBJECT (0/1/2/many on the
    /// largest awaiter set, the largest listener set) rather than as a sum over
    /// all promises. A sum cannot tell one promise with five awaiters from five
    /// promises with one each — fan-in from fan-out — which is the distinction
    /// the merged layout's behaviour actually turns on.
    Shapes,
}

impl Feedback {
    fn from_env() -> Self {
        match std::env::var("FUZZ_FEEDBACK").unwrap_or_default().as_str() {
            "points" => Feedback::Points,
            "edges" => Feedback::Edges,
            "preconds" => Feedback::Preconds,
            _ => Feedback::Shapes,
        }
    }
    fn name(self) -> &'static str {
        match self {
            Feedback::Points => "points",
            Feedback::Edges => "edges",
            Feedback::Preconds => "preconds",
            Feedback::Shapes => "shapes",
        }
    }
}

// ─── the bitmap ──────────────────────────────────────────────────────────────

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

/// 0 / 1 / 2 / many — the boundaries the implementation itself branches on.
/// `well_formed_promise_callbacks_unique` is literally
/// `cardinality(awaiters) < 2 OR _arr_uniq(awaiters)`.
fn small(n: i64) -> u64 {
    match n {
        0 => 0,
        1 => 1,
        2 => 2,
        _ => 3,
    }
}

fn mix(mut h: u64, v: u64) -> u64 {
    h ^= v
        .wrapping_add(0x9e37_79b9_7f4a_7c15)
        .wrapping_add(h << 6)
        .wrapping_add(h >> 2);
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

/// One promise row, as the planner and the shape signals need it.
#[derive(Clone)]
struct Row {
    id: String,
    state: String,
    task_state: Option<String>,
    version: i32,
    pid: Option<String>,
    targeted: bool,
    external: bool,
    timeout_at: i64,
    awaiters: i64,
    listeners: i64,
    resumes: i64,
}

struct Live {
    rows: Vec<Row>,
    outbox: i64,
    schedule_ids: Vec<String>,
}

impl Live {
    fn by_task(&self, st: &str) -> Vec<&Row> {
        self.rows.iter().filter(|r| r.task_state.as_deref() == Some(st)).collect()
    }
    fn pending_promises(&self) -> Vec<&Row> {
        self.rows.iter().filter(|r| r.state == "pending").collect()
    }
    fn pending_targeted(&self) -> Vec<&Row> {
        self.rows.iter().filter(|r| r.state == "pending" && r.targeted).collect()
    }
    fn find(&self, id: &str) -> Option<&Row> {
        self.rows.iter().find(|r| r.id == id)
    }
}

/// One round trip, not two: the outbox count rides along as an uncorrelated
/// scalar subquery, which the planner evaluates once.
fn read_live(c: &mut Client) -> Result<Live, postgres::Error> {
    let mut rows = Vec::new();
    let mut outbox = 0i64;
    for r in c.query(
        "SELECT id, state, task_state, COALESCE(task_version, 0), pid,
                target IS NOT NULL, external, timeout_at,
                cardinality(awaiters), cardinality(listeners), cardinality(resumes),
                (SELECT count(*) FROM resonate.outbox)
           FROM resonate.promises ORDER BY id LIMIT 300",
        &[],
    )? {
        rows.push(Row {
            id: r.get(0),
            state: r.get(1),
            task_state: r.get(2),
            version: r.get(3),
            pid: r.get(4),
            targeted: r.get(5),
            external: r.get(6),
            timeout_at: r.get(7),
            awaiters: r.get::<_, Option<i32>>(8).unwrap_or(0) as i64,
            listeners: r.get::<_, Option<i32>>(9).unwrap_or(0) as i64,
            resumes: r.get::<_, Option<i32>>(10).unwrap_or(0) as i64,
        });
        outbox = r.get(11);
    }
    let mut schedule_ids = Vec::new();
    for r in c.query("SELECT id FROM resonate.schedules ORDER BY id LIMIT 50", &[])? {
        schedule_ids.push(r.get(0));
    }
    Ok(Live { rows, outbox, schedule_ids })
}

/// Aggregate shape — sums, which cannot tell fan-in from fan-out.
fn shape_aggregate(l: &Live) -> u64 {
    let c = |f: &dyn Fn(&Row) -> bool| l.rows.iter().filter(|r| f(r)).count() as i64;
    let mut h = 0u64;
    h = mix(h, bucket(c(&|r| r.state == "pending")));
    h = mix(h, bucket(c(&|r| r.state != "pending")));
    for st in ["pending", "acquired", "suspended", "halted", "fulfilled"] {
        h = mix(h, bucket(c(&|r| r.task_state.as_deref() == Some(st))));
    }
    h = mix(h, bucket(l.rows.iter().map(|r| r.awaiters).sum()));
    h = mix(h, bucket(l.rows.iter().map(|r| r.listeners).sum()));
    h = mix(h, bucket(l.rows.iter().map(|r| r.resumes).sum()));
    h = mix(h, bucket(l.outbox));
    h = mix(h, bucket(l.schedule_ids.len() as i64));
    h
}

/// Per-object shape — the largest set, and how many objects carry one, so a
/// promise awaited by five is a different shape from five awaited by one.
fn shape_per_object(l: &Live) -> u64 {
    let mut h = shape_aggregate(l);
    h = mix(h, small(l.rows.iter().map(|r| r.awaiters).max().unwrap_or(0)));
    h = mix(h, small(l.rows.iter().map(|r| r.listeners).max().unwrap_or(0)));
    h = mix(h, small(l.rows.iter().map(|r| r.resumes).max().unwrap_or(0)));
    h = mix(h, small(l.rows.iter().filter(|r| r.awaiters > 0).count() as i64));
    h = mix(h, small(l.rows.iter().filter(|r| r.listeners > 0).count() as i64));
    h
}

/// The preconditions that held on this request's operand, before it ran.
///
/// A proxy for "which guard fired": the guards test exactly these predicates,
/// so the vector separates the three distinct ways `task.acquire` reaches 409.
/// Free — it reads the snapshot the planner already fetched.
fn preconds(l: &Live, kind: &str, data: &Value, now: i64) -> u64 {
    let id = data
        .get("id")
        .and_then(|v| v.as_str())
        .or_else(|| data.pointer("/action/data/id").and_then(|v| v.as_str()))
        .or_else(|| data.get("awaited").and_then(|v| v.as_str()));
    let mut h = 0u64;
    match id.and_then(|i| l.find(i)) {
        None => h = mix(h, 1),
        Some(r) => {
            h = mix(h, 2);
            h = mix(h, if r.state == "pending" { 1 } else { 0 });
            h = mix(h, if r.timeout_at > now { 1 } else { 0 });
            h = mix(h, if r.external { 1 } else { 0 });
            h = mix(h, if r.targeted { 1 } else { 0 });
            h = mix(
                h,
                match r.task_state.as_deref() {
                    None => 0,
                    Some("pending") => 1,
                    Some("acquired") => 2,
                    Some("suspended") => 3,
                    Some("halted") => 4,
                    _ => 5,
                },
            );
            // does the request name the version the store actually holds?
            let v = data.get("version").and_then(|v| v.as_i64());
            h = mix(h, match v {
                None => 0,
                Some(v) if v == r.version as i64 => 1,
                Some(_) => 2,
            });
            h = mix(h, small(r.awaiters));
            h = mix(h, small(r.resumes));
        }
    }
    // `task.suspend`'s malformed shapes are guards in their own right.
    if kind == "task.suspend" {
        let n = data.get("actions").and_then(|v| v.as_array()).map_or(0, |a| a.len());
        h = mix(h, small(n as i64));
    }
    h
}

/// Which operations can actually land against this state.
fn eligible(l: &Live) -> Vec<&'static str> {
    let mut v: Vec<&'static str> =
        vec!["promise.create", "task.create", "schedule.create", "tick"];
    if !l.rows.is_empty() {
        v.push("promise.get");
        v.push("task.get");
    }
    if !l.pending_promises().is_empty() {
        v.push("promise.settle");
        v.push("promise.register_listener");
    }
    if !l.pending_targeted().is_empty() {
        v.push("promise.register_callback");
    }
    if !l.by_task("pending").is_empty() {
        v.push("task.acquire");
    }
    if !l.by_task("acquired").is_empty() {
        v.push("task.release");
        v.push("task.fulfill");
        v.push("task.fence");
        v.push("task.heartbeat");
        if !l.pending_targeted().is_empty() {
            v.push("task.suspend");
        }
    }
    if !l.by_task("halted").is_empty() {
        v.push("task.continue");
    }
    if !l.by_task("pending").is_empty()
        || !l.by_task("acquired").is_empty()
        || !l.by_task("suspended").is_empty()
    {
        v.push("task.halt");
    }
    if !l.schedule_ids.is_empty() {
        v.push("schedule.get");
        v.push("schedule.delete");
    }
    v
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
fn pick_id(t: &mut Tape, v: &[&Row]) -> Option<(String, i32, String)> {
    if v.is_empty() {
        None
    } else {
        let r = v[t.upto(v.len())];
        Some((r.id.clone(), r.version, r.pid.clone().unwrap_or_default()))
    }
}

fn plan(t: &mut Tape, l: &Live, now: i64) -> Option<(String, Value)> {
    let target = if t.byte() % 4 == 0 { "g2" } else { "g1" };
    let elig = eligible(l);
    // One byte in eight ignores eligibility, so the reject paths stay covered.
    let op = if t.byte() % 8 == 0 {
        OPS[t.upto(OPS.len())]
    } else {
        elig[t.upto(elig.len())]
    };
    let fallback = || ("foo.0".to_string(), 0i32, "w0".to_string());
    let d = match op {
        "promise.create" => {
            let root = format!("foo.{}", t.upto(6));
            let id = if t.byte() % 3 == 0 {
                format!("{}:{}", root, t.upto(4))
            } else {
                root.clone()
            };
            // Each tag is rolled INDEPENDENTLY. An earlier version used a match,
            // which made timer and target mutually exclusive — and a store with
            // the timerTargeted guard removed then survived a whole run, because
            // the witness was inexpressible.
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
        "promise.get" => {
            let (id, _, _) = pick_id(t, &l.rows.iter().collect::<Vec<_>>()).unwrap_or_else(fallback);
            json!({ "id": id })
        }
        "promise.settle" => {
            let (id, _, _) = pick_id(t, &l.pending_promises()).unwrap_or_else(fallback);
            json!({ "id": id, "state": one_of(t, SETTLE),
                    "value": { "headers": {}, "data": "v" } })
        }
        "promise.register_callback" => {
            let (a, _, _) = pick_id(t, &l.pending_targeted()).unwrap_or_else(fallback);
            let (b, _, _) = pick_id(t, &l.pending_targeted()).unwrap_or_else(fallback);
            json!({ "awaited": a, "awaiter": b })
        }
        "promise.register_listener" => {
            let (id, _, _) = pick_id(t, &l.pending_promises()).unwrap_or_else(fallback);
            json!({ "awaited": id, "address": one_of(t, ADDRS) })
        }
        "task.create" => {
            let root = format!("foo.{}", t.upto(6));
            let ttl = one_i64(t, TTLS);
            let tags = if t.byte() % 4 == 0 {
                json!({ "resonate:target": target, "resonate:origin": root,
                        "resonate:timer": "true" })
            } else {
                json!({ "resonate:target": target, "resonate:origin": root })
            };
            json!({ "pid": format!("w{}", t.upto(3)), "ttl": ttl,
                    "action": { "data": { "id": root,
                        "timeoutAt": now + (t.word() as i64) * 10,
                        "param": { "headers": {}, "data": null }, "tags": tags } } })
        }
        "task.get" => {
            let (id, _, _) = pick_id(t, &l.rows.iter().collect::<Vec<_>>()).unwrap_or_else(fallback);
            json!({ "id": id })
        }
        "task.acquire" => {
            let (id, v, _) = pick_id(t, &l.by_task("pending")).unwrap_or_else(fallback);
            let ttl = one_i64(t, TTLS);
            // one byte in four names a WRONG version, to keep the fence covered
            let v = if t.byte() % 4 == 0 { v + 1 } else { v };
            json!({ "id": id, "version": v, "pid": format!("w{}", t.upto(3)), "ttl": ttl })
        }
        "task.release" => {
            let (id, v, _) = pick_id(t, &l.by_task("acquired")).unwrap_or_else(fallback);
            let v = if t.byte() % 4 == 0 { v + 1 } else { v };
            json!({ "id": id, "version": v })
        }
        "task.fulfill" => {
            let (id, v, _) = pick_id(t, &l.by_task("acquired")).unwrap_or_else(fallback);
            let st = one_of(t, SETTLE);
            json!({ "id": id, "version": v, "action": { "data": {
                "id": id, "state": st, "value": { "headers": {}, "data": "r" } } } })
        }
        "task.suspend" => {
            let (id, v, _) = pick_id(t, &l.by_task("acquired")).unwrap_or_else(fallback);
            let n = t.upto(4); // 0 is legal input and must be refused
            let mut actions = vec![];
            for _ in 0..n {
                if let Some((a, _, _)) = pick_id(t, &l.pending_targeted()) {
                    actions.push(json!({ "data": { "awaited": a } }));
                }
            }
            json!({ "id": id, "version": v, "actions": actions })
        }
        "task.fence" => {
            let (id, v, _) = pick_id(t, &l.by_task("acquired")).unwrap_or_else(fallback);
            let (other, _, _) = pick_id(t, &l.pending_promises()).unwrap_or_else(fallback);
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
            let (id, v, pid) = pick_id(t, &l.by_task("acquired")).unwrap_or_else(fallback);
            json!({ "pid": pid, "tasks": [{ "id": id, "version": v }] })
        }
        "task.halt" => {
            let mut c: Vec<&Row> = l.by_task("suspended");
            c.extend(l.by_task("pending"));
            c.extend(l.by_task("acquired"));
            let (id, _, _) = pick_id(t, &c).unwrap_or_else(fallback);
            json!({ "id": id })
        }
        "task.continue" => {
            let (id, _, _) = pick_id(t, &l.by_task("halted")).unwrap_or_else(fallback);
            json!({ "id": id })
        }
        "schedule.create" => json!({
            "id": format!("s{}", t.upto(3)), "cron": one_of(t, CRONS),
            "promiseId": "sp.{{.id}}.{{.timestamp}}", "promiseTimeout": 60000,
            "promiseParam": { "headers": {}, "data": null }, "promiseTags": {} }),
        "schedule.get" | "schedule.delete" => {
            let id = if l.schedule_ids.is_empty() {
                format!("s{}", t.upto(3))
            } else {
                l.schedule_ids[t.upto(l.schedule_ids.len())].clone()
            };
            json!({ "id": id })
        }
        "tick" => json!({}),
        _ => unreachable!(),
    };
    Some((op.to_string(), d))
}

// ─── productivity counters ───────────────────────────────────────────────────

static mut OP_TOTAL: [u64; 32] = [0; 32];
static mut OP_OK: [u64; 32] = [0; 32];
static mut EXECS: u64 = 0;

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
        let t = (&raw const OP_TOTAL) as *const u64;
        let k = (&raw const OP_OK) as *const u64;
        eprintln!("[planner] share of each op reaching 2xx/3xx:");
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

fn canon(v: &Value) -> Value {
    let status = v.pointer("/head/status").cloned().unwrap_or(Value::Null);
    let s = status.as_i64().unwrap_or(0);
    if s != 200 && s != 300 {
        return json!({ "status": status });
    }
    let mut out = json!({ "status": status });
    if let Some(d) = v.get("data").and_then(|d| d.as_object()) {
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

fn run_program(
    two: &mut Client,
    one: &mut Client,
    tape: &[u8],
    fb: Feedback,
) -> Option<Finding> {
    const RESET_TWO: &str = "TRUNCATE resonate.outbox, resonate.listeners, resonate.callbacks, \
         resonate.task_resumes, resonate.tasks, resonate.schedules, resonate.promises CASCADE";
    const RESET_ONE: &str =
        "TRUNCATE resonate.outbox, resonate.schedules, resonate.promises CASCADE";
    if two.execute(RESET_TWO, &[]).is_err() || one.execute(RESET_ONE, &[]).is_err() {
        return None;
    }

    let mut t = Tape::new(tape);
    let mut now: i64 = 1_000_000_000;
    let mut step = 0usize;
    let mut prev: u64 = 0; // AFL's prev_location

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
                (Ok(_), Ok(_)) => {
                    (json!({"head":{"status":200}}), json!({"head":{"status":200}}))
                }
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

        // ── FEEDBACK ────────────────────────────────────────────────────────
        let mut cur = mix(mix(0, op_i as u64), s1 as u64);
        cur = mix(cur, match fb {
            Feedback::Shapes => shape_per_object(&live),
            _ => shape_aggregate(&live),
        });
        if fb == Feedback::Preconds || fb == Feedback::Shapes {
            cur = mix(cur, preconds(&live, &kind, &data, now));
        }
        let idx = match fb {
            // Points: the step alone. Edges and above: the step in the context
            // of the one before it, which is AFL's whole trick.
            Feedback::Points => cur,
            _ => mix(prev, cur),
        };
        signal((idx % MAP_SIZE as u64) as usize);
        prev = cur >> 1;

        // ── OBJECTIVE ───────────────────────────────────────────────────────
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
    let fb = Feedback::from_env();
    let max_execs: u64 = std::env::var("FUZZ_MAX_EXECS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(0);
    let quiet = std::env::var("FUZZ_QUIET").is_ok();

    let two_dsn = std::env::var("FUZZ_TWO")
        .unwrap_or_else(|_| "host=/tmp port=5433 user=postgres dbname=res_fuzz_two".into());
    let one_dsn = std::env::var("FUZZ_ONE")
        .unwrap_or_else(|_| "host=/tmp port=5433 user=postgres dbname=res_fuzz_one".into());

    let mut two = Client::connect(&two_dsn, NoTls).expect("connect two-table store");
    let mut one = Client::connect(&one_dsn, NoTls).expect("connect single-table store");

    let findings_dir = PathBuf::from("./findings");
    std::fs::create_dir_all(&findings_dir).ok();

    eprintln!("[fuzz] feedback={} max_execs={}", fb.name(), max_execs);

    let mut harness = |input: &BytesInput| {
        let bytes = input.target_bytes();
        clear_map();
        let n = unsafe {
            let p = (&raw mut EXECS) as *mut u64;
            *p += 1;
            *p
        };
        let r = run_program(&mut two, &mut one, &bytes, fb);
        match r {
            Some(f) => {
                // The measurement the battery reads: how many executions until
                // the objective fired.
                println!("RESULT found=1 execs={} feedback={} why={} op={}",
                         n, fb.name(), f.why, f.kind);
                if !quiet {
                    let report = json!({
                        "why": f.why, "step": f.step, "op": f.kind, "request": f.data,
                        "two_table": f.two, "single_table": f.one,
                        "execs": n, "feedback": fb.name(),
                    });
                    std::fs::write(findings_dir.join("finding.json"),
                                   serde_json::to_string_pretty(&report).unwrap()).ok();
                    dump_counts();
                }
                std::io::Write::flush(&mut std::io::stdout()).ok();
                std::process::exit(0);
            }
            None => {
                if max_execs > 0 && n >= max_execs {
                    println!("RESULT found=0 execs={} feedback={}", n, fb.name());
                    if !quiet {
                        dump_counts();
                    }
                    std::io::Write::flush(&mut std::io::stdout()).ok();
                    std::process::exit(0);
                }
                ExitKind::Ok
            }
        }
    };

    let observer = unsafe { StdMapObserver::new("signatures", &mut *(&raw mut SIGNALS)) };
    let mut feedback = MaxMapFeedback::new(&observer);
    let mut objective = CrashFeedback::new();

    let mut state = StdState::new(
        StdRand::with_seed(current_nanos()),
        InMemoryCorpus::new(),
        OnDiskCorpus::new(findings_dir.join("tapes")).unwrap(),
        &mut feedback,
        &mut objective,
    )
    .unwrap();

    let mon = SimpleMonitor::new(move |s| {
        if !quiet {
            println!("{s}");
        }
    });
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

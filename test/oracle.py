"""The reference engine, ported from resonatehq/resonate.

Source: `crates/resonate-server-dbms/src/oracle.rs` on branch `core-crate` —
the in-memory model the Rust server's differential holds beside its real
backends. This is a transcription, not an import: same state, same transitions,
same order of guards.

WHAT WAS ADAPTED, AND WHY
-------------------------
The Rust engine port (`engine_port.rs`) returns what a transition emitted:

    pub struct Output { response, messages: Vec<Outgoing>, timeouts: Vec<Scheduled> }

and its doc says why — "Was an outbox row; now the result", "which is what lets
the outbox and the pump that drains it go away, one backend at a time".
resonate-pg has not made that move. It still writes an outbox that a pump
drains, and it still arms deadlines into columns that `process_timeouts`
sweeps. So the two halves of `Output` that exist to replace those are dropped
here:

  * `emitted` / `take_emitted` and the `Outgoing` type — gone. Messages go into
    `self.outbox`, keyed the way `resonate.outbox` keys them, so the model's
    queue and the database's table compare directly. The oracle already kept
    this queue alongside the returned form, precisely for backends that had not
    ported; this keeps only that half.
  * `armed` / `take_armed`, `Scheduled` and `Timeout` — gone. The three timeout
    tables stay, and `tick(now)` sweeps them, which is what
    `resonate.process_timeouts(now)` does.

What is left is `apply(request) -> response` and `tick(now)`, which is exactly
the surface `resonate_rpc` and `process_timeouts` present.

WHERE THE TWO PROTOCOLS HAVE DRIFTED
------------------------------------
The `core-crate` branch is ahead of resonate-pg in ways that are not bugs in
either. Left unparameterised they would swamp a differential run with noise, so
each is a knob on `Compat`, defaulting to resonate-pg's reading:

  * `retry_ttl` — the model's PENDING_RETRY_TTL is 30 000ms; resonate-pg's
    `_retry_timeout()` is 5 000.
  * `validate_target_address` — the model 400s a `resonate:target` that is not
    a parseable URL (`is_valid_address` is `Url::parse(..).is_ok()`), so it
    rejects `resonate:target = "g1"`. resonate-pg treats a target as a worker
    group name and validates nothing. The specification sides with resonate-pg
    and says so explicitly.
  * `direct_resume` — the model's `promise.register_callback` wakes a suspended
    awaiter when the awaited is ALREADY settled. resonate-pg stores nothing and
    returns 200.
  * `promise_timeout_needs_target` — the model arms a promise timeout only for
    targeted promises; resonate-pg arms one for every `external` promise
    (target, timer, or the external tag).
  * `search` — the model implements promise/task/schedule search; resonate-pg
    answers 501.

Set `Compat.strict()` to run the model as the Rust source has it.

NOT PORTED
----------
Field-level request validation. The Rust `parse` runs `validator` derives off
`resonate-core::types`, and those rules are not in this repo. This port checks
only what the transitions actually branch on — a missing id, an unsettleable
state — so it can disagree with either server on the exact shape of a 400.
"""

from __future__ import annotations

import copy
import datetime as _dt
from dataclasses import dataclass, field
from typing import Any

PENDING_RETRY_TTL = 30_000          # oracle.rs
RESONATE_PG_RETRY_TTL = 5_000       # resonate.sql `_retry_timeout()`

PENDING = "pending"
RESOLVED = "resolved"
REJECTED = "rejected"
REJECTED_CANCELED = "rejected_canceled"
REJECTED_TIMEDOUT = "rejected_timedout"
SETTLEABLE = (RESOLVED, REJECTED, REJECTED_CANCELED)

T_PENDING, T_ACQUIRED, T_SUSPENDED, T_HALTED, T_FULFILLED = (
    "pending", "acquired", "suspended", "halted", "fulfilled")

RETRY, LEASE = 0, 1                 # TTimeoutKind


# ─── compatibility knobs ─────────────────────────────────────────────────────

@dataclass
class Compat:
    """Where `core-crate` and resonate-pg read the protocol differently."""
    retry_ttl: int = RESONATE_PG_RETRY_TTL
    validate_target_address: bool = False
    validate_listener_address: bool = True
    direct_resume: bool = False
    promise_timeout_needs_target: bool = False
    search: bool = False

    @staticmethod
    def strict() -> "Compat":
        """The model exactly as oracle.rs has it."""
        return Compat(retry_ttl=PENDING_RETRY_TTL, validate_target_address=True,
                      validate_listener_address=True, direct_resume=True,
                      promise_timeout_needs_target=True, search=True)


def is_valid_url(a: str) -> bool:
    """`resonate_core::is_valid_address` — `Url::parse(a).is_ok()`.

    Url::parse wants an absolute URL: a scheme, a colon, and a non-empty
    scheme that starts with a letter. A bare `g1` is a relative reference and
    fails.
    """
    i = a.find(":")
    if i <= 0:
        return False
    s = a[:i]
    return s[0].isalpha() and all(c.isalnum() or c in "+-." for c in s)


def is_addr_valid_pg(a: str) -> bool:
    """resonate-pg's listener rule, which is also the specification's."""
    return (a.startswith("http://") or a.startswith("https://")
            or (a.startswith("poll://") and "@" in a))


# ─── cron ────────────────────────────────────────────────────────────────────
# resonate-pg's `_next_cron` / `_cron_field`, transcribed. The Rust side uses
# the `cron` crate over a normalised 6-field expression; where the two disagree
# the disagreement is about cron, not about the protocol, so the model uses the
# one the database it is being compared against uses.

def _cron_field(spec: str, lo: int, hi: int) -> list[int]:
    acc: list[int] = []
    for part in spec.split(","):
        rng, step = part, 1
        if "/" in part:
            rng, s = part.split("/", 1)
            step = int(s)
            if step < 1:
                raise ValueError(f"invalid cron step: {spec}")
        if rng == "*":
            a, b = lo, hi
        elif "-" in rng:
            a, b = (int(x) for x in rng.split("-", 1))
        else:
            a = int(rng)
            b = hi if "/" in part else a
        acc.extend(range(a, b + 1, step))
    return acc


def next_cron(cron: str, after_ms: int) -> int:
    f = cron.split()
    if len(f) != 5:
        raise ValueError(f"unsupported cron (need 5 fields): {cron}")
    mins, hrs = _cron_field(f[0], 0, 59), _cron_field(f[1], 0, 23)
    doms, mons = _cron_field(f[2], 1, 31), _cron_field(f[3], 1, 12)
    dows = _cron_field(f[4], 0, 6)
    dom_star, dow_star = f[2] == "*", f[4] == "*"

    ts = _dt.datetime.fromtimestamp(after_ms / 1000.0, _dt.timezone.utc)
    ts = ts.replace(second=0, microsecond=0) + _dt.timedelta(minutes=1)
    cutoff = ts + _dt.timedelta(days=365 * 9)
    while ts < cutoff:
        if ts.month not in mons:
            ts = (ts.replace(day=1, hour=0, minute=0)
                  + _dt.timedelta(days=32)).replace(day=1, hour=0, minute=0)
            continue
        dow = (ts.weekday() + 1) % 7          # postgres: Sunday = 0
        if dom_star and dow_star:
            ok = True
        elif dom_star:
            ok = dow in dows
        elif dow_star:
            ok = ts.day in doms
        else:
            ok = ts.day in doms or dow in dows
        if not ok:
            ts = ts.replace(hour=0, minute=0) + _dt.timedelta(days=1)
            continue
        if ts.hour not in hrs:
            ts = ts.replace(minute=0) + _dt.timedelta(hours=1)
            continue
        if ts.minute in mins:
            return int(ts.timestamp() * 1000)
        ts += _dt.timedelta(minutes=1)
    raise ValueError(f"no cron match within horizon: {cron}")


# ─── state ───────────────────────────────────────────────────────────────────

@dataclass
class Promise:
    state: str
    param: dict
    value: dict
    tags: dict
    timeout_at: int
    created_at: int
    settled_at: int | None = None
    callbacks: set = field(default_factory=set)
    listeners: set = field(default_factory=set)


@dataclass
class Task:
    state: str
    version: int
    pid: str | None = None
    ttl: int | None = None
    resumes: set = field(default_factory=set)


@dataclass
class Schedule:
    cron: str
    promise_id: str
    promise_timeout: int
    promise_param: dict
    promise_tags: dict
    created_at: int
    last_run_at: int | None = None


def _val(v):
    v = v or {}
    return {"headers": v.get("headers") or {}, "data": v.get("data")}


class Oracle:
    def __init__(self, compat: Compat | None = None):
        self.compat = compat or Compat()
        self.reset()

    def reset(self):
        self.promises: dict[str, Promise] = {}
        self.tasks: dict[str, Task] = {}
        self.schedules: dict[str, Schedule] = {}
        self.p_timeouts: dict[str, int] = {}
        self.t_timeouts: dict[str, tuple[int, int]] = {}     # id -> (kind, at)
        self.s_timeouts: dict[str, int] = {}
        self.outbox: dict[str, dict] = {}                    # key -> row

    # ─── entry points ────────────────────────────────────────────────────────

    def apply(self, req: dict) -> dict:
        kind = req.get("kind", "")
        head = req.get("head") or {}
        corr = head.get("corrId", "")
        now = int(head.get("resonate:debug_time"))
        d = req.get("data") or {}
        op = getattr(self, "_op_" + kind.replace(".", "_"), None)
        if op is None:
            return self._err(kind, corr, 400, f"Unknown operation: {kind}")
        return op(kind, corr, d, now)

    def tick(self, now: int) -> None:
        """The bulk sweep. `resonate.process_timeouts(now)`."""
        self._tick(now)

    # ─── responses ───────────────────────────────────────────────────────────

    @staticmethod
    def _ok(kind, corr, data):
        return {"kind": kind, "head": {"corrId": corr, "status": 200,
                                       "version": "1"}, "data": data}

    @staticmethod
    def _resp(kind, corr, status, data):
        return {"kind": kind, "head": {"corrId": corr, "status": status,
                                       "version": "1"}, "data": data}

    @staticmethod
    def _err(kind, corr, status, msg):
        return {"kind": kind, "head": {"corrId": corr, "status": status,
                                       "version": "1"}, "data": msg}

    # ─── records ─────────────────────────────────────────────────────────────

    @staticmethod
    def _timeout_state(tags) -> str:
        return RESOLVED if tags.get("resonate:timer") == "true" else REJECTED_TIMEDOUT

    def _promise_record(self, now: int, pid: str, p: Promise) -> dict:
        state, settled = p.state, p.settled_at
        if p.state == PENDING and now > 0 and now >= p.timeout_at:
            state, settled = self._timeout_state(p.tags), p.timeout_at
        return {"id": pid, "state": state, "param": _val(p.param),
                "value": _val(p.value), "tags": dict(p.tags),
                "timeoutAt": p.timeout_at, "createdAt": p.created_at,
                "settledAt": settled}

    @staticmethod
    def _task_record(tid: str, t: Task) -> dict:
        return {"id": tid, "state": t.state, "version": t.version,
                "resumes": len(t.resumes), "ttl": t.ttl, "pid": t.pid}

    def _schedule_record(self, sid: str, s: Schedule) -> dict:
        return {"id": sid, "cron": s.cron, "promiseId": s.promise_id,
                "promiseTimeout": s.promise_timeout,
                "promiseParam": _val(s.promise_param),
                "promiseTags": dict(s.promise_tags),
                "createdAt": s.created_at,
                "nextRunAt": self.s_timeouts.get(sid, 0),
                "lastRunAt": s.last_run_at}

    # ─── outbox (was: returned messages) ─────────────────────────────────────

    def _send_execute(self, address: str, task_id: str, version: int) -> None:
        # resonate-pg keys an execute row on the task id and upserts, which is
        # what the model's linear scan-and-replace does.
        self.outbox[task_id] = {"key": task_id, "kind": "execute",
                                "address": address, "taskId": task_id,
                                "version": version, "promise": None}

    def _send_unblock(self, address: str, record: dict) -> None:
        key = f"{record['id']}:notify:{address}"
        self.outbox[key] = {"key": key, "kind": "unblock", "address": address,
                            "taskId": None, "version": None,
                            "promise": copy.deepcopy(record)}

    # ─── timeout tables (was: returned Scheduled) ────────────────────────────

    def _set_p_timeout(self, pid, at):
        self.p_timeouts[pid] = at

    def _del_p_timeout(self, pid):
        self.p_timeouts.pop(pid, None)

    def _set_t_timeout(self, tid, kind, at):
        self.t_timeouts[tid] = (kind, at)

    def _del_t_timeout(self, tid):
        self.t_timeouts.pop(tid, None)

    def _set_s_timeout(self, sid, at):
        self.s_timeouts[sid] = at

    def _del_s_timeout(self, sid):
        self.s_timeouts.pop(sid, None)

    # ─── promise operations ──────────────────────────────────────────────────

    def _op_promise_get(self, kind, corr, d, now):
        pid = d.get("id")
        self._try_timeout([pid], now)
        p = self.promises.get(pid)
        if p is None:
            return self._err(kind, corr, 404, "Promise not found")
        return self._ok(kind, corr, {"promise": self._promise_record(now, pid, p)})

    def _create_promise(self, pid, timeout_at, param, tags, now):
        """The body shared by promise.create, task.fence(create) and a schedule
        firing. Returns the record as of creation."""
        addr = tags.get("resonate:target")
        already = now >= timeout_at
        if already:
            state, created_at, settled_at = self._timeout_state(tags), timeout_at, timeout_at
        else:
            state, created_at, settled_at = PENDING, now, None
        p = Promise(state=state, param=_val(param), value=_val(None), tags=dict(tags),
                    timeout_at=timeout_at, created_at=created_at, settled_at=settled_at)
        record = self._promise_record(now, pid, p)
        self.promises[pid] = p

        if already:
            if addr is not None:
                self.tasks[pid] = Task(state=T_FULFILLED, version=0)
            return record

        if addr is not None or not self.compat.promise_timeout_needs_target:
            if addr is not None or self._external(p):
                self._set_p_timeout(pid, timeout_at)
        if addr is not None:
            self.tasks[pid] = Task(state=T_PENDING, version=0)
            delay = tags.get("resonate:delay")
            delay_at = int(delay) if delay is not None and delay.lstrip("-").isdigit() else None
            if delay_at is not None and now < delay_at:
                self._set_t_timeout(pid, RETRY, delay_at)
            else:
                self._set_t_timeout(pid, RETRY, created_at + self.compat.retry_ttl)
                self._send_execute(addr, pid, 0)
        return record

    @staticmethod
    def _external(p: Promise) -> bool:
        t = p.tags
        return ("resonate:target" in t or t.get("resonate:timer") == "true"
                or t.get("resonate:external") == "true")

    def _op_promise_create(self, kind, corr, d, now):
        pid, tags = d.get("id"), d.get("tags") or {}
        addr = tags.get("resonate:target")
        if self.compat.validate_target_address and addr is not None and not is_valid_url(addr):
            return self._err(kind, corr, 400, "Invalid resonate:target address")
        self._try_timeout([pid], now)
        if pid in self.promises:
            return self._ok(kind, corr,
                            {"promise": self._promise_record(now, pid, self.promises[pid])})
        rec = self._create_promise(pid, int(d["timeoutAt"]), d.get("param"), tags, now)
        return self._ok(kind, corr, {"promise": rec})

    def _op_promise_settle(self, kind, corr, d, now):
        pid, st = d.get("id"), d.get("state")
        if st not in SETTLEABLE:
            return self._err(kind, corr, 400, "Invalid state")
        self._try_timeout([pid], now)
        p = self.promises.get(pid)
        if p is None:
            return self._err(kind, corr, 404, "Promise not found")
        if p.state != PENDING:
            return self._ok(kind, corr, {"promise": self._promise_record(now, pid, p)})
        p.state, p.value, p.settled_at = st, _val(d.get("value")), now
        rec = self._promise_record(now, pid, p)
        self._del_p_timeout(pid)
        self._trigger_settlement(pid, now)
        return self._ok(kind, corr, {"promise": rec})

    def _op_promise_register_callback(self, kind, corr, d, now):
        awaited, awaiter = d.get("awaited"), d.get("awaiter")
        self._try_timeout([awaited, awaiter], now)
        pa = self.promises.get(awaited)
        if pa is None:
            return self._err(kind, corr, 404, "Awaited promise not found")
        awaited_record = self._promise_record(now, awaited, pa)
        pw = self.promises.get(awaiter)
        if pw is None:
            return self._err(kind, corr, 422, "Awaiter promise not found")
        if "resonate:target" not in pw.tags:
            return self._err(kind, corr, 422, "Awaiter promise has no resonate:target tag")

        awaited_pending = awaited_record["state"] == PENDING
        awaiter_pending = pw.state == PENDING
        if awaited_pending and awaiter_pending:
            pa.callbacks.add(awaiter)
        elif not awaited_pending and awaiter_pending and self.compat.direct_resume:
            # Direct resume: the awaited had already settled. Only a suspended
            # awaiter needs waking; a running one sees the settled promise in
            # the response.
            t = self.tasks.get(awaiter)
            if t is not None:
                if t.state == T_SUSPENDED:
                    t.state = T_PENDING
                    self._set_t_timeout(awaiter, RETRY, now + self.compat.retry_ttl)
                    a = pw.tags.get("resonate:target")
                    if a is not None:
                        self._send_execute(a, awaiter, t.version)
                elif t.state in (T_PENDING, T_ACQUIRED):
                    t.resumes.add(awaited)
        return self._ok(kind, corr, {"promise": awaited_record})

    def _op_promise_register_listener(self, kind, corr, d, now):
        awaited, address = d.get("awaited"), d.get("address")
        if self.compat.validate_listener_address:
            valid = is_valid_url(address) if self.compat.direct_resume else is_addr_valid_pg(address)
            if not address or not valid:
                return self._err(kind, corr, 400, "Invalid listener address")
        self._try_timeout([awaited], now)
        p = self.promises.get(awaited)
        if p is None:
            return self._err(kind, corr, 404, "Awaited promise not found")
        if p.state == PENDING:
            p.listeners.add(address)
        return self._ok(kind, corr, {"promise": self._promise_record(now, awaited, p)})

    def _op_promise_search(self, kind, corr, d, now):
        if not self.compat.search:
            return self._err(kind, corr, 501, "not implemented")
        rows = [self._promise_record(0, i, p) for i, p in self.promises.items()]
        rows.sort(key=lambda r: r["id"])
        return self._ok(kind, corr, {"promises": rows, "cursor": None})

    # ─── task operations ─────────────────────────────────────────────────────

    def _effective_task_state(self, now, tid):
        t = self.tasks.get(tid)
        if t is None:
            return None
        if t.state == T_FULFILLED:
            return T_FULFILLED
        p = self.promises.get(tid)
        if p is not None and (p.state != PENDING or now >= p.timeout_at):
            return T_FULFILLED
        return t.state

    def _op_task_get(self, kind, corr, d, now):
        tid = d.get("id")
        self._try_timeout([tid], now)
        t = self.tasks.get(tid)
        if t is None:
            return self._err(kind, corr, 404, "Task not found")
        eff = self._effective_task_state(now, tid) or t.state
        return self._ok(kind, corr, {"task": {
            "id": tid, "state": eff, "version": t.version,
            "resumes": len(t.resumes),
            "ttl": None if eff == T_FULFILLED else t.ttl,
            "pid": None if eff == T_FULFILLED else t.pid}})

    def _op_task_create(self, kind, corr, d, now):
        action = (d.get("action") or {}).get("data") or {}
        tags = action.get("tags") or {}
        addr = tags.get("resonate:target")
        if self.compat.validate_target_address and addr is not None and not is_valid_url(addr):
            return self._err(kind, corr, 400, "Invalid resonate:target address")
        pid, ttl = d.get("pid"), int(d.get("ttl"))
        promise_id = action.get("id")
        self._try_timeout([promise_id], now)

        t = self.tasks.get(promise_id)
        if t is not None:
            eff = self._effective_task_state(now, promise_id) or t.state
            if eff == T_PENDING:
                t.state, t.version = T_ACQUIRED, t.version + 1
                t.pid, t.ttl, t.resumes = pid, ttl, set()
                self._del_t_timeout(promise_id)
                self._set_t_timeout(promise_id, LEASE, now + ttl)
            elif eff != T_FULFILLED:
                return self._err(kind, corr, 409, "Already exists")
            return self._ok(kind, corr, {
                "task": self._task_record(promise_id, self.tasks[promise_id]),
                "promise": self._promise_record(now, promise_id,
                                                self.promises[promise_id]),
                "preload": self._preload(promise_id)})

        if promise_id in self.promises:
            return self._err(kind, corr, 422,
                             "The promise does not have a resonate:target tag")

        timeout_at = int(action["timeoutAt"])
        already = now >= timeout_at
        if already:
            state, created_at, settled_at = self._timeout_state(tags), timeout_at, timeout_at
        else:
            state, created_at, settled_at = PENDING, now, None
        p = Promise(state=state, param=_val(action.get("param")), value=_val(None),
                    tags=dict(tags), timeout_at=timeout_at, created_at=created_at,
                    settled_at=settled_at)
        promise_record = self._promise_record(now, promise_id, p)
        self.promises[promise_id] = p
        if already:
            self.tasks[promise_id] = Task(state=T_FULFILLED, version=0)
        else:
            self.tasks[promise_id] = Task(state=T_ACQUIRED, version=1, pid=pid, ttl=ttl)
            self._set_p_timeout(promise_id, timeout_at)
            self._set_t_timeout(promise_id, LEASE, now + ttl)
            # task.create hands an already-acquired task to the caller, who IS
            # the worker, so no execute is emitted.
        return self._ok(kind, corr, {
            "task": self._task_record(promise_id, self.tasks[promise_id]),
            "promise": promise_record, "preload": self._preload(promise_id)})

    def _op_task_acquire(self, kind, corr, d, now):
        tid, version, pid, ttl = d.get("id"), d.get("version"), d.get("pid"), d.get("ttl")
        self._try_timeout([tid], now)
        t = self.tasks.get(tid)
        if t is None:
            return self._err(kind, corr, 404, "Task not found")
        if t.state != T_PENDING:
            return self._err(kind, corr, 409, "Task is not pending")
        if t.version != version:
            return self._err(kind, corr, 409, "Version mismatch")
        p = self.promises.get(tid)
        if p is None:
            return self._err(kind, corr, 404, "Task not found")
        promise = self._promise_record(now, tid, p)
        t.state, t.version = T_ACQUIRED, version + 1
        t.pid, t.ttl, t.resumes = pid, int(ttl), set()
        self._del_t_timeout(tid)
        self._set_t_timeout(tid, LEASE, now + int(ttl))
        return self._ok(kind, corr, {
            "task": {"id": tid, "state": T_ACQUIRED, "version": t.version,
                     "resumes": 0, "ttl": t.ttl, "pid": t.pid},
            "promise": promise, "preload": self._preload(tid)})

    def _op_task_release(self, kind, corr, d, now):
        tid, version = d.get("id"), d.get("version")
        self._try_timeout([tid], now)
        t = self.tasks.get(tid)
        if t is None:
            return self._err(kind, corr, 404, "Task not found")
        if t.state != T_ACQUIRED or t.version != version:
            return self._err(kind, corr, 409, "Task version mismatch or invalid state")
        p = self.promises.get(tid)
        addr = p.tags.get("resonate:target") if p else None
        t.state, t.pid, t.ttl = T_PENDING, None, None
        self._del_t_timeout(tid)
        self._set_t_timeout(tid, RETRY, now + self.compat.retry_ttl)
        if addr is not None:
            self._send_execute(addr, tid, t.version)
        return self._resp(kind, corr, 200, {})

    def _op_task_fulfill(self, kind, corr, d, now):
        action = (d.get("action") or {}).get("data") or {}
        tid, version = d.get("id"), d.get("version")
        aid, st = action.get("id", tid), action.get("state")
        if st not in SETTLEABLE:
            return self._err(kind, corr, 400, "Invalid state")
        self._try_timeout([aid], now)
        t = self.tasks.get(tid)
        if t is None:
            return self._err(kind, corr, 404, "Task not found")
        if t.state != T_ACQUIRED or t.version != version:
            return self._err(kind, corr, 409, "Task version mismatch or invalid state")
        p = self.promises.get(aid)
        if p is None:
            return self._err(kind, corr, 404, "Promise not found")
        if p.state != PENDING:
            rec = self._promise_record(now, aid, p)
            self._trigger_fulfilled(tid)
            return self._ok(kind, corr, {"promise": rec})
        p.state, p.value, p.settled_at = st, _val(action.get("value")), now
        rec = self._promise_record(now, aid, p)
        self._del_p_timeout(aid)
        self._trigger_settlement(aid, now)
        return self._ok(kind, corr, {"promise": rec})

    def _op_task_suspend(self, kind, corr, d, now):
        tid, version = d.get("id"), d.get("version")
        actions = d.get("actions") or []
        awaited_ids = [(a.get("data") or {}).get("awaited") for a in actions]
        self._try_timeout([tid] + awaited_ids, now)
        t = self.tasks.get(tid)
        if t is None:
            return self._err(kind, corr, 404, "Task not found")
        if t.state != T_ACQUIRED or t.version != version:
            return self._err(kind, corr, 409, "Task is not acquired or version mismatch")
        for a in awaited_ids:
            if a not in self.promises:
                return self._err(kind, corr, 422, "Awaited promise not found")
        unique = list(dict.fromkeys(awaited_ids))
        if any(self.promises[a].state != PENDING for a in unique):
            t.resumes = set()
            return self._resp(kind, corr, 300, {"preload": self._preload(tid)})
        for a in unique:
            self.promises[a].callbacks.add(tid)
        t.state, t.pid, t.ttl, t.resumes = T_SUSPENDED, None, None, set()
        self._del_t_timeout(tid)
        return self._resp(kind, corr, 200, {})

    def _op_task_fence(self, kind, corr, d, now):
        tid, version = d.get("id"), d.get("version")
        action = d.get("action") or {}
        inner_kind, inner = action.get("kind"), action.get("data") or {}
        self._try_timeout([tid, inner.get("id")], now)
        t = self.tasks.get(tid)
        if t is None:
            return self._err(kind, corr, 404, "Task not found")
        if t.state != T_ACQUIRED or t.version != version:
            return self._err(kind, corr, 409, "Version mismatch")

        if inner_kind == "promise.create":
            addr = (inner.get("tags") or {}).get("resonate:target")
            if self.compat.validate_target_address and addr is not None and not is_valid_url(addr):
                return self._err(kind, corr, 400, "Invalid resonate:target address")
            iid = inner.get("id")
            self._try_timeout([iid], now)
            if iid in self.promises:
                rec = self._promise_record(now, iid, self.promises[iid])
            else:
                rec = self._create_promise(iid, int(inner["timeoutAt"]),
                                           inner.get("param"),
                                           inner.get("tags") or {}, now)
            inner_env = {"kind": inner_kind,
                         "head": {"corrId": corr, "status": 200, "version": "1"},
                         "data": {"promise": rec}}
        elif inner_kind == "promise.settle":
            iid, st = inner.get("id"), inner.get("state")
            if st not in SETTLEABLE:
                return self._err(kind, corr, 400, "Invalid action data")
            p = self.promises.get(iid)
            if p is not None and p.state == PENDING:
                p.state, p.value, p.settled_at = st, _val(inner.get("value")), now
                self._del_p_timeout(iid)
                self._trigger_settlement(iid, now)
            p = self.promises.get(iid)
            status = 200 if p is not None else 404
            data = ({"promise": self._promise_record(now, iid, p)} if p is not None
                    else "Promise not found")
            inner_env = {"kind": inner_kind,
                         "head": {"corrId": corr, "status": status, "version": "1"},
                         "data": data}
        else:
            return self._err(kind, corr, 400, "Invalid fence action kind")
        return self._ok(kind, corr, {"action": inner_env, "preload": self._preload(tid)})

    def _op_task_heartbeat(self, kind, corr, d, now):
        for ref in d.get("tasks") or []:
            t = self.tasks.get(ref.get("id"))
            if (t is not None and t.state == T_ACQUIRED
                    and t.version == ref.get("version") and t.pid == d.get("pid")
                    and t.ttl is not None):
                self._set_t_timeout(ref["id"], LEASE, now + t.ttl)
        return self._resp(kind, corr, 200, {})

    def _op_task_halt(self, kind, corr, d, now):
        tid = d.get("id")
        self._try_timeout([tid], now)
        t = self.tasks.get(tid)
        if t is None:
            return self._err(kind, corr, 404, "Task not found")
        if t.state == T_FULFILLED:
            return self._err(kind, corr, 409, "Task is fulfilled")
        if t.state == T_HALTED:
            return self._resp(kind, corr, 200, {})
        t.state, t.pid, t.ttl = T_HALTED, None, None
        self._del_t_timeout(tid)
        return self._resp(kind, corr, 200, {})

    def _op_task_continue(self, kind, corr, d, now):
        tid = d.get("id")
        self._try_timeout([tid], now)
        t = self.tasks.get(tid)
        if t is None:
            return self._err(kind, corr, 404, "Task not found")
        if t.state != T_HALTED:
            return self._err(kind, corr, 409, "Task is not halted")
        p = self.promises.get(tid)
        addr = p.tags.get("resonate:target") if p else None
        t.state = T_PENDING
        self._set_t_timeout(tid, RETRY, now + self.compat.retry_ttl)
        if addr is not None:
            self._send_execute(addr, tid, t.version)
        return self._resp(kind, corr, 200, {})

    def _op_task_search(self, kind, corr, d, now):
        if not self.compat.search:
            return self._err(kind, corr, 501, "not implemented")
        rows = [self._task_record(i, t) for i, t in self.tasks.items()]
        rows.sort(key=lambda r: r["id"])
        return self._ok(kind, corr, {"tasks": rows, "cursor": None})

    # ─── schedule operations ─────────────────────────────────────────────────

    def _op_schedule_get(self, kind, corr, d, now):
        sid = d.get("id")
        s = self.schedules.get(sid)
        if s is None:
            return self._err(kind, corr, 404, "Schedule not found")
        return self._ok(kind, corr, {"schedule": self._schedule_record(sid, s)})

    def _op_schedule_create(self, kind, corr, d, now):
        sid = d.get("id")
        try:
            nxt = next_cron(d.get("cron") or "", now)
        except Exception:
            return self._err(kind, corr, 400, "Invalid cron expression")
        s = self.schedules.get(sid)
        if s is not None:
            return self._ok(kind, corr, {"schedule": self._schedule_record(sid, s)})
        self.schedules[sid] = Schedule(
            cron=d["cron"], promise_id=d.get("promiseId"),
            promise_timeout=int(d.get("promiseTimeout")),
            promise_param=_val(d.get("promiseParam")),
            promise_tags=d.get("promiseTags") or {}, created_at=now)
        self._set_s_timeout(sid, nxt)
        return self._ok(kind, corr,
                        {"schedule": self._schedule_record(sid, self.schedules[sid])})

    def _op_schedule_delete(self, kind, corr, d, now):
        sid = d.get("id")
        if self.schedules.pop(sid, None) is None:
            return self._err(kind, corr, 404, "Schedule not found")
        self._del_s_timeout(sid)
        return self._resp(kind, corr, 200, {})

    def _op_schedule_search(self, kind, corr, d, now):
        if not self.compat.search:
            return self._err(kind, corr, 501, "not implemented")
        rows = [self._schedule_record(i, s) for i, s in self.schedules.items()]
        rows.sort(key=lambda r: r["id"])
        return self._ok(kind, corr, {"schedules": rows, "cursor": None})

    # ─── internal helpers ────────────────────────────────────────────────────

    def _try_timeout(self, ids, now):
        for pid in [i for i in ids if i is not None]:
            p = self.promises.get(pid)
            if p is None or p.state != PENDING or now < p.timeout_at:
                continue
            p.state, p.settled_at = self._timeout_state(p.tags), p.timeout_at
            self._del_p_timeout(pid)
            self._trigger_settlement(pid, now)

    def _trigger_settlement(self, pid, now):
        self._trigger_fulfilled(pid)
        self._trigger_callbacks(pid, now)
        self._trigger_listeners(pid, now)

    def _trigger_fulfilled(self, pid):
        t = self.tasks.get(pid)
        if t is None or t.state == T_FULFILLED:
            return
        t.state, t.pid, t.ttl, t.resumes = T_FULFILLED, None, None, set()
        self._del_t_timeout(pid)
        for p in self.promises.values():
            p.callbacks.discard(pid)

    def _trigger_callbacks(self, pid, now):
        p = self.promises.get(pid)
        if p is None:
            return
        callbacks, p.callbacks = sorted(p.callbacks), set()
        for awaiter in callbacks:
            pw = self.promises.get(awaiter)
            if pw is None or pw.state != PENDING or now >= pw.timeout_at:
                continue
            t = self.tasks.get(awaiter)
            if t is None:
                continue
            addr = pw.tags.get("resonate:target")
            if t.state == T_SUSPENDED:
                t.state, t.resumes = T_PENDING, {pid}
                self._set_t_timeout(awaiter, RETRY, now + self.compat.retry_ttl)
                if addr is not None:
                    self._send_execute(addr, awaiter, t.version)
            elif t.state in (T_PENDING, T_ACQUIRED, T_HALTED):
                t.resumes.add(pid)

    def _trigger_listeners(self, pid, now):
        p = self.promises.get(pid)
        if p is None or not p.listeners:
            return
        listeners, p.listeners = sorted(p.listeners), set()
        record = self._promise_record(now, pid, p)
        for addr in listeners:
            self._send_unblock(addr, record)

    def _preload(self, pid):
        p = self.promises.get(pid)
        branch = (p.tags.get("resonate:branch") if p else None) or ""
        if not branch:
            return []
        return [self._promise_record(0, i, q) for i, q in sorted(self.promises.items())
                if i != pid and q.tags.get("resonate:branch") == branch]

    # ─── the sweep ───────────────────────────────────────────────────────────

    def _tick(self, now):
        expired_promises = [i for i, at in self.p_timeouts.items() if now >= at]
        expired_leases = [(i, self.tasks[i].version)
                          for i, (k, at) in self.t_timeouts.items()
                          if now >= at and k == LEASE
                          and i in self.tasks and self.tasks[i].state == T_ACQUIRED]
        expired_retries = [i for i, (k, at) in self.t_timeouts.items()
                           if now >= at and k == RETRY
                           and i in self.tasks and self.tasks[i].state == T_PENDING]

        for pid in sorted(expired_promises):
            p = self.promises.get(pid)
            if p is not None:
                p.state, p.settled_at = self._timeout_state(p.tags), p.timeout_at
            self._del_p_timeout(pid)
        for pid in sorted(expired_promises):
            self._trigger_settlement(pid, now)

        for tid, version in sorted(expired_leases):
            t = self.tasks.get(tid)
            if t is None or t.state != T_ACQUIRED or t.version != version:
                continue
            p = self.promises.get(tid)
            addr = p.tags.get("resonate:target") if p else None
            t.state, t.pid, t.ttl = T_PENDING, None, None
            self._del_t_timeout(tid)
            self._set_t_timeout(tid, RETRY, now + self.compat.retry_ttl)
            if addr is not None:
                self._send_execute(addr, tid, t.version)

        for tid in sorted(expired_retries):
            t = self.tasks.get(tid)
            if t is None or t.state != T_PENDING:
                continue
            p = self.promises.get(tid)
            addr = p.tags.get("resonate:target") if p else None
            self._set_t_timeout(tid, RETRY, now + self.compat.retry_ttl)
            if addr is not None:
                self._send_execute(addr, tid, t.version)

        for sid, fired_at in sorted([(i, at) for i, at in self.s_timeouts.items()
                                     if now >= at]):
            s = self.schedules.get(sid)
            if s is None:
                continue
            current = fired_at
            while current <= now:
                tags = dict(s.promise_tags)
                promise_id = (s.promise_id.replace("{{.id}}", sid)
                              .replace("{{.timestamp}}", str(current)))
                tags["resonate:schedule"] = sid
                for k in ("resonate:origin", "resonate:branch",
                          "resonate:parent", "resonate:prefix"):
                    tags[k] = promise_id
                if promise_id not in self.promises:
                    self._create_promise(promise_id, current + s.promise_timeout,
                                         s.promise_param, tags, current)
                s.last_run_at = current
                current = next_cron(s.cron, current)
            self._set_s_timeout(sid, current)

    # ─── snapshot, for the differential ──────────────────────────────────────

    def snapshot(self):
        """The same canonical shape `test/model.py` projects a database into."""
        promises = []
        for pid, p in sorted(self.promises.items()):
            r = self._promise_record(0, pid, p)
            r["paramHeaders"] = r["param"]["headers"]
            r["paramData"] = r["param"]["data"]
            r["valueHeaders"] = r["value"]["headers"]
            r["valueData"] = r["value"]["data"]
            del r["param"], r["value"]
            r["callbacks"] = sorted(p.callbacks)
            r["listeners"] = sorted(p.listeners)
            promises.append(r)
        tasks = []
        for tid, t in sorted(self.tasks.items()):
            kind_at = self.t_timeouts.get(tid)
            tasks.append({
                "id": tid, "state": t.state, "version": t.version,
                "ttl": t.ttl, "pid": t.pid,
                "expiresAt": kind_at[1] if kind_at and kind_at[0] == LEASE else None,
                "retryAt": kind_at[1] if kind_at and kind_at[0] == RETRY else None,
                "resumes": sorted(t.resumes)})
        outbox = []
        for i, (key, row) in enumerate(sorted(self.outbox.items())):
            r = dict(row)
            r["rank"] = i + 1
            outbox.append(r)
        # schedules in the shape `model.check` reads (snake_case, as the
        # database projects them), not the wire shape
        schedules = [{"id": i, "cron": s.cron, "promise_id": s.promise_id,
                      "promise_tags": dict(s.promise_tags),
                      "created_at": s.created_at,
                      "next_run_at": self.s_timeouts.get(i, 0),
                      "last_run_at": s.last_run_at}
                     for i, s in sorted(self.schedules.items())]
        return {"promises": promises, "tasks": tasks,
                "schedules": schedules, "outbox": outbox}

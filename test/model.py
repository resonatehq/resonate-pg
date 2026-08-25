"""Canonical state snapshot + the specification's `.state` catalogue.

Both schemas are projected onto the abstract machine's `ServerState`
(resonatehq/resonate-specification, spec/02-abstract/state.lean) so that a
two-table store and a single-table store are compared as the same object, and
so that the 48 `.state` properties can be evaluated against either one.

The projection is the only place that knows which schema it is reading.
"""

# --------------------------------------------------------------------------
# projection
# --------------------------------------------------------------------------

SNAP_TWO = """
SELECT jsonb_build_object(
  'promises', (SELECT COALESCE(jsonb_agg(jsonb_build_object(
      'id', p.id, 'state', p.state,
      'paramHeaders', p.param_headers, 'paramData', p.param_data,
      'valueHeaders', p.value_headers, 'valueData', p.value_data,
      'tags', p.tags, 'timeoutAt', p.timeout_at,
      'createdAt', p.created_at, 'settledAt', p.settled_at,
      'callbacks', (SELECT COALESCE(jsonb_agg(c.awaiter_id ORDER BY c.awaiter_id), '[]'::jsonb)
                    FROM resonate.callbacks c WHERE c.awaited_id = p.id),
      'listeners', (SELECT COALESCE(jsonb_agg(l.address ORDER BY l.address), '[]'::jsonb)
                    FROM resonate.listeners l WHERE l.awaited_id = p.id)
    ) ORDER BY p.id), '[]'::jsonb) FROM resonate.promises p),
  'tasks', (SELECT COALESCE(jsonb_agg(jsonb_build_object(
      'id', t.id, 'state', t.state, 'version', t.version,
      'ttl', t.ttl, 'pid', t.pid,
      'expiresAt', CASE WHEN t.state = 'acquired' THEN t.timeout_at END,
      'retryAt',   CASE WHEN t.state = 'pending'  THEN t.timeout_at END,
      'resumes', (SELECT COALESCE(jsonb_agg(r.awaited_id ORDER BY r.awaited_id), '[]'::jsonb)
                  FROM resonate.task_resumes r WHERE r.task_id = t.id)
    ) ORDER BY t.id), '[]'::jsonb) FROM resonate.tasks t),
  'schedules', (SELECT COALESCE(jsonb_agg(to_jsonb(s) ORDER BY s.id), '[]'::jsonb)
                FROM resonate.schedules s),
  'outbox', (SELECT COALESCE(jsonb_agg(jsonb_build_object(
      'key', o.key, 'kind', o.kind, 'address', o.address,
      'taskId', o.task_id, 'version', o.version, 'promise', o.promise,
      'rank', o.rnk) ORDER BY o.key), '[]'::jsonb)
    FROM (SELECT x.*, rank() OVER (ORDER BY x.seq) AS rnk
          FROM resonate.outbox x) o))
"""

SNAP_ONE = """
SELECT jsonb_build_object(
  'promises', (SELECT COALESCE(jsonb_agg(jsonb_build_object(
      'id', p.id, 'state', p.state,
      'paramHeaders', p.param_headers, 'paramData', p.param_data,
      'valueHeaders', p.value_headers, 'valueData', p.value_data,
      'tags', p.tags, 'timeoutAt', p.timeout_at,
      'createdAt', p.created_at, 'settledAt', p.settled_at,
      'callbacks', (SELECT COALESCE(jsonb_agg(a ORDER BY a), '[]'::jsonb)
                    FROM unnest(p.awaiters) AS a),
      'listeners', (SELECT COALESCE(jsonb_agg(a ORDER BY a), '[]'::jsonb)
                    FROM unnest(p.listeners) AS a)
    ) ORDER BY p.id), '[]'::jsonb) FROM resonate.promises p),
  'tasks', (SELECT COALESCE(jsonb_agg(jsonb_build_object(
      'id', t.id, 'state', t.task_state, 'version', t.task_version,
      'ttl', t.ttl, 'pid', t.pid,
      'expiresAt', t.expires_at, 'retryAt', t.retry_at,
      'resumes', (SELECT COALESCE(jsonb_agg(a ORDER BY a), '[]'::jsonb)
                  FROM unnest(t.resumes) AS a)
    ) ORDER BY t.id), '[]'::jsonb)
    FROM resonate.promises t WHERE t.task_state IS NOT NULL),
  'schedules', (SELECT COALESCE(jsonb_agg(to_jsonb(s) ORDER BY s.id), '[]'::jsonb)
                FROM resonate.schedules s),
  'outbox', (SELECT COALESCE(jsonb_agg(jsonb_build_object(
      'key', o.key, 'kind', o.kind, 'address', o.address,
      'taskId', o.task_id, 'version', o.version, 'promise', o.promise,
      'rank', o.rnk) ORDER BY o.key), '[]'::jsonb)
    FROM (SELECT x.*, rank() OVER (ORDER BY x.seq) AS rnk
          FROM resonate.outbox x) o))
"""


def snapshot(conn, layout):
    sql = SNAP_TWO if layout == "two" else SNAP_ONE
    return conn.execute(sql).fetchone()[0]


def normalize(s):
    """Erase the one documented divergence between the two layouts.

    The two-table `_cascade_settle` eagerly deletes the settling promise's OWN
    pending registrations (it has `idx_callbacks_awaiter_id` for the reverse
    lookup). The array layout has no reverse index and drops that sweep: a
    settled awaiter is left as a tombstone, which `_enqueue_resume` ignores
    because a fulfilled task matches none of its branches. Canonicalising away
    awaiter ids whose own promise is settled makes the comparison about
    behaviour rather than about that sweep.
    """
    settled = {p["id"] for p in s["promises"] if p["state"] != "pending"}
    for p in s["promises"]:
        p["callbacks"] = [a for a in p["callbacks"] if a not in settled]
    return s


# --------------------------------------------------------------------------
# the catalogue — the 48 `.state` entries, transcribed
# --------------------------------------------------------------------------

def _timer(p):
    return p["tags"].get("resonate:timer") == "true"


def _targeted(p):
    return "resonate:target" in p["tags"]


def _external(p):
    return (p["tags"].get("resonate:external") == "true"
            or _targeted(p) or _timer(p))


def _addr_valid(a):
    return (a.startswith("http://") or a.startswith("https://")
            or (a.startswith("poll://") and "@" in a))


def _parse_nat(s):
    acc = 0
    for c in s:
        acc = acc * 10 + max(ord(c) - 48, 0)
    return acc


def _project(p, now):
    if p["state"] == "pending" and p["timeoutAt"] <= now:
        q = dict(p)
        q["state"] = "resolved" if _timer(p) else "rejected_timedout"
        q["settledAt"] = p["timeoutAt"]
        return q
    return p


def _uniq(xs):
    return len(set(xs)) == len(xs)


# The three constraints the specification records as KNOWN GAPS: true of the
# protocol, false of the machine (a reachable state violates each), so they are
# deliberately outside `catalogue`. The spec asks implementations to enforce
# them at their doors anyway. Enforcing them CHANGES BEHAVIOUR, so they are
# reported separately and are only switched on by constraints-gaps.sql.
GAPS = {
    "well_formed_task_ttl_positive",
    "well_formed_promise_target_is_nonempty",
    "well_formed_promise_delay_before_deadline",
}


def check(now, s):
    """Return the names of every `.state` property that fails on `s`."""
    P, T, S, O = s["promises"], s["tasks"], s["schedules"], s["outbox"]
    byid = {p["id"]: p for p in P}
    tbyid = {t["id"]: t for t in T}
    bad = []

    def w(name, ok):
        if not ok:
            bad.append(name)

    # --- promises ---------------------------------------------------------
    w("well_formed_promise_created_at_lte_timeout_at",
      all(p["createdAt"] <= p["timeoutAt"] for p in P))
    w("well_formed_promise_pending_created_before_deadline",
      all(p["state"] != "pending" or p["createdAt"] < p["timeoutAt"] for p in P))
    w("well_formed_promise_settled_at_lte_timeout_at",
      all(p["settledAt"] is None or p["settledAt"] <= p["timeoutAt"] for p in P))
    w("well_formed_promise_created_at_lte_settled_at",
      all(p["settledAt"] is None or p["createdAt"] <= p["settledAt"] for p in P))
    w("well_formed_promise_settled_at_iff_not_pending",
      all((p["state"] != "pending") == (p["settledAt"] is not None) for p in P))
    w("well_formed_promise_pending_has_no_value",
      all(p["state"] != "pending"
          or (p["valueData"] is None and not p["valueHeaders"]) for p in P))
    w("well_formed_promise_deadline_verdict_matches_timer_tag",
      all(p["settledAt"] != p["timeoutAt"]
          or p["state"] == ("resolved" if _timer(p) else "rejected_timedout") for p in P))
    w("well_formed_promise_deadline_settlement_has_no_value",
      all(p["settledAt"] != p["timeoutAt"]
          or (p["valueData"] is None and not p["valueHeaders"]) for p in P))
    w("well_formed_promise_timer_not_targeted",
      all(not (_timer(p) and _targeted(p)) for p in P))
    w("well_formed_promise_timedout_is_server_owned",
      all(p["state"] != "rejected_timedout" or p["settledAt"] == p["timeoutAt"] for p in P))
    w("well_formed_promise_callbacks_unique", all(_uniq(p["callbacks"]) for p in P))
    w("well_formed_promise_listeners_unique", all(_uniq(p["listeners"]) for p in P))
    w("well_formed_promise_obligations_require_external",
      all((not p["callbacks"] and not p["listeners"]) or _external(p) for p in P))
    w("well_formed_promise_awaiter_is_not_self",
      all(p["id"] not in p["callbacks"] for p in P))
    w("well_formed_promise_created_at_lte_now", all(p["createdAt"] <= now for p in P))
    w("well_formed_promise_settled_at_lte_now",
      all(p["settledAt"] is None or p["settledAt"] <= now for p in P))

    # --- tasks ------------------------------------------------------------
    w("well_formed_task_acquired_iff_has_pid",
      all((t["state"] == "acquired") == (t["pid"] is not None) for t in T))
    w("well_formed_task_acquired_iff_has_ttl",
      all((t["state"] == "acquired") == (t["ttl"] is not None) for t in T))
    w("well_formed_task_acquired_iff_has_expires_at",
      all((t["state"] == "acquired") == (t["expiresAt"] is not None) for t in T))
    w("well_formed_task_pending_iff_has_retry_at",
      all((t["state"] == "pending") == (t["retryAt"] is not None) for t in T))
    w("well_formed_task_fulfilled_is_cleared",
      all(t["state"] != "fulfilled"
          or (t["pid"] is None and t["ttl"] is None and t["expiresAt"] is None
              and t["retryAt"] is None and not t["resumes"]) for t in T))
    w("well_formed_task_suspended_is_cleared",
      all(t["state"] != "suspended"
          or (t["pid"] is None and t["ttl"] is None and t["expiresAt"] is None
              and t["retryAt"] is None) for t in T))
    w("well_formed_task_halted_is_cleared",
      all(t["state"] != "halted"
          or (t["pid"] is None and t["ttl"] is None and t["expiresAt"] is None
              and t["retryAt"] is None) for t in T))
    w("well_formed_task_suspended_has_no_resumes",
      all(t["state"] != "suspended" or not t["resumes"] for t in T))
    w("well_formed_task_resumes_unique", all(_uniq(t["resumes"]) for t in T))
    w("well_formed_task_acquired_version_positive",
      all(t["state"] != "acquired" or t["version"] >= 1 for t in T))
    w("well_formed_task_ttl_positive",
      all(t["state"] != "acquired" or (t["ttl"] or 0) > 0 for t in T))

    # --- schedules --------------------------------------------------------
    w("well_formed_schedule_promise_tags_not_timer_targeted",
      all(not (c["promise_tags"].get("resonate:timer") == "true"
               and "resonate:target" in c["promise_tags"]) for c in S))
    w("well_formed_schedule_created_at_lte_next_run_at",
      all(c["created_at"] <= c["next_run_at"] for c in S))
    w("well_formed_schedule_created_at_lte_last_run_at",
      all(c["last_run_at"] is None or c["created_at"] <= c["last_run_at"] for c in S))
    w("well_formed_schedule_last_run_at_lt_next_run_at",
      all(c["last_run_at"] is None or c["last_run_at"] < c["next_run_at"] for c in S))

    # --- store ------------------------------------------------------------
    w("well_formed_store_promise_ids_unique", _uniq([p["id"] for p in P]))
    w("well_formed_store_task_ids_unique", _uniq([t["id"] for t in T]))
    w("well_formed_store_schedule_ids_unique", _uniq([c["id"] for c in S]))
    w("well_formed_store_outbox_keys_unique", _uniq([o["key"] for o in O]))

    # --- cross-object -----------------------------------------------------
    w("consistent_task_iff_targeted_promise",
      all(t["id"] in byid and _targeted(byid[t["id"]]) for t in T)
      and all(not _targeted(p) or p["id"] in tbyid for p in P))
    w("consistent_settled_promise_has_fulfilled_task",
      all(p["state"] == "pending"
          or p["id"] not in tbyid or tbyid[p["id"]]["state"] == "fulfilled" for p in P))
    w("consistent_settled_task_promise_settled",
      all(t["state"] != "fulfilled"
          or t["id"] not in byid or byid[t["id"]]["state"] != "pending" for t in T))
    w("consistent_callback_awaiter_is_targeted",
      all(a in byid and _targeted(byid[a]) for p in P for a in p["callbacks"]))
    w("consistent_listener_addresses_deliverable",
      all(_addr_valid(a) for p in P for a in p["listeners"]))
    w("consistent_suspended_task_holds_rung",
      all(t["state"] != "suspended"
          or t["id"] not in byid
          or _project(byid[t["id"]], now)["state"] != "pending"
          or any(t["id"] in q["callbacks"] for q in P) for t in T))
    w("well_formed_promise_target_is_nonempty",
      all(not _targeted(p) or p["tags"]["resonate:target"] != "" for p in P))
    w("well_formed_promise_delay_before_deadline",
      all("resonate:delay" not in p["tags"]
          or _parse_nat(p["tags"]["resonate:delay"]) < p["timeoutAt"] for p in P))

    # --- outbox -----------------------------------------------------------
    ex = [o for o in O if o["kind"] == "execute"]
    ub = [o for o in O if o["kind"] == "unblock"]
    w("consistent_outbox_execute_names_existing_task",
      all(o["taskId"] in tbyid for o in ex))
    w("consistent_outbox_never_ahead",
      all(o["taskId"] not in tbyid or o["version"] <= tbyid[o["taskId"]]["version"]
          for o in ex))
    w("consistent_outbox_execute_address_is_target_tag",
      all(o["taskId"] not in byid
          or o["address"] == (byid[o["taskId"]]["tags"].get("resonate:target") or "")
          for o in ex))
    w("consistent_outbox_unblock_names_settled_promise",
      all(o["promise"]["state"] != "pending"
          and o["promise"]["id"] in byid
          and byid[o["promise"]["id"]]["state"] != "pending" for o in ub))
    w("consistent_outbox_unblock_address_deliverable",
      all(_addr_valid(o["address"]) for o in ub))

    return bad


COUNT = 45          # `.state` entries in `catalogue`
GAP_COUNT = 3       # `.state` entries in `gaps`

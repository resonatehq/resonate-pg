#!/usr/bin/env python3
"""Insert Specula trace emit calls into resonate.sql.

Every insertion point sits inside a real handler body, after the last statement
that mutates state, so the emitted snapshot is that action's post-state. The
emit runs in the handler's own transaction. No protocol logic is reimplemented
or duplicated anywhere.

Each anchor below must occur exactly once in resonate.sql and carries a
`<<EMIT>>` marker on its own line, which is replaced by the emit call.

Usage:  instrument.py <resonate.sql> <output.sql>
"""
import sys

MARKER = "<<EMIT>>"

POINTS = [
    # ---- P-02 promise.create, the two branches that write state -------------
    ("""      INSERT INTO tasks (id, state, version, timeout_at) VALUES (p.id, 'pending', 0, delay);
    END IF;
<<EMIT>>
    RETURN jsonb_build_object('status', 200, 'promise', _promise_json(p, p_now));""",
     "PromiseCreate"),

    ("""      INSERT INTO tasks (id, state, version) VALUES (p.id, 'fulfilled', 0);
    END IF;
<<EMIT>>
    RETURN jsonb_build_object('status', 200, 'promise', _promise_json(p, p_now));""",
     "PromiseCreate"),

    # ---- P-03 promise.settle -------------------------------------------------
    ("""     RETURNING * INTO p;
    PERFORM _cascade_settle(p, p_now);
<<EMIT>>
  END IF;""",
     "PromiseSettle"),

    # ---- P-04 promise.register_callback --------------------------------------
    ("""        INSERT INTO callbacks (awaited_id, awaiter_id) VALUES (p_awaited, p_awaiter)
          ON CONFLICT DO NOTHING;
<<EMIT>>
      END IF;""",
     "RegisterCallback"),

    # ---- P-05 promise.register_listener --------------------------------------
    ("""    INSERT INTO listeners (awaited_id, address) VALUES (p_awaited, p_address)
      ON CONFLICT DO NOTHING;
<<EMIT>>
  END IF;""",
     "RegisterListener"),

    # ---- T-02 task.create, claim branch ---------------------------------------
    ("""      DELETE FROM task_resumes WHERE task_id = t.id;
      UPDATE tasks SET state = 'acquired', version = version + 1, ttl = p_ttl, pid = p_pid,
                       timeout_at = p_now + p_ttl WHERE id = t.id RETURNING * INTO t;
<<EMIT>>
    ELSE""",
     "TaskClaim"),

    # ---- T-03 task.acquire -----------------------------------------------------
    ("""  UPDATE tasks SET state = 'acquired', version = version + 1, ttl = p_ttl, pid = p_pid,
                   timeout_at = p_now + p_ttl
    WHERE id = t.id RETURNING * INTO t;
<<EMIT>>
  RETURN jsonb_build_object('status', 200, 'task', _task_json(t),""",
     "TaskAcquire"),

    # ---- T-06 task.suspend, the 300 path and the 200 path ----------------------
    ("""  IF settled THEN
    DELETE FROM task_resumes WHERE task_id = t.id;
<<EMIT>>
    RETURN jsonb_build_object('status', 300);""",
     "TaskSuspend300"),

    ("""  UPDATE tasks SET state = 'suspended', pid = NULL, ttl = NULL WHERE id = t.id;
<<EMIT>>
  RETURN jsonb_build_object('status', 200);""",
     "TaskSuspend"),

    # ---- T-07 task.fulfill ------------------------------------------------------
    ("""  PERFORM _cascade_settle(p, p_now);
<<EMIT>>
  RETURN jsonb_build_object('status', 200, 'promise', _promise_json_raw(p));""",
     "TaskFulfill"),

    # ---- T-08 task.release ------------------------------------------------------
    ("""  IF p.target IS NOT NULL AND p.target <> '' THEN
    PERFORM _emit_execute(p.target, t.id, t.version);
  END IF;
<<EMIT>>
  RETURN jsonb_build_object('status', 200);
END $$;

CREATE OR REPLACE FUNCTION task_halt(""",
     "TaskRelease"),

    # ---- T-09 task.halt ----------------------------------------------------------
    ("""  UPDATE tasks SET state = 'halted', pid = NULL, ttl = NULL WHERE id = t.id;
<<EMIT>>
  RETURN jsonb_build_object('status', 200);""",
     "TaskHalt"),

    # ---- T-10 task.continue ------------------------------------------------------
    ("""  IF p.target IS NOT NULL AND p.target <> '' THEN
    PERFORM _emit_execute(p.target, t.id, t.version);
  END IF;
<<EMIT>>
  RETURN jsonb_build_object('status', 200);
END $$;

CREATE OR REPLACE FUNCTION task_search(""",
     "TaskContinue"),

    # ---- internal transitions ------------------------------------------------------
    ("""  UPDATE promises SET state = st, settled_at = p.timeout_at WHERE id = p.id RETURNING * INTO p;
  PERFORM _cascade_settle(p, p_now);
<<EMIT>>
END $$;""",
     "OnPromiseTimeout"),

    ("""  IF p.target IS NOT NULL AND p.target <> '' THEN
    PERFORM _emit_execute(p.target, t.id, t.version);
  END IF;
<<EMIT>>
END $$;

CREATE OR REPLACE FUNCTION _on_task_lease_timeout(""",
     "OnTaskRetryTimeout"),

    ("""  IF p.target IS NOT NULL AND p.target <> '' THEN
    PERFORM _emit_execute(p.target, t.id, t.version);
  END IF;
<<EMIT>>
END $$;

CREATE OR REPLACE FUNCTION _on_schedule_timeout(""",
     "OnTaskLeaseTimeout"),
]


def main(src_path: str, out_path: str) -> int:
    src = open(src_path).read()

    for anchor, event in POINTS:
        assert MARKER in anchor, f"anchor for {event} has no {MARKER}"
        bare = anchor.replace(MARKER + "\n", "")
        if src.count(bare) != 1:
            print(f"anchor for {event} matched {src.count(bare)} times, expected 1",
                  file=sys.stderr)
            return 1
        # indent the emit to match the line that follows the marker
        after = anchor.split(MARKER + "\n", 1)[1].split("\n", 1)[0]
        indent = " " * (len(after) - len(after.lstrip()))
        filled = anchor.replace(MARKER, f"{indent}PERFORM _trace_emit('{event}', p_now);")
        src = src.replace(bare, filled, 1)

    open(out_path, "w").write(src)
    print(f"instrumented {len(POINTS)} points -> {out_path}")
    return 0


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print(__doc__, file=sys.stderr)
        sys.exit(2)
    sys.exit(main(sys.argv[1], sys.argv[2]))

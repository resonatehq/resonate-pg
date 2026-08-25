"""Which catalogue properties each layout can state as a table constraint.

The `.state` half of the specification's catalogue is "a bad row or a bad join".
A CHECK constraint rejects exactly that, provided the join it needs is inside
one row — so the question "how many properties can the schema enforce?" is a
question about how the state is laid out, and it is the sharpest available
measure of the difference between the two designs.

The single-table column is verified against the live database: a property
counts as enforced only if a constraint of that exact name exists. The
two-table column is a hand classification with a recorded reason, because the
two-table layout has no such constraints to read.
"""
import json, sys
import psycopg

# name -> (two_table_verdict, note)
#   yes     — statable on the two-table layout as CHECK/PK/FK
#   no      — not statable; the note says what blocks it
#   partial — only part of the entry is statable
TWO = {
    "well_formed_promise_created_at_lte_timeout_at": ("yes", ""),
    "well_formed_promise_pending_created_before_deadline": ("yes", ""),
    "well_formed_promise_settled_at_lte_timeout_at": ("yes", ""),
    "well_formed_promise_created_at_lte_settled_at": ("yes", ""),
    "well_formed_promise_settled_at_iff_not_pending": ("yes", ""),
    "well_formed_promise_pending_has_no_value": ("yes", ""),
    "well_formed_promise_deadline_verdict_matches_timer_tag": ("yes", ""),
    "well_formed_promise_deadline_settlement_has_no_value": ("yes", ""),
    "well_formed_promise_timer_not_targeted": ("yes", ""),
    "well_formed_promise_timedout_is_server_owned": ("yes", ""),
    "well_formed_promise_callbacks_unique": ("yes", "the callbacks primary key"),
    "well_formed_promise_listeners_unique": ("yes", "the listeners primary key"),
    "well_formed_promise_obligations_require_external":
        ("no", "joins callbacks/listeners to promises.external; a foreign key "
               "cannot reference a partial unique index"),
    "well_formed_promise_awaiter_is_not_self": ("yes", "intra-row on callbacks"),
    "well_formed_promise_created_at_lte_now":
        ("no", "clock-relative; `now` is a request parameter, not a column"),
    "well_formed_promise_settled_at_lte_now": ("no", "clock-relative"),
    "well_formed_task_acquired_iff_has_pid": ("yes", ""),
    "well_formed_task_acquired_iff_has_ttl": ("yes", ""),
    "well_formed_task_acquired_iff_has_expires_at":
        ("no", "one `timeout_at` column serves as both retry and lease instant, "
               "so the two cannot be quantified over separately"),
    "well_formed_task_pending_iff_has_retry_at":
        ("no", "same single `timeout_at` column"),
    "well_formed_task_fulfilled_is_cleared":
        ("no", "the `resumes` clause joins tasks to task_resumes"),
    "well_formed_task_suspended_is_cleared":
        ("no", "needs retry_at and expires_at separately"),
    "well_formed_task_halted_is_cleared":
        ("no", "needs retry_at and expires_at separately"),
    "well_formed_task_suspended_has_no_resumes":
        ("no", "joins tasks to task_resumes"),
    "well_formed_task_resumes_unique": ("yes", "the task_resumes primary key"),
    "well_formed_task_acquired_version_positive": ("yes", ""),
    "well_formed_schedule_promise_tags_not_timer_targeted": ("yes", ""),
    "well_formed_schedule_created_at_lte_next_run_at": ("yes", ""),
    "well_formed_schedule_created_at_lte_last_run_at": ("yes", ""),
    "well_formed_schedule_last_run_at_lt_next_run_at": ("yes", ""),
    "well_formed_store_promise_ids_unique": ("yes", "primary key"),
    "well_formed_store_task_ids_unique": ("yes", "primary key"),
    "well_formed_store_schedule_ids_unique": ("yes", "primary key"),
    "well_formed_store_outbox_keys_unique": ("yes", "primary key"),
    "consistent_task_iff_targeted_promise":
        ("no", "a biconditional between two tables: no CHECK sees both, and a "
               "foreign key states only one direction and only on existence"),
    "consistent_settled_promise_has_fulfilled_task":
        ("no", "relates promises.state to tasks.state across tables"),
    "consistent_callback_awaiter_is_targeted":
        ("no", "cross-ROW: the awaiter is a different promise. Merging does not "
               "help — Postgres has no array-element foreign key"),
    "consistent_listener_addresses_deliverable": ("yes", "intra-row on listeners"),
    "consistent_outbox_execute_names_existing_task":
        ("yes", "outbox.task_id REFERENCES tasks(id) — the one entry the "
                "two-table layout states BETTER, since merged there is no "
                "task-only key to point a foreign key at"),
    "consistent_outbox_never_ahead":
        ("no", "compares an outbox row to a task row"),
    "consistent_outbox_execute_address_is_target_tag":
        ("no", "compares an outbox row to a promise row"),
    "consistent_outbox_unblock_names_settled_promise":
        ("partial", "the shape half is intra-row; 'and the stored promise is "
                    "settled' is cross-row"),
    "consistent_outbox_unblock_address_deliverable": ("yes", ""),
    "consistent_settled_task_promise_settled":
        ("no", "relates tasks.state to promises.state across tables"),
    "consistent_suspended_task_holds_rung":
        ("no", "cross-row and clock-relative"),
}

# Entries the single-table layout enforces under a constraint whose name is not
# the property name (a primary key cannot be renamed without renaming the key).
ALIAS_ONE = {
    "well_formed_store_promise_ids_unique": "promises_pkey",
    "well_formed_store_task_ids_unique": "promises_pkey",
    "well_formed_store_schedule_ids_unique": "schedules_pkey",
    "well_formed_store_outbox_keys_unique": "outbox_pkey",
}

PARTIAL_ONE = {
    # stated in constraints.sql, but only the intra-row half of the entry
    "consistent_outbox_unblock_names_settled_promise",
}


def main(dsn):
    with psycopg.connect(dsn, autocommit=True) as c:
        live = {r[0] for r in c.execute("""
            SELECT c.conname FROM pg_constraint c
            JOIN pg_class t ON t.oid = c.conrelid
            JOIN pg_namespace n ON n.oid = t.relnamespace
            WHERE n.nspname = 'resonate'""").fetchall()}

    rows, tally = [], {"one": 0, "two": 0, "one_partial": 0, "two_partial": 0}
    for name, (two, note) in TWO.items():
        one = "yes" if (name in live or ALIAS_ONE.get(name) in live) else "no"
        if name in PARTIAL_ONE and one == "yes":
            one = "partial"
        rows.append((name, one, two, note))
        if one == "yes":
            tally["one"] += 1
        elif one == "partial":
            tally["one_partial"] += 1
        if two == "yes":
            tally["two"] += 1
        elif two == "partial":
            tally["two_partial"] += 1

    w = max(len(r[0]) for r in rows)
    print(f"{'property':<{w}}  single  two   note")
    print("-" * (w + 40))
    for name, one, two, note in sorted(rows, key=lambda r: (r[1] != "yes", r[0])):
        print(f"{name:<{w}}  {one:<6}  {two:<5} {note}")
    print()
    print(f"catalogue .state entries: {len(rows)}")
    print(f"  single-table: {tally['one']} full + {tally['one_partial']} partial")
    print(f"  two-table:    {tally['two']} full + {tally['two_partial']} partial")
    only = [n for n, o, t, _ in rows if o == "yes" and t == "no"]
    print(f"  only the merged layout can state: {len(only)}")
    for n in only:
        print(f"    - {n}")
    lost = [n for n, o, t, _ in rows if t == "yes" and o != "yes"]
    print(f"  only the split layout can state: {len(lost)}")
    for n in lost:
        print(f"    - {n}")
    return rows, tally


if __name__ == "__main__":
    main(sys.argv[1] if len(sys.argv) > 1
         else "host=/tmp port=5433 user=postgres dbname=res_one")

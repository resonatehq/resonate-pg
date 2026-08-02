-------------------------------- MODULE base --------------------------------
(***************************************************************************)
(* A TLA+ model of the resonate-pg server (resonate.sql).                   *)
(*                                                                          *)
(* Every action below is one Postgres transaction.  resonate.sql takes a    *)
(* pg_advisory_xact_lock on every id it touches (resonate.sql:381), and     *)
(* every row read is SELECT ... FOR UPDATE, so transactions that touch      *)
(* overlapping ids are serialized.  Modelling each RPC as an atomic TLA+    *)
(* action is therefore faithful; interleaving happens *between* RPCs,       *)
(* which is exactly what this model explores.                               *)
(*                                                                          *)
(* Wall-clock `now` is passed explicitly into every action by the caller    *)
(* (resonate.sql:1063), and the pg_cron driver passes its own `now`         *)
(* (resonate.sql:1014).  `now` is modelled as a monotone counter that any   *)
(* action may observe, and Tick advances it.                                *)
(*                                                                          *)
(* Source line references are to resonate.sql at commit 54fe651.            *)
(***************************************************************************)
EXTENDS Naturals, FiniteSets

CONSTANTS
    Ids,          \* promise/task ids
    Addrs,        \* listener addresses
    MaxTime,      \* time horizon
    MaxVersion,   \* bound on task.version, to keep the state space finite
    Retry,        \* _retry_timeout(), resonate.sql:175
    Ttl,          \* lease ttl handed to task.acquire / task.create

    (***********************************************************************)
    (* Switches that select between resonate.sql as shipped and the guards  *)
    (* the reference specification (resonatehq/resonate-specification,      *)
    (* branch claude/close-the-square) requires.  Setting all three to TRUE *)
    (* models the fixed server; MC.cfg / MC_fixed.cfg run both.             *)
    (***********************************************************************)
    ListenerExternalGuard,  \* P-05 refuses an internal awaited with 422
    PromiseLivenessGuard,   \* T-02 claim / T-09 halt / T-10 continue gate on
                            \* p.state = pending AND p.timeout_at > now
    TimeoutLivenessGuard,   \* the two task-timeout handlers gate on the same
                            \* predicate (spec 3e8a1d6); kept separate so the
                            \* model can isolate that site from T-02/09/10
    SequencedDriver         \* TRUE models process_timeouts (:997-1012), which
                            \* runs the promise loop to a fixpoint before the
                            \* task loop; FALSE models the task-timeout
                            \* handlers reached on their own, e.g. via a bare
                            \* process_task_timeouts() call (:973-985)

NoAddr == "-"
ASSUME NoAddr \notin Addrs

ClientSettable == {"resolved", "rejected", "rejected_canceled"}

\* resonate:target | resonate:timer | resonate:external | (none)
Kinds == {"target", "timer", "ext", "plain"}

VARIABLES
    now,          \* wall clock
    promises,     \* resonate.promises
    tasks,        \* resonate.tasks
    callbacks,    \* resonate.callbacks : set of <<awaited, awaiter>>
    listeners,    \* resonate.listeners : set of <<awaited, address>>
    resumes,      \* resonate.task_resumes : set of <<task, awaited>>
    outbox,       \* resonate.outbox
    obs,          \* history: first non-pending state each promise was ever observed in
    badDispatch,  \* history: ids handed an execution lease / dispatched while
                  \* already observably dead
    badHalt       \* history: ids whose task was halted while task.get would
                  \* already have reported that task 'fulfilled'

vars == <<now, promises, tasks, callbacks, listeners, resumes, outbox,
          obs, badDispatch, badHalt>>

core == <<promises, tasks, callbacks, listeners, resumes, outbox>>

NoPromise == [exists    |-> FALSE, state    |-> "pending", timeoutAt |-> 0,
              external  |-> FALSE, isTimer  |-> FALSE,     hasTarget |-> FALSE]

NoTask == [exists |-> FALSE, state |-> "pending", version |-> 0, timeoutAt |-> 0]

ExecMsg(i, v)    == [kind |-> "execute", id |-> i, version |-> v, addr |-> NoAddr]
UnblockMsg(i, a) == [kind |-> "unblock", id |-> i, version |-> 0, addr |-> a]

(***************************************************************************)
(* _promise_json's projection, resonate.sql:179-196.  A pending promise     *)
(* whose timeout_at has passed is *reported* as settled even though the row *)
(* still says 'pending'.                                                    *)
(***************************************************************************)
ProjOf(pm, t, i) ==
    IF ~pm[i].exists THEN "none"
    ELSE IF pm[i].state = "pending" /\ pm[i].timeoutAt <= t
         THEN IF pm[i].isTimer THEN "resolved" ELSE "rejected_timedout"
         ELSE pm[i].state

Proj(i) == ProjOf(promises, now, i)

\* _emit_execute, resonate.sql:236-243: ON CONFLICT (key) DO UPDATE, keyed by task id.
PutExec(ob, i, v) ==
    {m \in ob : ~(m.kind = "execute" /\ m.id = i)} \cup {ExecMsg(i, v)}

PutExecs(ob, S) ==
    {m \in ob : ~(m.kind = "execute" /\ m.id \in S)}
      \cup {ExecMsg(i, tasks[i].version) : i \in S}

-----------------------------------------------------------------------------
(***************************************************************************)
(* _cascade_settle, resonate.sql:269-298.                                   *)
(***************************************************************************)

Awaiters(i)      == {j \in Ids : <<i, j>> \in callbacks}
SuspAwaiters(i)  == {j \in Awaiters(i) : tasks[j].exists /\ tasks[j].state = "suspended"}
OtherAwaiters(i) == {j \in Awaiters(i) : tasks[j].exists
                                      /\ tasks[j].state \in {"pending", "acquired", "halted"}}
LAddrs(i)        == {a \in Addrs : <<i, a>> \in listeners}

\* resonate.sql:273 (settling promise's own task -> fulfilled) and
\* _enqueue_resume resonate.sql:245-266 (suspended awaiter -> pending).
CascadeTasks(i) ==
    [j \in Ids |->
        IF j = i /\ tasks[j].exists
          THEN [tasks[j] EXCEPT !.state = "fulfilled"]
        ELSE IF j \in SuspAwaiters(i)
          THEN [tasks[j] EXCEPT !.state = "pending", !.timeoutAt = now + Retry]
        ELSE tasks[j]]

\* resonate.sql:274 + 255-256 + 263-264
CascadeResumes(i) ==
    {r \in resumes : r[1] # i /\ r[1] \notin SuspAwaiters(i)}
      \cup {<<j, i>> : j \in (SuspAwaiters(i) \cup OtherAwaiters(i))}

\* resonate.sql:286-288 (drop the settling promise's own registrations) and
\* resonate.sql:296 (drop registrations on the settling promise).
CascadeCallbacks(i) ==
    {c \in callbacks : c[1] # i /\ ~(c[2] = i /\ promises[c[1]].state = "pending")}

\* resonate.sql:297
CascadeListeners(i) == {l \in listeners : l[1] # i}

\* resonate.sql:275 (drop own execute), 277-284 (unblock per listener),
\* 260 (execute per resumed suspended awaiter that has a target).
CascadeOutbox(i) ==
    LET dropOwn   == {m \in outbox : ~(m.kind = "execute" /\ m.id = i)}
        resumeIds == {j \in SuspAwaiters(i) : promises[j].hasTarget}
        cleaned   == {m \in dropOwn : ~(m.kind = "execute" /\ m.id \in resumeIds)}
    IN  cleaned
          \cup {UnblockMsg(i, a) : a \in LAddrs(i)}
          \cup {ExecMsg(j, tasks[j].version) : j \in resumeIds}

\* Settle promise i to st and run the cascade.  Leaves `now` and history vars alone.
DoSettle(i, st) ==
    /\ promises' = [promises EXCEPT ![i].state = st]
    /\ tasks'     = CascadeTasks(i)
    /\ resumes'   = CascadeResumes(i)
    /\ callbacks' = CascadeCallbacks(i)
    /\ listeners' = CascadeListeners(i)
    /\ outbox'    = CascadeOutbox(i)

-----------------------------------------------------------------------------
(***************************************************************************)
(* SECTION 3 - promise actions                                              *)
(***************************************************************************)

\* promise_create, resonate.sql:393-440.  P-02.
PromiseCreate(i, toat, kind) ==
    /\ ~promises[i].exists
    /\ LET ext == kind \in {"target", "timer", "ext"}
           tim == kind = "timer"
           tgt == kind = "target"
           rec == [exists |-> TRUE, timeoutAt |-> toat, external |-> ext,
                   isTimer |-> tim, hasTarget |-> tgt, state |-> "pending"]
       IN IF toat > now
          THEN /\ promises' = [promises EXCEPT ![i] = rec]
               /\ IF tgt
                  THEN \* resonate.sql:420-422
                       /\ tasks'  = [tasks EXCEPT ![i] = [exists |-> TRUE, state |-> "pending",
                                                          version |-> 0, timeoutAt |-> now + Retry]]
                       /\ outbox' = PutExec(outbox, i, 0)
                  ELSE UNCHANGED <<tasks, outbox>>
          ELSE \* resonate.sql:425-435: created already settled
               /\ promises' = [promises EXCEPT ![i] =
                     [rec EXCEPT !.state = IF tim THEN "resolved" ELSE "rejected_timedout"]]
               /\ IF tgt
                  THEN tasks' = [tasks EXCEPT ![i] = [exists |-> TRUE, state |-> "fulfilled",
                                                      version |-> 0, timeoutAt |-> 0]]
                  ELSE UNCHANGED tasks
               /\ UNCHANGED outbox
    /\ UNCHANGED <<now, callbacks, listeners, resumes, badDispatch, badHalt>>

\* promise_settle, resonate.sql:442-469.  P-03.  Only the effective branch is
\* modelled; every other branch is a pure read.
PromiseSettle(i, st) ==
    /\ promises[i].exists
    /\ promises[i].state = "pending"
    /\ promises[i].timeoutAt > now          \* resonate.sql:461
    /\ DoSettle(i, st)
    /\ UNCHANGED <<now, badDispatch, badHalt>>

\* promise_register_callback, resonate.sql:471-499.  P-04.
RegisterCallback(aw, ar) ==
    /\ aw # ar                              \* 400, resonate.sql:476
    /\ promises[aw].exists                  \* 404
    /\ promises[ar].exists                  \* 422
    /\ promises[ar].hasTarget               \* 422, resonate.sql:484
    /\ promises[aw].external                \* 422, resonate.sql:488
    /\ promises[aw].state = "pending" /\ promises[aw].timeoutAt > now
    /\ promises[ar].state = "pending" /\ promises[ar].timeoutAt > now
    /\ <<aw, ar>> \notin callbacks
    /\ callbacks' = callbacks \cup {<<aw, ar>>}
    /\ UNCHANGED <<now, promises, tasks, listeners, resumes, outbox, badDispatch, badHalt>>

\* promise_register_listener, resonate.sql:501-520.  P-05.
\* resonate.sql has no `external` guard here, unlike P-04 above.  The
\* reference spec added one -- 422, mirroring P-04 -- in
\* resonate-specification 6ddfab7 "external-only waiters everywhere".
RegisterListener(aw, ad) ==
    /\ promises[aw].exists                  \* 404
    /\ ListenerExternalGuard => promises[aw].external      \* 422 (spec only)
    /\ promises[aw].state = "pending" /\ promises[aw].timeoutAt > now
    /\ <<aw, ad>> \notin listeners
    /\ listeners' = listeners \cup {<<aw, ad>>}
    /\ UNCHANGED <<now, promises, tasks, callbacks, resumes, outbox, badDispatch, badHalt>>

-----------------------------------------------------------------------------
(***************************************************************************)
(* SECTION 4 - task actions                                                 *)
(***************************************************************************)

\* task_create on an id that already names a promise whose task is 'pending':
\* resonate.sql:580-594.  Note the guard set: unlike task_acquire it consults
\* neither the promise state nor the promise timeout.
TaskClaim(i) ==
    /\ promises[i].exists
    /\ promises[i].hasTarget
    /\ tasks[i].exists
    /\ tasks[i].state = "pending"
    /\ PromiseLivenessGuard => Proj(i) = "pending"         \* spec T-02 gate
    /\ tasks[i].version < MaxVersion
    /\ tasks'   = [tasks EXCEPT ![i].state = "acquired",
                                ![i].version = tasks[i].version + 1,
                                ![i].timeoutAt = now + Ttl]
    /\ resumes' = {r \in resumes : r[1] # i}
    /\ badDispatch' = IF Proj(i) # "pending"
                      THEN badDispatch \cup {i} ELSE badDispatch
    /\ UNCHANGED <<now, promises, callbacks, listeners, outbox, badHalt>>

\* task_acquire, resonate.sql:600-622.  T-03.
TaskAcquire(i) ==
    /\ tasks[i].exists
    /\ tasks[i].state = "pending"                        \* 409, resonate.sql:612
    /\ promises[i].exists
    /\ promises[i].state = "pending"
    /\ promises[i].timeoutAt > now                       \* 409, resonate.sql:613
    /\ tasks[i].version < MaxVersion
    /\ tasks'   = [tasks EXCEPT ![i].state = "acquired",
                                ![i].version = tasks[i].version + 1,
                                ![i].timeoutAt = now + Ttl]
    /\ resumes' = {r \in resumes : r[1] # i}             \* resonate.sql:616
    /\ UNCHANGED <<now, promises, callbacks, listeners, outbox, badDispatch, badHalt>>

\* task_suspend, resonate.sql:673-735.  T-06.
TaskSuspend(i, S) ==
    /\ S # {}                                            \* 400, resonate.sql:686
    /\ i \notin S                                        \* 400, resonate.sql:689
    /\ tasks[i].exists
    /\ tasks[i].state = "acquired"                       \* 409
    /\ promises[i].exists
    /\ promises[i].state = "pending" /\ promises[i].timeoutAt > now  \* 409
    /\ \A j \in S : promises[j].exists                   \* 422, resonate.sql:719
    /\ \A j \in S : promises[j].external                 \* 422, resonate.sql:722
    /\ IF \E j \in S : promises[j].state # "pending" \/ promises[j].timeoutAt <= now
       THEN \* 300, resonate.sql:723-726
            /\ resumes' = {r \in resumes : r[1] # i}
            /\ UNCHANGED <<tasks, callbacks>>
       ELSE \* resonate.sql:728-733
            /\ callbacks' = callbacks \cup {<<j, i>> : j \in S}
            /\ resumes'   = {r \in resumes : r[1] # i}
            /\ tasks'     = [tasks EXCEPT ![i].state = "suspended"]
    /\ UNCHANGED <<now, promises, listeners, outbox, badDispatch, badHalt>>

\* task_fulfill, resonate.sql:739-765.  T-07.
TaskFulfill(i, st) ==
    /\ tasks[i].exists
    /\ tasks[i].state = "acquired"                       \* 409
    /\ promises[i].exists
    /\ promises[i].state = "pending"
    /\ promises[i].timeoutAt > now                       \* 409, resonate.sql:759
    /\ DoSettle(i, st)
    /\ UNCHANGED <<now, badDispatch, badHalt>>

\* task_release, resonate.sql:767-787.  T-08.
TaskRelease(i) ==
    /\ tasks[i].exists
    /\ tasks[i].state = "acquired"
    /\ promises[i].exists
    /\ promises[i].state = "pending"
    /\ promises[i].timeoutAt > now                       \* 409, resonate.sql:778
    /\ tasks'  = [tasks EXCEPT ![i].state = "pending", ![i].timeoutAt = now + Retry]
    /\ outbox' = IF promises[i].hasTarget
                 THEN PutExec(outbox, i, tasks[i].version) ELSE outbox
    /\ UNCHANGED <<now, promises, callbacks, listeners, resumes, badDispatch, badHalt>>

\* task_halt, resonate.sql:789-801.  T-09.
\* resonate.sql's task_halt never loads the promise at all; the reference
\* spec's T-09 returns 409 once the promise is logically settled, because
\* task.get already reports such a task 'fulfilled' (resonate.sql:540-545)
\* and halt-on-fulfilled is 409.
TaskHalt(i) ==
    /\ tasks[i].exists
    /\ tasks[i].state \notin {"fulfilled", "halted"}
    /\ PromiseLivenessGuard => Proj(i) = "pending"         \* spec T-09 gate
    /\ tasks' = [tasks EXCEPT ![i].state = "halted"]
    /\ badHalt' = IF Proj(i) # "pending" THEN badHalt \cup {i} ELSE badHalt
    /\ UNCHANGED <<now, promises, callbacks, listeners, resumes, outbox, badDispatch>>

\* task_continue, resonate.sql:803-819.  T-10.  Like task_create and unlike
\* task_acquire, this consults neither the promise state nor its timeout.
TaskContinue(i) ==
    /\ tasks[i].exists
    /\ tasks[i].state = "halted"
    /\ promises[i].exists
    /\ PromiseLivenessGuard => Proj(i) = "pending"         \* spec T-10 gate
    /\ tasks'  = [tasks EXCEPT ![i].state = "pending", ![i].timeoutAt = now + Retry]
    /\ outbox' = IF promises[i].hasTarget
                 THEN PutExec(outbox, i, tasks[i].version) ELSE outbox
    /\ badDispatch' = IF Proj(i) # "pending"
                      THEN badDispatch \cup {i} ELSE badDispatch
    /\ UNCHANGED <<now, promises, callbacks, listeners, resumes, badHalt>>

-----------------------------------------------------------------------------
(***************************************************************************)
(* SECTION 6 - internal transitions driven by pg_cron                       *)
(***************************************************************************)

\* process_timeouts (:997-1012) runs process_promise_timeouts to a fixpoint
\* before process_task_timeouts, so within the shipped driver no task-timeout
\* handler ever runs while a promise timeout is still due.
NoPromiseTimeoutDue ==
    \A j \in Ids : ~(promises[j].exists /\ promises[j].state = "pending"
                     /\ promises[j].external /\ promises[j].timeoutAt <= now)

\* _on_promise_timeout, resonate.sql:890-902, reachable only through
\* process_promise_timeouts, whose WHERE clause is _promise_timed AND
\* timeout_at <= now (resonate.sql:965-967).  _promise_timed requires
\* `external` (resonate.sql:204-207) -- an internal promise is never selected.
OnPromiseTimeout(i) ==
    /\ promises[i].exists
    /\ promises[i].state = "pending"
    /\ promises[i].external
    /\ promises[i].timeoutAt <= now
    /\ DoSettle(i, IF promises[i].isTimer THEN "resolved" ELSE "rejected_timedout")
    /\ UNCHANGED <<now, badDispatch, badHalt>>

\* _on_task_retry_timeout, resonate.sql:904-919.
OnTaskRetryTimeout(i) ==
    /\ tasks[i].exists
    /\ tasks[i].state = "pending"
    /\ tasks[i].timeoutAt <= now
    /\ SequencedDriver => NoPromiseTimeoutDue
    /\ TimeoutLivenessGuard => Proj(i) = "pending"         \* spec gate (3e8a1d6)
    /\ tasks'  = [tasks EXCEPT ![i].timeoutAt = now + Retry]
    /\ outbox' = IF promises[i].exists /\ promises[i].hasTarget
                 THEN PutExec(outbox, i, tasks[i].version) ELSE outbox
    /\ badDispatch' = IF promises[i].exists /\ promises[i].hasTarget /\ Proj(i) # "pending"
                      THEN badDispatch \cup {i} ELSE badDispatch
    /\ UNCHANGED <<now, promises, callbacks, listeners, resumes, badHalt>>

\* _on_task_lease_timeout, resonate.sql:921-937.  Note the version is *not*
\* bumped: the fence token survives the lease.
OnTaskLeaseTimeout(i) ==
    /\ tasks[i].exists
    /\ tasks[i].state = "acquired"
    /\ tasks[i].timeoutAt <= now
    /\ SequencedDriver => NoPromiseTimeoutDue
    /\ TimeoutLivenessGuard => Proj(i) = "pending"         \* spec gate (3e8a1d6)
    /\ tasks'  = [tasks EXCEPT ![i].state = "pending", ![i].timeoutAt = now + Retry]
    /\ outbox' = IF promises[i].exists /\ promises[i].hasTarget
                 THEN PutExec(outbox, i, tasks[i].version) ELSE outbox
    /\ badDispatch' = IF promises[i].exists /\ promises[i].hasTarget /\ Proj(i) # "pending"
                      THEN badDispatch \cup {i} ELSE badDispatch
    /\ UNCHANGED <<now, promises, callbacks, listeners, resumes, badHalt>>

-----------------------------------------------------------------------------
(***************************************************************************)
(* Dispatch (SECTION 9) and time                                            *)
(***************************************************************************)

\* dequeue_execute / dequeue_unblock, resonate.sql:1235-1265: destructive read.
Dequeue(m) ==
    /\ m \in outbox
    /\ outbox' = outbox \ {m}
    /\ UNCHANGED <<now, promises, tasks, callbacks, listeners, resumes, badDispatch, badHalt>>

Tick ==
    /\ now < MaxTime
    /\ now' = now + 1
    /\ UNCHANGED <<core, badDispatch, badHalt>>

-----------------------------------------------------------------------------
Init ==
    /\ now        = 0
    /\ promises   = [i \in Ids |-> NoPromise]
    /\ tasks      = [i \in Ids |-> NoTask]
    /\ callbacks  = {}
    /\ listeners  = {}
    /\ resumes    = {}
    /\ outbox     = {}
    /\ obs        = [i \in Ids |-> "none"]
    /\ badDispatch = {}
    /\ badHalt     = {}

Step ==
    \/ \E i \in Ids, toat \in 1..MaxTime, k \in Kinds : PromiseCreate(i, toat, k)
    \/ \E i \in Ids, st \in ClientSettable            : PromiseSettle(i, st)
    \/ \E aw, ar \in Ids                              : RegisterCallback(aw, ar)
    \/ \E aw \in Ids, ad \in Addrs                    : RegisterListener(aw, ad)
    \/ \E i \in Ids                                   : TaskClaim(i)
    \/ \E i \in Ids                                   : TaskAcquire(i)
    \/ \E i \in Ids, S \in SUBSET Ids                 : TaskSuspend(i, S)
    \/ \E i \in Ids, st \in ClientSettable            : TaskFulfill(i, st)
    \/ \E i \in Ids                                   : TaskRelease(i)
    \/ \E i \in Ids                                   : TaskHalt(i)
    \/ \E i \in Ids                                   : TaskContinue(i)
    \/ \E i \in Ids                                   : OnPromiseTimeout(i)
    \/ \E i \in Ids                                   : OnTaskRetryTimeout(i)
    \/ \E i \in Ids                                   : OnTaskLeaseTimeout(i)
    \/ \E m \in outbox                                : Dequeue(m)
    \/ Tick

\* obs records, for each promise, the first non-pending state any client could
\* have read through promise.get.  It is a history variable only.
Next ==
    /\ Step
    /\ obs' = [i \in Ids |->
                 IF obs[i] = "none" /\ ProjOf(promises', now', i) \notin {"none", "pending"}
                 THEN ProjOf(promises', now', i)
                 ELSE obs[i]]

Spec == Init /\ [][Next]_vars

-----------------------------------------------------------------------------
(***************************************************************************)
(* Type invariant                                                           *)
(***************************************************************************)
PStates == {"pending", "resolved", "rejected", "rejected_canceled", "rejected_timedout"}
TStates == {"pending", "acquired", "suspended", "halted", "fulfilled"}

TypeOK ==
    /\ now \in 0..MaxTime
    /\ \A i \in Ids : promises[i].state \in PStates
    /\ \A i \in Ids : tasks[i].state \in TStates
    /\ \A i \in Ids : tasks[i].version \in 0..MaxVersion
    /\ callbacks \subseteq (Ids \X Ids)
    /\ listeners \subseteq (Ids \X Addrs)
    /\ resumes   \subseteq (Ids \X Ids)

-----------------------------------------------------------------------------
(***************************************************************************)
(* Properties                                                               *)
(***************************************************************************)

\* The pg_cron driver has nothing left to do at the current instant.
DriverIdle ==
    /\ \A i \in Ids : ~(promises[i].exists /\ promises[i].state = "pending"
                        /\ promises[i].external /\ promises[i].timeoutAt <= now)
    /\ \A i \in Ids : ~(tasks[i].exists /\ tasks[i].state \in {"pending", "acquired"}
                        /\ tasks[i].timeoutAt <= now)

\* End of the world: time has run out and the driver has caught up.
Quiesced == now = MaxTime /\ DriverIdle

(*--------------------------------------------------------------------------
  INV-1  Settlement is sticky.  Once a client has read a promise as settled
  through promise.get, it must never read anything else.
 --------------------------------------------------------------------------*)
Stickiness == \A i \in Ids : obs[i] # "none" => Proj(i) = obs[i]

(*--------------------------------------------------------------------------
  INV-2  A promise that any observer reads as settled must have delivered its
  unblock messages.  _cascade_settle deletes the listener rows as it emits
  (resonate.sql:277-284, 297), so a surviving listener row on an observably
  settled promise means the notification was never emitted.
 --------------------------------------------------------------------------*)
NoStrandedListener ==
    Quiesced => \A l \in listeners : Proj(l[1]) = "pending"

(*--------------------------------------------------------------------------
  INV-3  A suspended task must still be parked on a promise that is genuinely
  pending -- otherwise nothing will ever resume it.
 --------------------------------------------------------------------------*)
NoStrandedTask ==
    Quiesced => \A i \in Ids :
        tasks[i].state = "suspended" =>
            \E j \in Ids : <<j, i>> \in callbacks /\ Proj(j) = "pending"

(*--------------------------------------------------------------------------
  INV-4  No worker is ever handed an execution lease for, or dispatched
  against, a promise that is already observably dead.
 --------------------------------------------------------------------------*)
NoDeadDispatch == badDispatch = {}

(*--------------------------------------------------------------------------
  INV-7  A task is never halted once task.get would already report it
  'fulfilled' -- the wire must not contradict itself.
 --------------------------------------------------------------------------*)
NoHaltOnDead == badHalt = {}

(*--------------------------------------------------------------------------
  INV-5  Promise/task coherence: once the driver has caught up, a settled
  promise's task is fulfilled.
 --------------------------------------------------------------------------*)
TaskPromiseCoherence ==
    DriverIdle => \A i \in Ids :
        (promises[i].exists /\ promises[i].state # "pending" /\ tasks[i].exists)
            => tasks[i].state = "fulfilled"

(*--------------------------------------------------------------------------
  INV-6  Every callback is registered against an external promise -- the
  property commit 9187493 set out to establish.
 --------------------------------------------------------------------------*)
CallbacksAreExternal ==
    \A c \in callbacks : promises[c[1]].exists /\ promises[c[1]].external

=============================================================================

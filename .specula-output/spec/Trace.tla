------------------------------- MODULE Trace -------------------------------
(***************************************************************************)
(* Trace validation (Specula Phase 3A) for base.tla.                        *)
(*                                                                          *)
(* Drives TLC through an NDJSON trace recorded from a live Postgres running *)
(* the instrumented resonate.sql (see ../harness/), and checks that the base *)
(* spec can reproduce every observed transition AND that the spec's state    *)
(* agrees, field by field, with the state the database actually held after   *)
(* each action.                                                              *)
(*                                                                          *)
(* Category A: single trace file, linear cursor `l`.                         *)
(*                                                                          *)
(*   l = N  means "currently validating TraceLog[N]" (1-indexed).            *)
(***************************************************************************)
EXTENDS base, Sequences, Naturals, TLC, Json

CONSTANT TraceFile

RawLog   == ndJsonDeserialize(TraceFile)
TraceLog == SelectSeq(RawLog, LAMBDA e : e.tag = "trace")

VARIABLE l
tvars == <<vars, l>>

logline == TraceLog[l].event

ToSet(sq) == { sq[i] : i \in DOMAIN sq }

\* base.tla's Next conjoins this; the wrappers below call the raw actions, so
\* the history variable has to be advanced here instead.
ObsUpdate ==
    obs' = [i \in Ids |->
              IF obs[i] = "none" /\ ProjOf(promises', now', i) \notin {"none", "pending"}
              THEN ProjOf(promises', now', i)
              ELSE obs[i]]

-----------------------------------------------------------------------------
(***************************************************************************)
(* Post-state validation.                                                   *)
(*                                                                          *)
(* Every wrapper below calls the real base spec action and then requires the *)
(* resulting state to equal the recorded state. Nothing here is a stub: the  *)
(* promise table, the task table, callbacks, listeners, resume records and   *)
(* both outbox kinds are all compared.                                      *)
(***************************************************************************)

\* Ids present in the recorded state (the model's Ids that the DB knows about).
RecordedIds == DOMAIN logline.state.promises

PromiseOK(i) ==
    LET r == logline.state.promises[i] IN
    /\ promises'[i].exists
    /\ promises'[i].state     = r.state
    /\ promises'[i].timeoutAt = r.timeoutAt
    /\ promises'[i].external  = r.external
    /\ promises'[i].isTimer   = r.isTimer
    /\ promises'[i].hasTarget = r.hasTarget

TaskOK(i) ==
    LET r == logline.state.tasks[i] IN
    /\ tasks'[i].exists
    /\ tasks'[i].state     = r.state
    /\ tasks'[i].version   = r.version
    /\ tasks'[i].timeoutAt = r.timeoutAt

\* The recorded relations, as sets of tuples.
RecCallbacks == { <<p[1], p[2]>> : p \in ToSet(logline.state.callbacks) }
RecListeners == { <<p[1], p[2]>> : p \in ToSet(logline.state.listeners) }
RecResumes   == { <<p[1], p[2]>> : p \in ToSet(logline.state.resumes) }
RecExecs     == { <<p[1], p[2]>> : p \in ToSet(logline.state.execs) }
RecUnblocks  == { <<p[1], p[2]>> : p \in ToSet(logline.state.unblocks) }

SpecExecs    == { <<m.id, m.version>> : m \in {x \in outbox' : x.kind = "execute"} }
SpecUnblocks == { <<m.id, m.addr>>    : m \in {x \in outbox' : x.kind = "unblock"} }

ValidatePostState ==
    /\ \A i \in RecordedIds       : PromiseOK(i)
    /\ \A i \in Ids \ RecordedIds : ~promises'[i].exists
    /\ \A i \in DOMAIN logline.state.tasks       : TaskOK(i)
    /\ \A i \in Ids \ DOMAIN logline.state.tasks : ~tasks'[i].exists
    /\ callbacks' = RecCallbacks
    /\ listeners' = RecListeners
    /\ resumes'   = RecResumes
    /\ SpecExecs    = RecExecs
    /\ SpecUnblocks = RecUnblocks

\* The clock the action ran at must be the clock the spec is at.
AtRecordedTime == now = logline.now

TStep(name, action) ==
    /\ l <= Len(TraceLog)
    /\ logline.name = name
    /\ AtRecordedTime
    /\ action
    /\ ValidatePostState
    /\ ObsUpdate
    /\ l' = l + 1

-----------------------------------------------------------------------------
(***************************************************************************)
(* Action wrappers -- one per instrumented emit point.                      *)
(***************************************************************************)

\* The recorded post-state tells us which promise was created and how, so the
\* wrapper does not have to guess the arguments.
NewIds == { i \in RecordedIds : ~promises[i].exists }

KindOf(i) ==
    LET r == logline.state.promises[i] IN
    IF r.hasTarget THEN "target"
    ELSE IF r.isTimer THEN "timer"
    ELSE IF r.external THEN "ext" ELSE "plain"

TPromiseCreate ==
    TStep("PromiseCreate",
         \E i \in NewIds :
            PromiseCreate(i, logline.state.promises[i].timeoutAt, KindOf(i)))

\* Which promise settled: the one whose spec state is pending but whose
\* recorded state is not.
SettledIds ==
    { i \in RecordedIds :
        /\ promises[i].exists
        /\ promises[i].state = "pending"
        /\ logline.state.promises[i].state # "pending" }

TPromiseSettle ==
    TStep("PromiseSettle",
         \E i \in SettledIds : PromiseSettle(i, logline.state.promises[i].state))

TRegisterCallback ==
    TStep("RegisterCallback",
         \E c \in RecCallbacks \ callbacks : RegisterCallback(c[1], c[2]))

TRegisterListener ==
    TStep("RegisterListener",
         \E ln \in RecListeners \ listeners : RegisterListener(ln[1], ln[2]))

\* Task actions name their subject by the task whose recorded state differs.
ChangedTasks ==
    { i \in DOMAIN logline.state.tasks :
        \/ ~tasks[i].exists
        \/ tasks[i].state     # logline.state.tasks[i].state
        \/ tasks[i].version   # logline.state.tasks[i].version
        \/ tasks[i].timeoutAt # logline.state.tasks[i].timeoutAt }

AnyTask == DOMAIN logline.state.tasks

TTaskClaim    == TStep("TaskClaim",    \E i \in ChangedTasks : TaskClaim(i))
TTaskAcquire  == TStep("TaskAcquire",  \E i \in ChangedTasks : TaskAcquire(i))
TTaskFulfill  == TStep("TaskFulfill",  \E i \in ChangedTasks : TaskFulfill(i, logline.state.promises[i].state))
TTaskRelease  == TStep("TaskRelease",  \E i \in ChangedTasks : TaskRelease(i))
TTaskHalt     == TStep("TaskHalt",     \E i \in ChangedTasks : TaskHalt(i))
TTaskContinue == TStep("TaskContinue", \E i \in ChangedTasks : TaskContinue(i))

\* task.suspend, 200 path: the task that became suspended, awaiting the ids it
\* registered callbacks for in this step.
TTaskSuspend ==
    TStep("TaskSuspend",
         \E i \in AnyTask :
            \E S \in SUBSET Ids :
               /\ S # {}
               /\ \A j \in S : <<j, i>> \in RecCallbacks
               /\ TaskSuspend(i, S))

\* task.suspend, 300 path: an awaited was already settled, so the task stays
\* acquired and only its resume records are cleared.
TTaskSuspend300 ==
    TStep("TaskSuspend300",
         \E i \in AnyTask :
            \E S \in SUBSET Ids :
               /\ S # {}
               /\ i \notin S
               /\ \A j \in S : promises[j].exists
               /\ TaskSuspend(i, S))

TOnPromiseTimeout ==
    TStep("OnPromiseTimeout", \E i \in SettledIds : OnPromiseTimeout(i))

TOnTaskRetryTimeout ==
    TStep("OnTaskRetryTimeout", \E i \in AnyTask : OnTaskRetryTimeout(i))

TOnTaskLeaseTimeout ==
    TStep("OnTaskLeaseTimeout", \E i \in AnyTask : OnTaskLeaseTimeout(i))

-----------------------------------------------------------------------------
(***************************************************************************)
(* Silent actions.                                                          *)
(*                                                                          *)
(* Only one: the clock. The harness advances time by passing a larger        *)
(* `resonate:debug_time`, which produces no event of its own. Constrained to  *)
(* fire only while the spec's clock is behind the next event's -- otherwise   *)
(* it would branch unboundedly.                                              *)
(***************************************************************************)
TTick ==
    /\ l <= Len(TraceLog)
    /\ now < logline.now
    /\ Tick
    /\ ObsUpdate
    /\ UNCHANGED l

-----------------------------------------------------------------------------
TraceInit ==
    /\ Init
    /\ l = 1

TraceNext ==
    \/ TPromiseCreate
    \/ TPromiseSettle
    \/ TRegisterCallback
    \/ TRegisterListener
    \/ TTaskClaim
    \/ TTaskAcquire
    \/ TTaskSuspend
    \/ TTaskSuspend300
    \/ TTaskFulfill
    \/ TTaskRelease
    \/ TTaskHalt
    \/ TTaskContinue
    \/ TOnPromiseTimeout
    \/ TOnTaskRetryTimeout
    \/ TOnTaskLeaseTimeout
    \/ TTick
    \* the trace is exhausted; stutter so TLC terminates cleanly
    \/ /\ l > Len(TraceLog)
       /\ UNCHANGED tvars

\* Weak fairness is required: without it, "stutter forever at line N" is a
\* legal behaviour of any TLA+ spec, and TraceMatched would report a false
\* violation no matter how well the trace matches.
TraceSpec == TraceInit /\ [][TraceNext]_tvars /\ WF_tvars(TraceNext)

\* Without this property TLC reports success even if `l` never advances.
TraceMatched == <>(l > Len(TraceLog))

=============================================================================

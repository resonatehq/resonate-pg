--------------------------------- MODULE MC ---------------------------------
EXTENDS base

CONSTANTS a, b

MCIds   == {a, b}
MCAddrs == {"L"}

\* Keep the search finite: versions cannot grow without bound, and the model
\* only ever needs to distinguish "same lease" from "new lease".
StateConstraint ==
    /\ now <= MaxTime
    /\ \A i \in MCIds : tasks[i].version <= MaxVersion

=============================================================================

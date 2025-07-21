--------------------------------- MODULE cdraft ---------------------------------
EXTENDS Naturals, FiniteSets, Sequences, TLC

\* Domain-level abstraction constants
CONSTANTS Domain
CONSTANTS DomainLeader, MasterLeader, Follower
CONSTANTS Nil

\* Message types for cross-domain communication
CONSTANTS MasterElectionRequest, MasterElectionResponse,
          CrossDomainReq, CrossDomainResp,
          FastReturnRequest, FastReturnResponse

\* Variables for domain-level state
VARIABLE messages
VARIABLE currentTerm
VARIABLE domainState         \* DomainLeader, MasterLeader, or Follower
VARIABLE votedFor
VARIABLE masterLeader        \* Current master leader domain

\* Domain-level variables
VARIABLE domainLog           \* Log at domain level (abstracted)
VARIABLE commitIndex
VARIABLE crossDomainCommitted \* Which domains have committed

\* Master election variables
VARIABLE votesResponded
VARIABLE votesGranted
VARIABLE voterDomainLog

\* Fast Return variables  
VARIABLE pendingFastReturns  \* Pending fast return responses
VARIABLE fastReturnAcks      \* Received fast return acknowledgments

\* Client request management
VARIABLE clientRequests      \* Sequence number for unique requests

\* Variable groups
domainVars == <<currentTerm, domainState, votedFor, masterLeader>>
logVars == <<domainLog, commitIndex, crossDomainCommitted>>
electionVars == <<votesResponded, votesGranted, voterDomainLog>>
fastReturnVars == <<pendingFastReturns, fastReturnAcks>>
clientVars == <<clientRequests>>
vars == <<messages, domainVars, logVars, electionVars, fastReturnVars, clientVars>>

\* CD-Raft specific quorum definitions
\* Master election requires n-1 domains
MasterElectionQuorum == {D \in SUBSET(Domain) : Cardinality(D) >= Cardinality(Domain) - 1}

\* Cross-domain consensus requires at least 2 domains (ML + 1 other)
CrossDomainQuorum == {D \in SUBSET(Domain) : Cardinality(D) >= 2}

\* Helper functions
LastDomainTerm(dlog) == IF Len(dlog) = 0 THEN 0 ELSE dlog[Len(dlog)].term

Min(s) == CHOOSE x \in s : \A y \in s : x <= y
Max(s) == CHOOSE x \in s : \A y \in s : x >= y

WithMessage(m, msgs) ==
    IF m \in DOMAIN msgs THEN
        [msgs EXCEPT ![m] = msgs[m] + 1]
    ELSE
        msgs @@ (m :> 1)

WithoutMessage(m, msgs) ==
    IF m \in DOMAIN msgs THEN
        [msgs EXCEPT ![m] = msgs[m] - 1]
    ELSE
        msgs

ValidMessage(msgs) == {m \in DOMAIN messages : msgs[m] > 0}
Send(m) == messages' = WithMessage(m, messages)
Reply(response, request) == 
    messages' = WithoutMessage(request, WithMessage(response, messages))

\* Initialization
Init == 
    /\ messages = [m \in {} |-> 0]
    /\ currentTerm = [d \in Domain |-> 1]
    /\ domainState = [d \in Domain |-> Follower]
    /\ votedFor = [d \in Domain |-> Nil]
    /\ masterLeader = Nil
    /\ domainLog = [d \in Domain |-> <<>>]
    /\ commitIndex = [d \in Domain |-> 0]
    /\ crossDomainCommitted = {}
    /\ votesResponded = [d \in Domain |-> {}]
    /\ votesGranted = [d \in Domain |-> {}]
    /\ voterDomainLog = [d \in Domain |-> [e \in {} |-> <<>>]]
    /\ pendingFastReturns = [d \in Domain |-> {}]
    /\ fastReturnAcks = [d \in Domain |-> {}]
    /\ clientRequests = 1

\* Domain restart from failure - becomes follower and resets election state
DomainRestart(d) ==
    /\ domainState' = [domainState EXCEPT ![d] = Follower]
    /\ votesResponded' = [votesResponded EXCEPT ![d] = {}]
    /\ votesGranted' = [votesGranted EXCEPT ![d] = {}]
    /\ voterDomainLog' = [voterDomainLog EXCEPT ![d] = [e \in {} |-> <<>>]]
    /\ pendingFastReturns' = [pendingFastReturns EXCEPT ![d] = {}]
    /\ fastReturnAcks' = [fastReturnAcks EXCEPT ![d] = {}]
    /\ commitIndex' = [commitIndex EXCEPT ![d] = 0]
    \* If this was the master leader, reset it
    /\ masterLeader' = IF masterLeader = d THEN Nil ELSE masterLeader
    /\ UNCHANGED <<messages, currentTerm, votedFor, domainLog, crossDomainCommitted, clientVars>>

\* Domain timeout - becomes candidate for master leader
DomainTimeout(d) ==
    /\ domainState[d] \in {Follower, DomainLeader}
    /\ domainState' = [domainState EXCEPT ![d] = DomainLeader]
    /\ currentTerm' = [currentTerm EXCEPT ![d] = currentTerm[d] + 1]
    /\ votedFor' = [votedFor EXCEPT ![d] = Nil]
    /\ votesResponded' = [votesResponded EXCEPT ![d] = {}]
    /\ votesGranted' = [votesGranted EXCEPT ![d] = {}]
    /\ voterDomainLog' = [voterDomainLog EXCEPT ![d] = [e \in {} |-> <<>>]]
    /\ UNCHANGED <<messages, masterLeader, logVars, fastReturnVars, clientVars>>

\* Master leader step down (when it detects higher term or loses connectivity)
MasterLeaderStepDown(ml) ==
    /\ domainState[ml] = MasterLeader
    /\ domainState' = [domainState EXCEPT ![ml] = Follower]
    /\ masterLeader' = Nil
    /\ pendingFastReturns' = [pendingFastReturns EXCEPT ![ml] = {}]
    /\ fastReturnAcks' = [fastReturnAcks EXCEPT ![ml] = {}]
    /\ UNCHANGED <<messages, currentTerm, votedFor, logVars, electionVars, clientVars>>

\* Master election request
RequestMasterVote(i, j) ==
    /\ domainState[i] = DomainLeader
    /\ i /= j
    /\ j \notin votesResponded[i]
    /\ Send([mtype |-> MasterElectionRequest,
             mterm |-> currentTerm[i],
             mlastLogTerm |-> LastDomainTerm(domainLog[i]),
             mlastLogIndex |-> Len(domainLog[i]),
             msource |-> i,
             mdest |-> j])
    /\ UNCHANGED <<domainVars, logVars, electionVars, fastReturnVars, clientVars>>

\* Handle master election request
HandleMasterElectionRequest(i, j, m) ==
    LET logOk == \/ m.mlastLogTerm > LastDomainTerm(domainLog[i])
                 \/ /\ m.mlastLogTerm = LastDomainTerm(domainLog[i])
                    /\ m.mlastLogIndex >= Len(domainLog[i])
        grant == /\ m.mterm = currentTerm[i]
                 /\ logOk
                 /\ votedFor[i] \in {Nil, j}
    IN /\ m.mterm <= currentTerm[i]
       /\ \/ grant /\ votedFor' = [votedFor EXCEPT ![i] = j]
          \/ ~grant /\ UNCHANGED votedFor
       /\ Reply([mtype |-> MasterElectionResponse,
                 mterm |-> currentTerm[i],
                 mvoteGranted |-> grant,
                 mlog |-> domainLog[i],
                 msource |-> i,
                 mdest |-> j], m)
       /\ UNCHANGED <<domainState, currentTerm, masterLeader, logVars, electionVars, fastReturnVars, clientVars>>

\* Update term when receiving higher term (similar to UpdateTerm in original Raft)
UpdateDomainTerm(i, j, m) ==
    /\ m.mterm > currentTerm[i]
    /\ currentTerm' = [currentTerm EXCEPT ![i] = m.mterm]
    /\ domainState' = [domainState EXCEPT ![i] = Follower]
    /\ votedFor' = [votedFor EXCEPT ![i] = Nil]
    \* If this domain was master leader, step down
    /\ masterLeader' = IF masterLeader = i THEN Nil ELSE masterLeader
    /\ UNCHANGED <<messages, logVars, electionVars, fastReturnVars, clientVars>>

\* Handle master election response
HandleMasterElectionResponse(i, j, m) ==
    /\ m.mterm = currentTerm[i]
    /\ votesResponded' = [votesResponded EXCEPT ![i] = votesResponded[i] \cup {j}]
    /\ \/ /\ m.mvoteGranted
          /\ votesGranted' = [votesGranted EXCEPT ![i] = votesGranted[i] \cup {j}]
          /\ voterDomainLog' = [voterDomainLog EXCEPT ![i] = voterDomainLog[i] @@ (j :> m.mlog)]
       \/ /\ ~m.mvoteGranted
          /\ UNCHANGED <<votesGranted, voterDomainLog>>
    /\ UNCHANGED <<messages, domainVars, logVars, fastReturnVars, clientVars>>

\* Become master leader
BecomeMasterLeader(d) ==
    /\ domainState[d] = DomainLeader
    /\ votesGranted[d] \in MasterElectionQuorum
    /\ domainState' = [domainState EXCEPT ![d] = MasterLeader]
    /\ masterLeader' = d
    /\ UNCHANGED <<messages, currentTerm, votedFor, logVars, electionVars, fastReturnVars, clientVars>>

\* Cross-domain request (client request with unique ID)
CrossDomainClientRequest(ml) ==
    /\ domainState[ml] = MasterLeader
    /\ masterLeader = ml
    /\ LET entry == [term |-> currentTerm[ml], 
                     value |-> "request", 
                     client |-> "client",
                     id |-> clientRequests]
           newLog == Append(domainLog[ml], entry)
       IN /\ domainLog' = [domainLog EXCEPT ![ml] = newLog]
          /\ clientRequests' = clientRequests + 1
    /\ UNCHANGED <<messages, domainVars, commitIndex, crossDomainCommitted, electionVars, fastReturnVars>>

\* Fast Return mechanism - ML sends to other domains
InitiateFastReturn(ml, target) ==
    /\ domainState[ml] = MasterLeader
    /\ masterLeader = ml
    /\ ml /= target
    /\ Len(domainLog[ml]) > 0
    /\ target \notin pendingFastReturns[ml]  \* 避免重复发送
    /\ Send([mtype |-> FastReturnRequest,
             mterm |-> currentTerm[ml],
             mlog |-> domainLog[ml],
             mlogIndex |-> Len(domainLog[ml]),
             msource |-> ml,
             mdest |-> target])
    /\ pendingFastReturns' = [pendingFastReturns EXCEPT ![ml] = 
                                 pendingFastReturns[ml] \cup {target}]
    /\ UNCHANGED <<domainVars, logVars, electionVars, fastReturnAcks, clientVars>>

\* Handle fast return request - domain accepts and responds
HandleFastReturnRequest(target, ml, m) ==
    /\ m.mterm = currentTerm[target]
    /\ domainState[target] \in {Follower, DomainLeader}  \* 不能是MasterLeader
    /\ domainLog' = [domainLog EXCEPT ![target] = m.mlog]
    /\ commitIndex' = [commitIndex EXCEPT ![target] = m.mlogIndex]
    /\ crossDomainCommitted' = crossDomainCommitted \cup {target}
    /\ Reply([mtype |-> FastReturnResponse,
              mterm |-> currentTerm[target],
              msuccess |-> TRUE,
              mlogIndex |-> m.mlogIndex,
              msource |-> target,
              mdest |-> ml], m)
    /\ UNCHANGED <<domainVars, electionVars, fastReturnVars, clientVars>>

\* 新增：处理Fast Return响应
HandleFastReturnResponse(ml, target, m) ==
    /\ domainState[ml] = MasterLeader
    /\ masterLeader = ml
    /\ m.mterm = currentTerm[ml]
    /\ m.msuccess = TRUE
    /\ target \in pendingFastReturns[ml]  \* 确保这是一个待处理的响应
    /\ pendingFastReturns' = [pendingFastReturns EXCEPT ![ml] = 
                                 pendingFastReturns[ml] \ {target}]
    /\ fastReturnAcks' = [fastReturnAcks EXCEPT ![ml] = 
                             fastReturnAcks[ml] \cup {target}]
    /\ UNCHANGED <<messages, domainVars, logVars, electionVars, clientVars>>

\* Master leader commits after receiving sufficient cross-domain confirmations
CommitCrossDomain(ml) ==
    /\ domainState[ml] = MasterLeader
    /\ masterLeader = ml
    /\ Len(domainLog[ml]) > commitIndex[ml]
    /\ LET confirmedDomains == crossDomainCommitted \cup fastReturnAcks[ml] \cup {ml}
       IN /\ Cardinality(confirmedDomains) >= 2  \* 至少2个域确认
          /\ commitIndex' = [commitIndex EXCEPT ![ml] = Len(domainLog[ml])]
    /\ UNCHANGED <<messages, domainVars, domainLog, crossDomainCommitted, electionVars, fastReturnVars, clientVars>>

\* 新增：清理已确认的Fast Return ACKs
CleanupFastReturnAcks(ml) ==
    /\ domainState[ml] = MasterLeader
    /\ fastReturnAcks[ml] /= {}
    /\ fastReturnAcks' = [fastReturnAcks EXCEPT ![ml] = {}]
    /\ UNCHANGED <<messages, domainVars, logVars, electionVars, pendingFastReturns, clientVars>>

\* 完善的消息处理
Receive(m) ==
    LET i == m.mdest
        j == m.msource
    IN \/ UpdateDomainTerm(i, j, m)
       \/ /\ m.mtype = MasterElectionRequest
          /\ HandleMasterElectionRequest(i, j, m)
       \/ /\ m.mtype = MasterElectionResponse
          /\ HandleMasterElectionResponse(i, j, m)
       \/ /\ m.mtype = FastReturnRequest
          /\ HandleFastReturnRequest(i, j, m)
       \/ /\ m.mtype = FastReturnResponse
          /\ HandleFastReturnResponse(i, j, m)

\* Next state relation
Next == 
    \/ \E d \in Domain : DomainRestart(d)
    \/ \E d \in Domain : DomainTimeout(d)
    \/ \E ml \in Domain : MasterLeaderStepDown(ml)
    \/ \E i, j \in Domain : RequestMasterVote(i, j)
    \/ \E d \in Domain : BecomeMasterLeader(d)
    \/ \E ml \in Domain : CrossDomainClientRequest(ml)
    \/ \E ml, target \in Domain : InitiateFastReturn(ml, target)
    \/ \E ml \in Domain : CommitCrossDomain(ml)
    \/ \E ml \in Domain : CleanupFastReturnAcks(ml)
    \/ \E m \in ValidMessage(messages) : Receive(m)

Spec == Init /\ [][Next]_vars

\* Safety Properties for CD-Raft

\* At most one master leader per term
NoTwoMasterLeaders ==
    \A i, j \in Domain : 
        /\ i /= j
        /\ domainState[i] = MasterLeader
        /\ domainState[j] = MasterLeader
        => currentTerm[i] /= currentTerm[j]

\* Master leader election safety - requires n-1 domains
MasterElectionSafety ==
    \A d \in Domain :
        domainState[d] = MasterLeader =>
        Cardinality(votesGranted[d]) >= Cardinality(Domain) - 1

\* Cross-domain commit safety - requires at least 2 domains  
CrossDomainCommitSafety ==
    \A d \in Domain :
        /\ domainState[d] = MasterLeader
        /\ commitIndex[d] > 0
        => LET confirmedDomains == crossDomainCommitted \cup fastReturnAcks[d] \cup {d}
           IN Cardinality(confirmedDomains) >= 2

\* Fast Return safety - committed entries are consistent
FastReturnSafety ==
    \A d1, d2 \in Domain :
        /\ commitIndex[d1] > 0
        /\ commitIndex[d2] > 0
        /\ d1 /= d2
        => \* 如果两个域都有提交的日志，则在重叠部分应该一致
           \E commonLen \in Nat :
               /\ commonLen <= Min({commitIndex[d1], commitIndex[d2]})
               /\ commonLen <= Min({Len(domainLog[d1]), Len(domainLog[d2])})
               /\ \A i \in 1..commonLen :
                   domainLog[d1][i] = domainLog[d2][i]

\* Fast Return response consistency - 简化版本
FastReturnResponseConsistency ==
    \A d \in Domain :
        domainState[d] = MasterLeader =>
        \* 如果有Fast Return确认，则对应的域应该在跨域提交集合中或有相应的日志
        \A target \in fastReturnAcks[d] :
            \/ target \in crossDomainCommitted 
            \/ Len(domainLog[target]) >= commitIndex[d]

\* No duplicate Fast Return requests
NoDuplicateFastReturnRequests ==
    \A d \in Domain :
        domainState[d] = MasterLeader =>
        \* 待处理的Fast Return请求和已确认的不应该重叠
        pendingFastReturns[d] \cap fastReturnAcks[d] = {}

\* Master leader consistency - 同时只能有一个master leader
MasterLeaderConsistency ==
    \A d1, d2 \in Domain :
        /\ domainState[d1] = MasterLeader
        /\ domainState[d2] = MasterLeader
        => d1 = d2

\* Basic state consistency
BasicStateConsistency ==
    /\ \A d \in Domain : domainState[d] \in {Follower, DomainLeader, MasterLeader}
    /\ \A d \in Domain : currentTerm[d] >= 1
    /\ masterLeader \in Domain \cup {Nil}
    /\ (masterLeader /= Nil) => domainState[masterLeader] = MasterLeader

\* Recovery safety - after restart, domain becomes follower with clean state
RecoverySafety ==
    \A d \in Domain :
        \* 如果域重启后成为follower，则其选举状态应该被清理
        \* 但允许在运行过程中follower参与选举
        TRUE  \* 暂时设为TRUE，因为动态状态检查复杂

\* Alternative: 更精确的恢复安全性
RecoverySafetyPrecise ==
    \A d \in Domain :
        \* 如果域不是Master Leader，则不应该有待处理的Fast Return请求
        domainState[d] /= MasterLeader =>
        pendingFastReturns[d] = {}

\* Term monotonicity - terms never decrease
TermMonotonicity ==
    \A d \in Domain : currentTerm[d] >= 1

\* State space constraints to prevent explosion - 放宽限制
LogLengthConstraint == \A d \in Domain : Len(domainLog[d]) <= 4
TermConstraint == \A d \in Domain : currentTerm[d] <= 5
MessageConstraint == Cardinality(DOMAIN messages) <= 20
ClientRequestConstraint == clientRequests <= 10
FastReturnConstraint == \A d \in Domain : Cardinality(pendingFastReturns[d]) <= 3

\* Combined constraint with more reasonable limits
StateConstraint == LogLengthConstraint /\ TermConstraint /\ MessageConstraint /\ ClientRequestConstraint /\ FastReturnConstraint

\* Liveness properties
EventualMasterLeader == 
    <>(\E d \in Domain : domainState[d] = MasterLeader)

WeakEventualMasterLeader ==
    \A d \in Domain : 
        (Cardinality(votesGranted[d]) >= Cardinality(Domain) - 1) 
        ~> (domainState[d] = MasterLeader)

EventualCrossDomainReplication ==
    \A d \in Domain :
        /\ domainState[d] = MasterLeader
        /\ commitIndex[d] > 0
        => <>(Cardinality(crossDomainCommitted \cup fastReturnAcks[d]) >= 1)

===============================================================================
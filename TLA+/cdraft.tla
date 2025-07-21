----------------------- MODULE cdraft -----------------------
EXTENDS Naturals, Sequences, FiniteSets

CONSTANTS
   Domains,        
   MaxRequests,    
   Nil

VARIABLES
   mlState,        
   mlTerm,         \* ML任期
   mlLog,          \* ML日志信息
   mlCommitted,    
   currentML,      
   
   dlStates,       \* DL状态映射
   dlTerms,        
   dlLogs,         \* DL的日志信息
   dlCommitted,    
   
   votes,          
   responses,      \* 快速返回响应
   reqCounter      

vars == <<mlState, mlTerm, mlLog, mlCommitted, currentML, dlStates, dlTerms, 
         dlLogs, dlCommitted, votes, responses, reqCounter>>

NumDomains == Cardinality(Domains)

Init ==
   /\ mlState = "follower"
   /\ mlTerm = 0
   /\ mlLog = <<>>
   /\ mlCommitted = {}
   /\ currentML = Nil          
   /\ dlStates = [d \in Domains |-> "follower"]
   /\ dlTerms = [d \in Domains |-> 0]
   /\ dlLogs = [d \in Domains |-> <<>>]
   /\ dlCommitted = [d \in Domains |-> {}]
   /\ votes = {}
   /\ responses = {}
   /\ reqCounter = 0

StartElection(candidateDomain) ==
   /\ candidateDomain \in Domains
   /\ mlState = "follower"
   /\ dlStates[candidateDomain] = "follower"
   /\ mlTerm < 100  
   /\ mlState' = "candidate"
   /\ mlTerm' = mlTerm + 1
   /\ currentML' = candidateDomain
   /\ votes' = {candidateDomain}          
   /\ dlStates' = [dlStates EXCEPT ![candidateDomain] = "candidate"]
   /\ dlTerms' = [dlTerms EXCEPT ![candidateDomain] = mlTerm + 1]
   /\ UNCHANGED <<mlLog, mlCommitted, dlLogs, dlCommitted, responses, reqCounter>>

VoteForML(voter) ==
   /\ voter \in Domains
   /\ mlState = "candidate"
   /\ currentML \in Domains
   /\ voter # currentML
   /\ dlStates[voter] = "follower"
   /\ voter \notin votes
   /\ dlTerms[voter] < mlTerm 
   /\ votes' = votes \cup {voter}
   /\ dlTerms' = [dlTerms EXCEPT ![voter] = mlTerm]
   /\ UNCHANGED <<mlState, mlTerm, mlLog, mlCommitted, currentML, dlStates, dlLogs, dlCommitted, responses, reqCounter>>

\* 成为领导者
BecomeLeader ==
   /\ mlState = "candidate"
   /\ currentML \in Domains
   /\ Cardinality(votes) >= NumDomains - 1  
   /\ mlState' = "leader"
   /\ dlStates' = [dlStates EXCEPT ![currentML] = "leader"]
   /\ UNCHANGED <<mlTerm, mlLog, mlCommitted, currentML, dlTerms, dlLogs, dlCommitted, votes, responses, reqCounter>>

\*ML接收客户端请求
ReceiveClientRequest ==
   /\ mlState = "leader"
   /\ currentML \in Domains
   /\ reqCounter < MaxRequests
   /\ reqCounter' = reqCounter + 1
   /\ LET req == reqCounter + 1
          clientDomain == CHOOSE d \in Domains : d # currentML
          entry == [request |-> req, term |-> mlTerm, clientDomain |-> clientDomain, mlDomain |-> currentML]
      IN /\ mlLog' = Append(mlLog, entry)
   /\ UNCHANGED <<mlState, mlTerm, mlCommitted, currentML, dlStates, dlTerms, dlLogs, dlCommitted, votes, responses>>

\* AppendEntries
ReplicateToDL ==
   /\ mlState = "leader"  
   /\ currentML \in Domains
   /\ Len(mlLog) > 0
   /\ \E d \in Domains, i \in 1..Len(mlLog) :
       /\ Len(dlLogs[d]) < Len(mlLog)  
       /\ i = Len(dlLogs[d]) + 1       
       /\ dlLogs' = [dlLogs EXCEPT ![d] = Append(dlLogs[d], mlLog[i])]
   /\ UNCHANGED <<mlState, mlTerm, mlLog, mlCommitted, currentML, dlStates, dlTerms, dlCommitted, votes, responses, reqCounter>>

MLCommit ==
   /\ mlState = "leader"
   /\ currentML \in Domains
   /\ Len(mlLog) > Cardinality(mlCommitted)
   /\ \E i \in 1..Len(mlLog) :
       /\ mlLog[i].request \notin mlCommitted
       /\ mlCommitted' = mlCommitted \cup {mlLog[i].request}
   /\ UNCHANGED <<mlState, mlTerm, mlLog, currentML, dlStates, dlTerms, dlLogs, dlCommitted, votes, responses, reqCounter>>

\* 客户端异域情况的判断
FastReturn(d) ==
   /\ d \in Domains
   /\ dlStates[d] = "follower"
   /\ \E i \in 1..Len(dlLogs[d]) :
       /\ dlLogs[d][i].clientDomain = d  
       /\ dlLogs[d][i].request \in mlCommitted 
       /\ dlLogs[d][i].request \notin dlCommitted[d]
       /\ dlCommitted' = [dlCommitted EXCEPT ![d] = @ \cup {dlLogs[d][i].request}]
       /\ responses' = responses \cup {[request |-> dlLogs[d][i].request, 
                                      domain |-> d, 
                                      mlDomain |-> dlLogs[d][i].mlDomain]}
   /\ UNCHANGED <<mlState, mlTerm, mlLog, mlCommitted, currentML, dlStates, dlTerms, dlLogs, votes, reqCounter>>

\* 避免死锁，设置一下Nil
ElectionTimeout ==
   /\ mlState = "candidate"
   /\ mlTerm < 100
   /\ Cardinality(votes) < NumDomains - 1  
   /\ mlState' = "follower"
   /\ currentML' = Nil
   /\ dlStates' = [d \in Domains |-> "follower"]
   /\ UNCHANGED <<mlTerm, mlLog, mlCommitted, dlTerms, dlLogs, dlCommitted, votes, responses, reqCounter>>

Reset ==
   /\ reqCounter >= MaxRequests
   /\ Cardinality(responses) >= 1
   /\ mlState' = "follower"
   /\ mlTerm' = 0
   /\ mlLog' = <<>>
   /\ mlCommitted' = {}
   /\ currentML' = Nil
   /\ dlStates' = [d \in Domains |-> "follower"]
   /\ dlTerms' = [d \in Domains |-> 0]
   /\ dlLogs' = [d \in Domains |-> <<>>]
   /\ dlCommitted' = [d \in Domains |-> {}]
   /\ votes' = {}
   /\ responses' = {}
   /\ reqCounter' = 0

Next ==
   \/ \E d \in Domains : StartElection(d)
   \/ \E d \in Domains : VoteForML(d)
   \/ BecomeLeader
   \/ ReceiveClientRequest        \* 1.ML接收client请求
   \/ ReplicateToDL              \* 2.ML分发到DL
   \/ MLCommit
   \/ \E d \in Domains : FastReturn(d)
   \/ ElectionTimeout
   \/ Reset

Spec == Init /\ [][Next]_vars

TypeOK ==
   /\ mlState \in {"follower", "candidate", "leader"}
   /\ mlTerm \in 0..100
   /\ mlCommitted \subseteq 1..MaxRequests
   /\ currentML \in Domains \cup {Nil}
   /\ votes \subseteq Domains
   /\ reqCounter \in 0..MaxRequests
   /\ \A d \in Domains : dlStates[d] \in {"follower", "candidate", "leader"}
   /\ \A d \in Domains : dlCommitted[d] \subseteq 1..MaxRequests


MLDLStateConsistency ==
   /\ (mlState = "leader") => (currentML \in Domains /\ dlStates[currentML] = "leader")
   /\ (mlState = "candidate") => (currentML \in Domains /\ dlStates[currentML] = "candidate")
   /\ (mlState = "follower") => (currentML = Nil)

VoteConsistency ==
   (mlState \in {"candidate", "leader"} /\ currentML \in Domains) => (currentML \in votes)

MLDomainConsistency ==
   mlState = "leader" => currentML \in Domains

\* test
ElectionSafety ==
   mlState = "leader" => Cardinality(votes) >= NumDomains - 1

FastReturnSafety ==
   \A resp \in responses :
       resp.request \in mlCommitted

Consistency ==
   \A d \in Domains :
       dlCommitted[d] \subseteq mlCommitted

CDRaftCoreSafety ==
   /\ FastReturnSafety
   /\ Consistency

ClientDomainSafety ==
   \A resp \in responses :
       \E i \in 1..Len(mlLog) :
           /\ mlLog[i].request = resp.request
           /\ mlLog[i].clientDomain = resp.domain

LogReplicationConsistency ==
   \A d \in Domains :
       /\ Len(dlLogs[d]) <= Len(mlLog)
       /\ \A i \in 1..Len(dlLogs[d]) : dlLogs[d][i] = mlLog[i]

DLCommitConsistency ==
   \A resp \in responses :
       LET d == resp.domain IN
       \A req \in dlCommitted[d] :
           \E i \in 1..Len(mlLog) :
               /\ mlLog[i].request = req
               /\ \E j \in 1..Len(dlLogs[d]) :
                   /\ dlLogs[d][j].request = req
                   /\ dlLogs[d][j] = mlLog[i]

FastReturnLogConsistency ==
   \A resp \in responses :
       LET d == resp.domain 
           reqId == resp.request IN
       \E i \in 1..Len(dlLogs[d]) :
           /\ dlLogs[d][i].request = reqId
           /\ \E j \in 1..Len(mlLog) :
               /\ mlLog[j].request = reqId
               /\ dlLogs[d][i] = mlLog[j]

CommitOrderConsistency ==
   \A d \in Domains :
       \A req1, req2 \in dlCommitted[d] :
           \E i, j \in 1..Len(mlLog) :
               /\ mlLog[i].request = req1
               /\ mlLog[j].request = req2
               /\ (req1 \in mlCommitted /\ req2 \in mlCommitted)
               /\ (i < j) => (req1 \in dlCommitted[d] => req2 \in dlCommitted[d])


ReplicationLogic ==
   \A d \in Domains :
       \A i \in 1..Len(dlLogs[d]) :
           \E j \in 1..Len(mlLog) :
               /\ j <= i
               /\ dlLogs[d][i] = mlLog[j]

MainInvariant ==
   /\ TypeOK
   /\ MLDLStateConsistency
   /\ VoteConsistency
   /\ MLDomainConsistency
   /\ ElectionSafety
   /\ CDRaftCoreSafety
   /\ ClientDomainSafety
   /\ LogReplicationConsistency
   /\ DLCommitConsistency
   /\ FastReturnLogConsistency
   /\ CommitOrderConsistency
   /\ ReplicationLogic


=============================================================================
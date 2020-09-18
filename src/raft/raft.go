package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import "sync"
import "sync/atomic"
import "../labrpc"
//import "fmt" // for debug purpose

import "time"          // for timeouts
import "math/rand"     // for random election timeouts

// import "bytes"
// import "../labgob"



//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

const (
	LEADER = iota
	FOLLOWER
	CANDIDATE
)

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	state int    // leader/follower/candidate

	// vote counts for candidate
	voteCnt int

	// apply channel
	applyCh chan ApplyMsg
	
	// timestamp for the last communication with leader (for followers)
	// or elect time stamp for candidates
	leadTimestamp time.Time
	electTimeout int64 // timeout for election

	// Persistent state on all servers:
	currentTerm int    // latest term server has seen
	votedFor int       // candidateId (index in peers[]) that received vote in current term (or -1 if none) 
	log []logEntry	   // log entries, log[0] is a dummy entry;
			   // real log starts at index 1.

	// Volatile state on all servers:
	commitIndex int    // index of highest log entry known to be committed
	lastApplied int    // index of highest log entry applied to state machine
	
	// Volatile state on leaders (reinitialized after election) 
	nextIndex []int   // for each server, index of next log entry to send to that server
	matchIndex []int  // for each server, index of highest log entry known to be replicated on server
}

type logEntry struct {
	// command for state machine:
	Command interface{} // empty interface in golang
	// term when entry was received by leader:
	Term int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term, isleader = rf.currentTerm, rf.state == LEADER
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}


//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

//
// AppendEntries RPC arguments structure:
//
type AppendEntriesArgs struct {
	Term int               // leader's term
	LeaderId int           // so follower can redirect clients
	PrevLogIndex int       // index of log entry immediately preceding new ones
	PrevLogTerm int        // term of PrevLogIndex entry
	Entries []logEntry     // log entries to store (empty for heartbeat)
	LeaderCommit int       // leader's commitIndex 
}

//
// AppendEntries RPC reply structure.
//
type AppendEntriesReply struct {
	Term int   	  // currentTerm, for leader to update itself
	Success bool      // true if follower contained entry matching 
			  // prevLogIndex and prevLogTerm
}

//
// AppendEntries RPC handler.
//
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	
	// first, rules for servers, All Servers, rule #2:
	// should we set currentTerm here at the beginning?
	if args.Term > rf.currentTerm {
	    rf.currentTerm = args.Term
	    rf.state = FOLLOWER
	    rf.votedFor = -1
	    rf.leadTimestamp = time.Now()
	    rf.electTimeout = 500 + rand.Int63n(1000)
	}

	reply.Term = rf.currentTerm
	
	// TODO: Receiver implementation
	// step1
	if args.Term < rf.currentTerm {
	    reply.Success = false
	    return
	}

	// update the timestamp after comparing the terms
	rf.leadTimestamp = time.Now()
	rf.electTimeout = 500 + rand.Int63n(1000)

	// step2
	if len(rf.log) - 1 < args.PrevLogIndex || 
	   rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
	    reply.Success = false
	    return
	} 
	// TODO: step 3 to step 5
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term int           // candidate's term
	CandidateId int    // candidate reqeusting vote
	LastLogIndex int   // index of candidate's last log entry
	LastLogTerm int    // term of candidate's last log entry
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term int   	  // currentTerm, for candidate to update itself
	VoteGranted bool  // true means candidate received vote
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	
	// first, rules for servers, All Servers, rule #2:
	if args.Term > rf.currentTerm {
	    rf.currentTerm = args.Term
	    rf.state = FOLLOWER
	    rf.votedFor = -1
	    rf.leadTimestamp = time.Now()
	    rf.electTimeout = 500 + rand.Int63n(1000)
	}
	
	// then, Receiver Implementation for RequestVote RPC
	
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
	    reply.VoteGranted = false
	    return
	}
	
	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) &&
	   (len(rf.log) - 1 <= args.LastLogIndex && 
	    rf.log[len(rf.log)-1].Term <= args.LastLogTerm) {
	    reply.VoteGranted = true
	    rf.votedFor = args.CandidateId
	    rf.leadTimestamp = time.Now()
	    rf.electTimeout = 500 + rand.Int63n(1000)
	}
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement (me: new go routine)and return immediately. 
// there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).


	return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.state = FOLLOWER
	rf.applyCh = applyCh
	rf.leadTimestamp = time.Now()
	rf.electTimeout = 500 + rand.Int63n(1000)
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = make([]logEntry, 0)
	// the dummy log[0]:
	rf.log = append(rf.log, logEntry{Command: nil, Term: 0})
	rf.commitIndex = 0
	rf.lastApplied = 0

	// initialize from state persisted before a crash
	// the following will be implemented in 2C
	rf.readPersist(persister.ReadRaftState())

	go rf.periodicCheck()
	go rf.heartBeat()

	return rf
}

// goroutine that will kick off leader election periodically
func (rf *Raft) periodicCheck() {
	for !rf.killed() {
	    // followers or candidates do this periodic check
	    rf.mu.Lock()
	    if rf.state != LEADER {
		// no need to do this periodic check for the leader
	        timeElapsed := time.Now().Sub(rf.leadTimestamp).Milliseconds() 
	        if timeElapsed > rf.electTimeout {
		    // kick off the leader election
		    go rf.elect()
	        }
	    }
	    rf.mu.Unlock()

	    time.Sleep(100*time.Millisecond)
	}	
}

// goroutine that does the election
func (rf *Raft) elect() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.state = CANDIDATE
	rf.currentTerm += 1
	rf.votedFor = rf.me
	rf.voteCnt = 1
	rf.leadTimestamp = time.Now()
	rf.electTimeout = 500 + rand.Int63n(1000)
	args := RequestVoteArgs{
	    Term: rf.currentTerm,
	    CandidateId: rf.me,
	    LastLogIndex: len(rf.log) - 1,
	    LastLogTerm: rf.log[len(rf.log)-1].Term,
	}
	for i := range rf.peers {
	    if i == rf.me {
	        continue
	    }
	    argsCopy := args
	    // using goroutines on loop iterator variables
	    go func(i int) {
	        reply := &RequestVoteReply{}
		if rf.sendRequestVote(i, &argsCopy, reply) {
		    // handle the reply
		    rf.mu.Lock()
		    defer rf.mu.Unlock()
		    if reply.Term > rf.currentTerm {
		        rf.currentTerm = reply.Term
			rf.state = FOLLOWER
			rf.votedFor = -1
			rf.leadTimestamp = time.Now()
			rf.electTimeout = 500 + rand.Int63n(1000)
		    }
		    if rf.state == CANDIDATE && rf.currentTerm == args.Term {
		        if reply.VoteGranted {
			    rf.voteCnt += 1
			    if 2 * rf.voteCnt > len(rf.peers) {
			        rf.state = LEADER
			    } 
			}
		    }
		}
	    } (i)
	}
}

func (rf *Raft) sendHeartBeat() {
    // only the leader sends heart beat
    rf.mu.Lock()
    defer rf.mu.Unlock()
    if rf.state == LEADER {
	args := AppendEntriesArgs {
	    Term: rf.currentTerm,
	    LeaderId: rf.me,
	    PrevLogIndex: 0,  // 0 for heartbeat
	    PrevLogTerm: 0,
	    Entries: nil,
	    LeaderCommit: rf.commitIndex,
	}
	for i := range rf.peers {
	    if i == rf.me {
		continue
	    }
	    argsCopy := args
	    go func (i int) {
		reply := &AppendEntriesReply{}
		if rf.sendAppendEntries(i, &argsCopy, reply){
		    // handle the reply
		    rf.mu.Lock()
		    defer rf.mu.Unlock()
		    if reply.Term > rf.currentTerm {
			rf.currentTerm = reply.Term
			rf.state = FOLLOWER
			rf.votedFor = -1
			rf.leadTimestamp = time.Now()
			rf.electTimeout = 500 + rand.Int63n(1000)
		    }
		}
	    } (i)
	}
    }
} 

// goroutine for periodic hearbeats from leader to followers
func (rf *Raft) heartBeat() {
	// 10 times per second
	for !rf.killed() {
	    go rf.sendHeartBeat()
	    time.Sleep(100*time.Millisecond)
	}
}



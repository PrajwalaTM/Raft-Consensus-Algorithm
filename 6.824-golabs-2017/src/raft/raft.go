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
import "labrpc"
import "math"
import "math/rand"
import "time"
import "fmt"

// import "bytes"
// import "encoding/gob"



//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//
// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	currentTerm int
	votedFor	int
	log 		[]*LogEntry
	commitIndex int
	lastApplied int
	nextIndex  []int
	matchIndex []int
	lastLogTerm int
	lastLogIndex int
	electiontimeout time.Duration 
	heartbeattimeout time.Duration 
	// to store states
	isLeader bool
	isCandidate bool
	isFollower bool
	election_tick *time.Ticker //ticker for election time-out of each Raft peer
	heartbeat_tick *time.Ticker // ticker for heartbeat timeouts
	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.	
}
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

// struct to store log entries
type LogEntry struct {
	Term int
	Command interface{}  
}
//

// Arguments for AppendEntriesRPC

type AppendEntriesArgs struct {
	Term int
	LeaderId int
	PrevLogIndex int
	PrevLogTerm int
//	Entries []*LogEntry
	LeaderCommit int
}

//Arguments for AppendEntriesReply

type AppendEntriesReply struct {
	Term int
	Success bool
}

// AppendEntries RPC definition
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {

 	fmt.Printf("LeaderCommit %d commitIndex",args.Term)
	//Resetting the timer if append entries is received
	/*rf.election_tick.Stop()
	var temp time.Duration
    temp = time.Duration(rand.Intn(250)+500)
    rf.election_tick = time.NewTicker(time.Millisecond*temp) 
    fmt.Printf("checkpoint A\n")
   // fmt.Printf("LeaderCommit %d commitIndex",args.Term)
    
    //update the commit index to the minimum of leader's and receiving peer's commit index
    if(args.LeaderCommit>rf.commitIndex) {
    	 fmt.Printf("checkpoint A'")
		rf.commitIndex=int(math.Min(float64(args.LeaderCommit),float64(rf.lastLogIndex)))
		if(rf.commitIndex>rf.lastApplied){
			rf.lastApplied=rf.lastApplied+1
		}

	}
	 fmt.Printf("checkpoint B")

	 // Change state of all candidates to followers once leader is elected
	if(rf.isCandidate == true){
		rf.isFollower = true
		rf.isCandidate = false
	}

	//If leader's term is less than peer's term, do no append entries
	 fmt.Printf("checkpoint c")
	if(args.Term<rf.currentTerm){
		reply.Success=false
	} else if (rf.log[args.PrevLogIndex]==nil || rf.log[args.PrevLogIndex].Term<args.PrevLogTerm) { // If log entry in prevLogIndex is empty, reply false
		reply.Success=false
		 fmt.Printf("checkpoint D")
	} else {
		if(len(args.Entries)==0) {
			reply.Success=true //heartbeat
			 fmt.Printf("checkpoint E")
		} else if(rf.log[args.PrevLogIndex+1]!=nil && rf.log[args.PrevLogIndex+1] != nil && args.Entries[0]!= nil && rf.log[args.PrevLogIndex+1].Term<(args.Entries[0]).Term) { // delete existing entries in case of conflicts
			for i:=args.PrevLogIndex+1; i <len(rf.log);i++ {
				rf.log[i]=nil
			} //append new entries
			rf.log[rf.lastLogIndex+1]=args.Entries[0]
			rf.lastLogIndex=rf.lastLogIndex+1
			rf.lastLogTerm=args.Entries[0].Term
			reply.Success=true
			 fmt.Printf("checkpoint F")
		} else {
			//append new entries
			rf.log[rf.lastLogIndex+1]=args.Entries[0]
			rf.lastLogIndex=rf.lastLogIndex+1
			rf.lastLogTerm=args.Entries[0].Term
			reply.Success=true
			 fmt.Printf("checkpoint g")
		}
	}
	*/


}


// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	term= rf.currentTerm
	if(rf.isLeader == true){
		isleader = true
	} else {
		isleader = false
	}
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
	// e := gob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := gob.NewDecoder(r)
	// d.Decode(&rf.xxx)
	// d.Decode(&rf.yyy)
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
}


	

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term int
	CandidateId int
	LastLogIndex int
	LastLogTerm int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term int
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	fmt.Printf("Term of args%d\n",args.Term)
	// ignore candidate's request if candidate's term is lesser than receiver's term
	if (args.Term<rf.currentTerm) {
		reply.VoteGranted=false
		reply.Term=rf.currentTerm
	} else if((rf.votedFor==-1 || rf.votedFor==args.CandidateId)&&(rf.lastLogIndex<args.LastLogIndex || (rf.lastLogTerm==args.LastLogTerm && rf.lastLogIndex<=args.LastLogIndex))){
		// Vote if all conditions are satisfied and receiver hasn't voted for any other candidate
		//Resetting the timer if it is about to grant the vote
		//rf.election_tick.Stop()
		//var temp time.Duration
    	//temp = (rand.Intn(250)+500)
    	//rf.election_tick = time.NewTicker(time.Millisecond*temp) 
		rf.votedFor = args.CandidateId
		reply.VoteGranted=true
		rf.currentTerm=int(math.Max(float64(rf.currentTerm),float64(args.Term)))
		reply.Term=rf.currentTerm
	} else{
		reply.VoteGranted=false
		reply.Term=rf.currentTerm
	}
	fmt.Printf("Vote sent %d\n",rf.me)
}

func (rf *Raft) RequestVote1(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	fmt.Printf("Term of args%d\n",args.Term)
	// ignore candidate's request if candidate's term is lesser than receiver's term
	if (args.Term<rf.currentTerm) {
		reply.VoteGranted=false
		reply.Term=rf.currentTerm
	} else if((rf.votedFor==-1 || rf.votedFor==args.CandidateId)&&(rf.lastLogIndex<args.LastLogIndex || (rf.lastLogTerm==args.LastLogTerm && rf.lastLogIndex<=args.LastLogIndex))){
		// Vote if all conditions are satisfied and receiver hasn't voted for any other candidate
		//Resetting the timer if it is about to grant the vote
		//rf.election_tick.Stop()
		//var temp time.Duration
    	//temp = (rand.Intn(250)+500)
    	//rf.election_tick = time.NewTicker(time.Millisecond*temp) 
		rf.votedFor = args.CandidateId
		reply.VoteGranted=true
		rf.currentTerm=int(math.Max(float64(rf.currentTerm),float64(args.Term)))
		reply.Term=rf.currentTerm
	} else{
		reply.VoteGranted=false
		reply.Term=rf.currentTerm
	}
	fmt.Printf("Vote sent %d\n",rf.me)
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
// functions for RPC calls
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}
func (rf *Raft) sendRequestVote1(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote1", args, reply)
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
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
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
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
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
// for any long-running work
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	// Initialise raft struct members
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.currentTerm = 0 
	rf.votedFor=-1
	rf.log= make([]*LogEntry,10)
	rf.nextIndex = make([]int,len(peers))
	rf.matchIndex = make([]int, len(peers))
	rf.commitIndex=0
	rf.lastApplied=0
	rf.lastLogTerm=0
	rf.lastLogIndex=0
	rf.electiontimeout=time.Duration(rand.Intn(250)+500)
	rf.heartbeattimeout=100
	rf.isLeader=false
	rf.isFollower = true
	rf.isCandidate = false
	// Your initialization code here (2A, 2B, 2C).
	rf.election_tick = time.NewTicker(time.Millisecond* rf.electiontimeout)
	rf.heartbeat_tick = time.NewTicker(time.Millisecond* rf.heartbeattimeout)
    if(rf.log[1] == nil){
    	fmt.Printf("Nil argument")
    }
    // Go routine which runs constantly checking for election and heartbeat timeouts
	go func() {
    for {
    	select{
    		case <-rf.election_tick.C :{
    			var temp time.Duration
    			temp = time.Duration(rand.Intn(250)+500)
    			rf.election_tick = time.NewTicker(time.Millisecond*temp)
    			// In case of election timeout
    			if(rf.isFollower == true || rf.isCandidate == true){
    				rf.isFollower = false
    				rf.isCandidate = true
    				// increment term and contest for elections
    				rf.currentTerm = rf.currentTerm + 1

    				// initialise the RequestVote args and reply struct
    				var Arguments RequestVoteArgs
    				Arguments.Term = rf.currentTerm
    				Arguments.CandidateId = rf.me
    				Arguments.LastLogIndex = rf.lastLogIndex
    				Arguments.LastLogTerm = rf.lastLogTerm
    				var Reply RequestVoteReply
    				var vote_count = 0
    				rf.votedFor = rf.me
    				// candidate votes for itself
    				vote_count++
    				var no_of_peers=len(rf.peers)
    				fmt.Printf("%d\n",no_of_peers)
    				for i:=0; i<no_of_peers;i++{
    					if(i!=rf.me){
    						if(rf.sendRequestVote1(i,&Arguments,&Reply)) {
    							//fmt.Printf("Sending Request Vote %d\n",i)
    							// When candidate's term is lesser than receiver's term
    							if(Reply.Term > rf.currentTerm){
    								rf.currentTerm = Reply.Term
    								//check once
    								rf.isFollower=true
    								rf.isCandidate=false
    								rf.isLeader=false
    							}
    							if(Reply.VoteGranted == true){
    								vote_count++;
    							}
    							fmt.Printf("vote count %d of %d\n",vote_count,rf.me)
    						}
    					}
    				}
    				// find out who the winner is , by taking majority vote
    				if(vote_count > no_of_peers/2){
    					rf.isLeader = true
    					fmt.Printf("%t\n",rf.isLeader)
    					for i:=0; i<no_of_peers;i++{
    						rf.nextIndex[i]=rf.lastLogIndex+1
    						fmt.Printf("Next index %d\n",rf.nextIndex[i])
    						rf.matchIndex[i]=0
    				}

    				}
    				
    			}
    			
    		}   
    		case <-rf.heartbeat_tick.C : {
    			// Heartbeat timeout reached, leader sends heartbeats to all peers
    			if(rf.isLeader == true){
    				fmt.Printf("heartbeat sent %d",rf.me)
    				// Initialise AppendEntries args and reply struct
    				var Arguments1 AppendEntriesArgs
    				Arguments1.Term=rf.currentTerm
    				Arguments1.LeaderId=rf.me
    				Arguments1.PrevLogIndex=rf.lastLogIndex
    				Arguments1.LeaderCommit=rf.commitIndex
    				//	Arguments1.Entries=make([]*LogEntry,10)
    				//  fmt.Printf("Leader commit %d\n",Arguments1.LeaderCommit)
    				var Reply1 AppendEntriesReply
    				var no_of_peers=len(rf.peers)
    				for i:=0; i<no_of_peers;i++{
    				if(i!=rf.me){
    				for ok:=false;!ok; {
    				fmt.Printf("sending append entries")
    				ok=rf.sendAppendEntries(i,&Arguments1,&Reply1)
    				fmt.Printf("append entries sent")
    				}
    				// if term of leader is less than receiver's term, change leader state to follower
    				if(Reply1.Term>rf.currentTerm){
    					rf.isLeader=false
    					rf.isFollower=true
    				}
    			}
    		} 				
    	}
    }
}
}
}()
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	return rf
}

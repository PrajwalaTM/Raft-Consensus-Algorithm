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
const (
HBINTERVAL = 50* time.Millisecond
)


type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

// struct to store log entries
type LogEntry struct {
	Term int
	Index int
	Command interface{}  
}

type Raft struct {

	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int 				// this peer's index into peers[]  

	//Persistent state
	currentTerm int
	votedFor	int
	log 		[]LogEntry

	//Volatile state of all servers
	commitIndex int
	lastApplied int

	//Leader state 
	nextIndex  []int
	matchIndex []int

	//electiontimeout time.Duration 
	//heartbeattimeout time.Duration 
	
	// to store states
	isLeader bool
	isCandidate bool
	isFollower bool
	
	//Channels
	chanApply chan ApplyMsg
	chanLeader chan bool
	chanCommit chan bool
	chanAppendEntry chan bool
	chanGrantVote chan bool

	vote_count int
	//election_tick *time.Ticker //ticker for election time-out of each Raft peer
	//heartbeat_tick *time.Ticker // ticker for heartbeat timeouts
	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
		
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

// Arguments for AppendEntriesRPC

type AppendEntriesArgs struct {
	Term int
	LeaderId int
	PrevLogIndex int
	PrevLogTerm int
	Entries []LogEntry
	LeaderCommit int
}

//Arguments for AppendEntriesReply

type AppendEntriesReply struct {
	Term int
	Success bool
	NextIndex int
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

func (rf *Raft) GetLastIndex() (int) {
	return rf.log[len(rf.log)-1].Index
}

func (rf *Raft) GetLastTerm()  (int) {
	return rf.log[len(rf.log)-1].Term
}
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	
	// Your code here (2A, 2B).
	//fmt.Printf("Term of args%d\n",args.Term)
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// ignore candidate's request if candidate's term is lesser than receiver's term
	if (args.Term<rf.currentTerm) {
		reply.VoteGranted=false
		reply.Term=rf.currentTerm
		return 
	} 

	reply.Term = rf.currentTerm
	if (args.Term > rf.currentTerm) {
		rf.currentTerm =args.Term
		rf.isLeader = false
		rf.isCandidate = false
		rf.isFollower = true
		rf.votedFor = -1
	} 
	term := rf.GetLastTerm()
	index := rf.GetLastIndex()
	//fmt.Printf("Entered RequestVote")
	if((rf.votedFor==-1 || rf.votedFor==args.CandidateId)&&(term<args.LastLogIndex || (term==args.LastLogTerm && index<=args.LastLogIndex))){
		// Vote if all conditions are satisfied and receiver hasn't voted for any other candidate
		//Resetting the timer if it is about to grant the vote
		//rf.election_tick.Stop()
		//var temp time.Duration
    	//temp = (rand.Intn(250)+500)
    	//rf.election_tick = time.NewTicker(time.Millisecond*temp) 
    	
		fmt.Printf("Vote sent %d\n",rf.me)
		rf.votedFor = args.CandidateId
		reply.VoteGranted=true
		rf.isLeader = false
		rf.isCandidate = false
		rf.isFollower = true
		rf.chanGrantVote <- true
//		rf.currentTerm=int(math.Max(float64(rf.currentTerm),float64(args.Term)))
	//	reply.Term=rf.currentTerm
	} else{
		reply.VoteGranted=false
		reply.Term=rf.currentTerm
	}
	
}
// AppendEntries RPC definition
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {

	rf.mu.Lock()
	defer rf.mu.Unlock()
	//Resetting the timer if append entries is received
	//rf.election_tick.Stop()
	//var temp time.Duration
    //temp = time.Duration(rand.Intn(250)+500)

    reply.Success=false
    if (args.Term<rf.currentTerm) {
		reply.Term=rf.currentTerm
		reply.NextIndex=rf.GetLastIndex()+1
		return 
	} 
	rf.chanAppendEntry <-true
	if (args.Term > rf.currentTerm) {
		rf.currentTerm =args.Term
		rf.isLeader = false
		rf.isCandidate = false
		rf.isFollower = true
		rf.votedFor = -1
	}

    //rf.election_tick = time.NewTicker(time.Millisecond*temp) 
    //fmt.Printf("checkpoint A\n")
   // fmt.Printf("LeaderCommit %d commitIndex",args.Term)
   
   	reply.Term=args.Term 

   	if(args.PrevLogIndex>rf.GetLastIndex()) {
   		reply.NextIndex=rf.GetLastIndex()+1
   		return
   	}
   	baseIndex := rf.log[0].Index
    
    if (args.PrevLogIndex > baseIndex) {
    	term := rf.log[args.PrevLogIndex-baseIndex].Term
    	if(args.PrevLogTerm!=term){
    		for i:=args.PrevLogIndex-1;i>=baseIndex;i-- {
    			if(rf.log[i-baseIndex].Term!=term){
    				reply.NextIndex=i+1
    				break
    			}
    		}
    		return
    	}
    }

    if(args.PrevLogIndex<baseIndex){

    }else
    {
    	rf.log=rf.log[:args.PrevLogIndex+1-baseIndex]
    	rf.log=append(rf.log,args.Entries...)
    	reply.Success=true
    	reply.NextIndex=rf.GetLastIndex()+1
    }
    //update the commit index to the minimum of leader's and receiving peer's commit index
   
    if(args.LeaderCommit>rf.commitIndex) {
    	// fmt.Printf("checkpoint A'")
		rf.commitIndex=int(math.Min(float64(args.LeaderCommit),float64(rf.GetLastIndex())))
		rf.chanCommit<-true
	}

	return
}
// functions for RPC calls
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//fmt.Printf("inside SRV")
	if ok {
		term := rf.currentTerm
		if(rf.isCandidate==false){
		return !ok
	}
	if(args.Term!=term){
		return !ok
	}
	if reply.Term > term {
		rf.currentTerm=reply.Term
		rf.isFollower=true
		rf.isCandidate=false
		rf.isLeader=false
		rf.votedFor=-1
	}
	if(reply.VoteGranted==true){
		rf.vote_count++;
		if(rf.isCandidate==true && rf.vote_count>len(rf.peers)/2){
			rf.chanLeader<-true
		}
	}
	
}
return ok
}
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if ok {
		if rf.isLeader== false {
			return ok
		}
		if args.Term!=rf.currentTerm {
			return ok
		}
	
	if reply.Term > rf.currentTerm {
		rf.currentTerm=reply.Term
		rf.isFollower=true
		rf.isLeader=false
		rf.isCandidate=false
		rf.votedFor=-1
		return ok
	}
	if reply.Success {
		if(len(args.Entries)>0) {
			//rf.nextIndex[server] = args.Entries[len(args.Entries) - 1].Index + 1
			rf.nextIndex[server]=reply.NextIndex			
			rf.matchIndex[server]=rf.nextIndex[server]-1
		}
	}else{
			rf.nextIndex[server]=reply.NextIndex
		}
	}
	return ok
}



//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.

func (rf* Raft) broadcastRequestArgs() {
	rf.mu.Lock()
	var Arguments RequestVoteArgs
    Arguments.Term = rf.currentTerm
    Arguments.CandidateId = rf.me
    Arguments.LastLogIndex = rf.GetLastIndex()
    Arguments.LastLogTerm = rf.GetLastTerm()
    rf.mu.Unlock()
    var no_of_peers=len(rf.peers)
    //fmt.Printf("%d\n",no_of_peers)
    for i:=0; i<no_of_peers;i++{
    				if(i!=rf.me && rf.isCandidate==true){
    						go func(i int) {
    						var Reply RequestVoteReply
    						rf.sendRequestVote(i,&Arguments,&Reply)
    						}(i)
    					}
    				}
   
}

func (rf *Raft) broadcastAppendEntriesargs() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	last := rf.GetLastIndex()
	N:= rf.commitIndex
	baseIndex := rf.log[0].Index
	for i:= rf.commitIndex+1; i<=last; i++ {
		peers_committed:=1
		for j:= range rf.peers {
			if(rf.matchIndex[j]>=i && j!=rf.me && rf.log[i-baseIndex].Term==rf.currentTerm){
				peers_committed++
			}
		}
		if(peers_committed>len(rf.peers)/2){
			N=i
		}
	}
	if N!=rf.commitIndex {
		rf.commitIndex=N
		rf.chanCommit <-true
	}
	for i:=range(rf.peers) {
		if(i!=rf.me && rf.isLeader==true){
			if rf.nextIndex[i]>baseIndex {
			var Arguments AppendEntriesArgs
			Arguments.Term=rf.currentTerm
    				Arguments.LeaderId=rf.me
    				Arguments.PrevLogIndex=rf.nextIndex[i] - 1
    				Arguments.PrevLogTerm=rf.log[Arguments.PrevLogIndex-baseIndex].Term
    				Arguments.Entries=make([]LogEntry,len(rf.log[Arguments.PrevLogIndex+1-baseIndex:]))
    				copy(Arguments.Entries,rf.log[Arguments.PrevLogIndex+1-baseIndex:])
    				Arguments.LeaderCommit=rf.commitIndex
    				go func(i int,args AppendEntriesArgs){
    					var Reply AppendEntriesReply
    					rf.sendAppendEntries(i,&args,&Reply)
    				}(i,Arguments)
		}
	}
	}
}
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
	rf.mu.Lock()
	defer rf.mu.Unlock()
	index := -1
	term := rf.currentTerm
	isLeader := rf.isLeader
	// Your code here (2B).
	if isLeader {
		index = rf.GetLastIndex()+1
		rf.log = append(rf.log, LogEntry{Term:term,Command:command,Index:index})
	}
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
	rf.log= append(rf.log,LogEntry{Term:0})
	//rf.nextIndex = make([]int,len(peers))
	//rf.matchIndex = make([]int, len(peers))
	rf.commitIndex=0
	rf.lastApplied=0
	//rf.electiontimeout=time.Duration(rand.Intn(250)+500)
	//rf.heartbeattimeout=100
	rf.isLeader=false
	rf.isFollower = true
	rf.isCandidate = false

	rf.chanCommit=make(chan bool,100)
	rf.chanAppendEntry=make(chan bool,100)
	rf.chanGrantVote=make(chan bool,100)
	rf.chanLeader=make(chan bool,100)
	rf.chanApply=applyCh

	// Your initialization code here (2A, 2B, 2C).
	//rf.election_tick = time.NewTicker(time.Millisecond* rf.electiontimeout)
	//rf.heartbeat_tick = time.NewTicker(time.Millisecond* rf.heartbeattimeout)
   // if(rf.log[1] == nil){
    //	fmt.Printf("Nil argument")
    //}
    // initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
    // Go routine which runs constantly checking for election and heartbeat timeouts
	go func() {
    for {
    	if(rf.isLeader==true){
    		rf.broadcastAppendEntriesargs()
    		time.Sleep(HBINTERVAL)
    	} else if (rf.isFollower==true){
    		select {
    		case <-rf.chanAppendEntry:
    		case <- rf.chanGrantVote:
    		case <-time.After(time.Duration(rand.Int63()%250 + 550)*time.Millisecond):
    			rf.isCandidate=true
    			rf.isLeader=false
    			rf.isFollower=false
    		} 
    	}else if (rf.isCandidate==true){
    		rf.mu.Lock()
    		rf.currentTerm++
    		rf.votedFor=rf.me
    		rf.vote_count=1
    		rf.mu.Unlock()
    		go rf.broadcastRequestArgs()
    			select{
    			case <-time.After(time.Duration(rand.Int63()%250+550)*time.Millisecond):
    			case <-rf.chanAppendEntry:
    				rf.isCandidate=false
    				rf.isLeader=false
    				rf.isFollower=true
    			case <- rf.chanLeader:
    				rf.mu.Lock()
    				rf.isCandidate=false
    				rf.isLeader=true
    				rf.isFollower=false
    				rf.nextIndex = make([]int,len(rf.peers))
					rf.matchIndex = make([]int, len(rf.peers))
					for i:= range(rf.peers){
						rf.nextIndex[i]=rf.GetLastIndex()+1
						rf.matchIndex[i]=0
					}
					rf.mu.Unlock()
    			}
    	}
    }
    	}()
	go func() {
		for {
			select {
			case <-rf.chanCommit:
				rf.mu.Lock()
				commitIndex := rf.commitIndex
				baseIndex:= rf.log[0].Index
				for i:= rf.lastApplied+1;i<=commitIndex;i++{
					msg:=ApplyMsg{Index:i,Command:rf.log[i-baseIndex].Command}
					applyCh<-msg
					rf.lastApplied=i
				}
				rf.mu.Unlock()
			}
		}
	}()
	return rf
}

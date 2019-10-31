import sys
import time
import socket
import json
import random
from threading import Thread

class State:
    def __init__(self):
        #Persistent state
        self.currentTerm = 0    #latest term server has seen
        self.votedFor = None    #candidateId that received vote in current term
        self.log = [[{},0]]           #log entries; each entry contains command for state machine, and term when entry was received by leader (first index is 1)
                                #[[{},term],[{},term]]
        #Volatile state
        self.commitIndex = 0    #index of highest log entry known to be committed 
        self.lastApplied = 0    #index of highest log entry applied to state machine

        #Volatile state on leaders
        self.nextIndex = {}     #for each server, index of the next log entry to send to that server
        self.matchIndex = {}    #for each server, index of highest log entry known to be replicated on server
 

class cluster:
    def __init__(self, port, start_port, num_ports):
        #self port
        self.port = int(port)

        #total port except self
        self.total_port = []
        for i in range(int(num_ports)):
            p = int(start_port)+i
            if p != self.port:
                self.total_port.append(p)

        #state machine storage
        self.storage = {}
        
        #cluster information
        self.win = int(num_ports)//2+1  #how mant votes to win

        #set leader ID
        if num_ports == 1:
            self.leader = self.port
        else:
            self.leader = None  

        self.role = 'F'  #F-follower C-candidate L-leader
        self.recv_AppendEntries = False #heart-beat update
        
        #raft state
        self.state = State()

    #Follower
    def Follower(self):
        timeout = random.randint(150,300)

        #while follower
        while self.role == 'F':
            #count timeout
            time.sleep(1/1000)
            timeout -= 1
            
            #if receive heartbeat, update timeout
            if self.recv_AppendEntries:
                #print("keep follower")
                timeout = random.randint(150,300)
                self.recv_AppendEntries = False

            #become candidate when timeout
            elif timeout == 0:
                #print("become candidate")
                self.leader = None
                self.state.votedFor = None
                self.state.currentTerm += 1   #Increment currentTerm
                self.role = 'C'
                self.Candidate()

    #Candidates 
    def Candidate(self):
        #vote for self
        self.state.votedFor = self.port  
        self.votes = 1
        #election timeout
        start = time.time()
        election_time = random.randint(150,300)/1000

        sock = socket.socket(socket.AF_INET,socket.SOCK_DGRAM)   #udp socket
        lastLogIndex = len(self.state.log) - 1 #get last log index
        LastLogTerm = self.state.log[-1][1] #get last log term
        
        #issues RequestVote RPCs in parallel to each of the other servers
        for p in self.total_port:
            server_address = ('localhost', p)
            content = {'type': 'RequestVote', 'payload': {'term':self.state.currentTerm, 'candidateId':self.port, 'lastLogIndex':lastLogIndex, 'LastLogTerm':LastLogTerm}}
            sock.sendto(json.dumps(content).encode('utf-8'), server_address)

        #start a thread to listen responses
        sock_recv = Thread(target=self.receive_message,args=(sock,))
        sock_recv.start()        

        
        while True:
            #wins the election if gain majority votes
            if self.votes >= self.win:
                #print('I become leader!!')
                sock.close()
                self.role = 'L'
                self.leader = self.port
                self.Leader()
                break

            #received AppendEntriesRPC, turn to follower
            elif self.role == 'F':
                sock.close()
                #print("follower: %d %d" %(self.port, self.state.currentTerm))
                self.Follower()
                break

            #Timeout, retry
            elif (time.time() - start) >= election_time:
                sock.close()
                self.state.currentTerm += 1
                self.state.votedFor = None
                #print("candidate: %d %d" %(self.port, self.state.currentTerm))
                self.Candidate()
                break

    #receive votes from server
    def receive_message(self,sock):
        sock.settimeout(140/1000) #set socket timeout 140ms

        try:
            for _ in range(len(self.total_port)):
                data, server = sock.recvfrom(4096)
                data = json.loads(data)
                payload = data['payload']
                if payload['voteGranted'] == True:
                    self.votes += 1
        except:
            sock.close()
            #print("receive vote timeout")

         
    #leader
    def Leader(self):
        #count down timeout
        self.lead_start = time.time()

        #initialize nextIndex for servers. initialized to leader last log index + 1
        for p in self.total_port:
            self.state.nextIndex[p] = len(self.state.log)

        #initialize matchIndex for servers
        for p in self.total_port:
            self.state.matchIndex[p] = 0

        #keep sending out heart beats
        while True:
            #if receive a higher term RPC, convert to follower     
            if self.role == 'F':
                #print("turn to follower")
                self.Follower()
                break

            if (time.time() - self.lead_start) >= 0.12:  #120ms resend heart-beat
                #print("send heart-beats")
                #issues heart-beats RPCs in parallel to each of the other servers
                for p in self.total_port:
                    thread = Thread(target=self.Send_nodes, args=(p,))
                    thread.start()

                self.lead_start = time.time()   #update timeout


            #If there exists an N such that N > commitIndex
            if len(self.state.log) - 1 > self.state.commitIndex:
                N = self.state.commitIndex + 1
        
                larger = 1
                #a majority of matchIndex[i] ≥ N
                for p in self.total_port:
                    if self.state.matchIndex[p] >= N: 
                        larger += 1
                
                if larger >= self.win:
                    self.state.log[N][1] = self.state.currentTerm   # log[N].term == currentTerm
                    self.state.commitIndex = N  #set commitIndex = N

                    self.Update() #applied to state machine

    #update log into state machine
    def Update(self):
        while self.state.commitIndex > self.state.lastApplied:
            self.state.lastApplied += 1
            KV = self.state.log[self.state.lastApplied][0]
            for key, val in KV.items():
                self.storage[key] = val


    #send an AppendEntries RPC to a specific port
    def Send_nodes(self, port):
        sock = socket.socket(socket.AF_INET,socket.SOCK_DGRAM)
        address = ('localhost', port) #get address
    
        #parameters need to send out
        prevLogIndex = self.state.nextIndex[port] - 1
        prevLogTerm = self.state.log[prevLogIndex][1]
        entries = self.state.log[prevLogIndex + 1 :]  #will be empty [] if nextIndex reaches the end of leader's log (heartbeats send an empty entry)

        content = {'type': 'AppendEntries', 'payload': {'term':self.state.currentTerm, 'leaderId':self.port, 'prevLogIndex':prevLogIndex, 'prevLogTerm':prevLogTerm, 'entries':entries, 'leaderCommit':self.state.commitIndex}}

        try:
            #send out
            sock.sendto(json.dumps(content).encode('utf-8'), address)

            sock.settimeout(0.1) #100ms timeout
            data, server = sock.recvfrom(4096) #receive

            #load data into json
            data = json.loads(data)
            payload = data['payload']

            #check response 
            if payload['success'] == True:
                sock.close()
                #print("return true")
                self.state.matchIndex[port] = len(self.state.log)-1   #if return true, all the current log entries would be added to the server's log
                self.state.nextIndex[port] = self.state.matchIndex[port] + 1 
                
            elif payload['success'] == False:
                sock.close()
                #print("return false")
                self.state.nextIndex[port] -= 1  #decrement the nextindex and retry
                if self.role == 'L': #if it is still the leader
                    self.Send_nodes(port)
        
        except socket.timeout:
            sock.close()
            #print("socket time out")
            

    #Invoked by candidates to gather votes
    def RequestVoteRPC(self, term, candidateId, lastLogIndex, LastLogTerm):
        voteGranted = False #true means candidate received vote
        higher_term = False #if RPC has a higher term
        
        #Reply false if term < currentTerm
        if term < self.state.currentTerm:
            return voteGranted
        #update term if receive higher term
        if term > self.state.currentTerm:
            higher_term = True
            self.state.votedFor = None
            self.state.currentTerm = term

        #up-to-date
        up_to_date = False
        
        #compare the index and term of the last entries in the logs
        server_last_term = self.state.log[-1][1]
        if LastLogTerm > server_last_term:
            up_to_date = True
        elif LastLogTerm == server_last_term and lastLogIndex >= self.state.commitIndex:
            up_to_date = True

        #If votedFor is null or candidateId, and candidate’s log is at least as up-to-date as receiver’s log
        if (self.state.votedFor == None or self.state.votedFor == candidateId) and up_to_date:
            self.state.votedFor = candidateId
            voteGranted = True #grant vote

            #If a candidate or leader discovers that its term is out of date, it immediately reverts to follower state.
            if higher_term:
                self.role = 'F'
            return voteGranted

        return voteGranted #FALSE


    #Invoked by leader to replicate log entries; also used as heartbeat
    def AppendEntriesRPC(self, term, leaderID, prevLogIndex, prevLogTerm, entries, leaderCommit):
        #true if follower contained entry matching prevLogIndex and prevLogTerm
        success = False
        
        #Reply false if term < currentTerm
        if term < self.state.currentTerm:
            return success      
        #update term if receive higher term
        if term > self.state.currentTerm:
            self.state.votedFor = None
            self.state.currentTerm = term
        
        self.role = 'F' #update to follower
        self.leader = leaderID #update leaderID
        
        #reply false if log index doesn't exist
        if prevLogIndex >= len(self.state.log):
            return success

        #Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm
        if self.state.log[prevLogIndex][1] != prevLogTerm:            
            #If an existing entry conflicts with a new one (same index but different terms), delete the existing entry and all that follow it
            self.state.log = self.state.log[:prevLogIndex]
            return success

        #Append any new entries not already in the log
        self.state.log += entries

        #If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
        if leaderCommit > self.state.commitIndex:
            self.state.commitIndex = min(leaderCommit, len(self.state.log)-1)
            self.Update()

        success = True
        return success

    #put reuqest from client
    def PUT(self,data):
        try:
            #analyze input and store data
            KV = data['payload']
            key = KV['key']
            value = KV['value']

            #update log
            self.state.log.append([{key:value}, self.state.currentTerm])

            #send AppendEntries
            #print("send AppendEntries")
            self.lead_start = time.time()
            self.send_AppendEntries()

            #return success
            return {'code':'success'}
        except:
            #return fail if can't store
            return {'code': 'fail'}

    #issues AppendEntries RPCs in parallel to each of the other servers
    def send_AppendEntries(self):       
        for p in self.total_port:
            thread = Thread(target=self.Send_nodes, args=(p,))
            thread.start()

        #the leader applies the entry to its state machine and returns the result of that execution to the client
        #wait for line 190 to finish
        while True:
            if self.state.commitIndex == len(self.state.log) -1:
                break

    #get data from state machine
    def GET(self,data):
        payload = data['payload']
        key = payload['key']
        if key in self.storage:
            return {'code':'success', 'payload':{'key':key, 'value':self.storage[key]}}
        else:
            return {'code': 'fail'}


    #udp socket used within cluster to transform messages
    def Cluster_serve(self):
        # Create UDP socket
        sock = socket.socket(socket.AF_INET,socket.SOCK_DGRAM)
        # Bind UDP socket to local port
        sock.bind(('', self.port))

        while True:
            data, address = sock.recvfrom(4096)
            data = json.loads(data)
            
            if data['type'] == 'RequestVote':
                #print("hear RequestVote")
                payload = data['payload']
                out = self.RequestVoteRPC(payload['term'], payload['candidateId'], payload['lastLogIndex'], payload['LastLogTerm'])
                content = {'type': 'RequestVoteReutrn', 'payload': {'voteGranted':out}}
                sock.sendto(json.dumps(content).encode('utf-8'), address)
            
            elif data['type'] == 'AppendEntries':
                #print("hear AppendEntries")
                payload = data['payload']

                #update status
                self.recv_AppendEntries = True

                #AppendEntriesRPC
                out = self.AppendEntriesRPC(payload['term'], payload['leaderId'], payload['prevLogIndex'], payload['prevLogTerm'], payload['entries'], payload['leaderCommit'])
                content = {'type': 'AppendEntriesReturn', 'payload': {'success':out}}
                sock.sendto(json.dumps(content).encode('utf-8'), address)
                #print("send back AppendEntries")

            #decide request type
            elif data['type'] == 'put':
                if self.leader != self.port:
                    content = {'type': 'redirect', 'payload': {'port':self.leader}}
                else:
                    #print("receive put")
                    content = self.PUT(data)
                #print("send back put")
                sock.sendto(json.dumps(content).encode('utf-8'), address)
            
            elif data['type'] == 'get':
                if self.leader != self.port:
                    content = {'type': 'redirect', 'payload': {'port':self.leader}}
                else:
                    content = self.GET(data)
                sock.sendto(json.dumps(content).encode('utf-8'), address)
            
            else: #if wrong request
                content = {'code': 'fail'}
                sock.sendto(json.dumps(content).encode('utf-8'), address)
       

if __name__ == "__main__":
    #python udp_server.py <port> <start port> <num of servers>
    port = sys.argv[1] 
    start_port = sys.argv[2]
    num_ports = sys.argv[3]

    #initialize
    c = cluster(port, start_port, num_ports)
    
    #build cluster sockets
    cluster_thread = Thread(target=c.Cluster_serve)
    cluster_thread.start()
    
    #start with follower
    c.Follower()   

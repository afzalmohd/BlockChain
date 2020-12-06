#!/usr/bin/env python
# coding: utf-8

# In[ ]:

import csv
from datetime import datetime
import socket
import threading
import select
import sys
import random
from time import sleep
import copy
import logging
from queue import Queue
import sys
import hashlib
import time
import numpy as np



class Peer:
    def __init__(self,ip,port=6464,csv_file_path='config.csv',log_file="peer_log.log",intarrivaltime=20,hashing_power = 33):
        
        logging.basicConfig(filename=log_file, level=logging.INFO)
        self.csv_file_path=csv_file_path
        self.MessageList= {} #hash(msg) as key and (fromip,to_ips) ex {hash(msg):(fromip,[toip1,toip2])}
        self.seed_connection={} #incoming connection list and seed
        self.out_connection_peer_list={} #exclusive for peer
        self.in_connection_peer_list={}
        self.requested_peer_list=[] #list of all requested peer list
        self.incoming_peer_list={} #incoming registered peers
        self.alive_timer={} #peer node timer
        self.ip=ip #self ip
        self.port=port #self port
        self.working=False
        self.gossip_msg_number=0
        self.thread_list=[]
        self.bc = [] #blockchain tree as list of list
        self.my_block_list = []
        self.pending_queue = Queue()
        self.genesis_hash = '0x9e1c'
        self.merkel_root = 'abcd'
        self.intarrivaltime = intarrivaltime
        self.hashing_power = hashing_power
        self.global_lamda = 1.0/self.intarrivaltime
        self.lamda = self.hashing_power*self.global_lamda/100.0
        print('lamda paremeter: {}'.format(self.lamda))
        self.mining_time_bound = time.time() + self.waiting_time()
        self.socket_sock=self.bind_sock() #binds itself to the ip and port
        
    
    def kill_node(self): #done
        try:
            sys.exit()
        except:
            pass
    
    def stop_node(self): #done
        self.close_all_connections()
        if self.socket_sock==None:
            print("the node is not bind to any ip")
        else:
            self.socket_sock.close()
            self.socket_sock=None
            print('closed the socket')
            self.working=False
            self.kill_node()
    
    def close_all_connections(self): #done
        for i in self.seed_connection:
            try:
                self.seed_connection[i].close()
            except:
                pass
        self.seed_connection={}
        
   
    def bind_sock(self): #done
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM) #initialize socket
            sock.bind((self.ip,int(self.port)))
            sock.settimeout(50)
            print("current node is bind to {}:{}".format(self.ip,self.port))
            self.working=True
            return sock
        except Exception as e:
            print("binding to {}:{} has failed".format(self.ip,self.port))
            self.working=False
            print("exiting the node...")
            self.kill_node()
            return None
      

            
    def read_seed_ip(self): #done
        csv_file=open(self.csv_file_path)
        csv_reader = csv.reader(csv_file, delimiter='\n')
        ip_port=[]
        for row in csv_reader:
            ip_port.append(row[0].split(":"))
        return ip_port

    
    def connect_to_config(self): #done implementing add request peer list connect to 4 peers
        #choosing first(randomly) floor(n/2)+1 seeds and connecting with them
        ip_list=self.read_seed_ip()
        random.shuffle(ip_list) #randomize ip_list
        len_ip_list=len(ip_list)
        
        for i in ip_list[:int(len_ip_list/2)+1]: 
            self.connect(i[0],int(i[1]),node_type='seed') #connected to seeds & sent register msgs and saved connections in peer connection list
        
        sleep(5)
        self.request_peer_list_from_seed() #req peer lists from all the seeds from connection list and saves it in requested_pl
                                           #['131#31,313#13,31#31,31#31','464#54,54#5,5#4,5#54,554#5'] this form
        if len(self.requested_peer_list)<=0:
            print('there is no peers to connect..')
        else:
            all_conn=','.join(self.requested_peer_list)
            req_peers=[]
            for i in all_conn.split(','):
                kkk=i.split('#')
                req_peers.append((kkk[0],int(kkk[1])))
                
            sublist = set(req_peers)
            req_peers = list(sublist)
        
            random.shuffle(req_peers) #shuffle all the req_peers
            if (self.ip,int(self.port)) in req_peers: req_peers.remove((self.ip,int(self.port))) #if someone sent me my peer :X
            log='The list of peers received from seeds.. : \n '
            print(log)
            logging.info(log)
            
            for i in req_peers: #writing all the received peers list into log file
                print(str(i)+'\n')
                logging.info(str(i)+'\n')
                
        
            for i in req_peers[:4]:  #connect to random 4 peers form requested list and set alive timer to 3
                self.connect(i[0],i[1],node_type='peer')
                
            
            #start gossip message and check alive timers
            #self.Start_counters()
            

    def request_peer_list_from_seed(self): #done
        msg=self.generate_message(msg_type='plrq')
        conlist=list(self.seed_connection.keys())[:]
        print('plrq msg: {}, conn list length: {}'.format(msg,len(conlist)))
        for i in conlist:
            self.send_msg(i,msg)
            sleep(1)
        sleep(10)
        
    def connect(self,ip,port,node_type='Seed'): #done
        try:
            conn=socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            conn.connect((ip, int(port)))
            conn.setblocking(False)
            
            if node_type.lower()=='peer':
                self.out_connection_peer_list[(ip,int(port))]=conn
                self.alive_timer[(ip,int(port))]=3
            else:
                self.seed_connection[(ip,int(port))]=conn
                regmsg=self.generate_message(msg_type='regi') #register msg generation 
                conn.send(regmsg.encode()) #sending register message Register:ip#port
                
            print("Connected to {} node ip: {} - port: {}".format(node_type, ip, port))
            
        except Exception as e:
            print("Failed to connect to {} (ip: {} - port: {})".format(node_type,ip,port))
        
    def send_msg(self,conn_key, msg): #done #send to both peer and seeds
        try:
            if conn_key in self.out_connection_peer_list :
                print('Inside peer connection')
                conn=self.out_connection_peer_list[conn_key]
            else:
                conn=self.seed_connection[conn_key]
                
            conn.send(msg.encode())
            sleep(10)
            
            #receive data from connected Seed
            print('Before recived')
            data = conn.recv(102476) 
            print('After recived: {}'.format(data.decode("utf-8")))
            sleep(5)
            self.message_reactor(rcvd_msg=data) 
            sleep(5)
            
        except Exception as e:
            print(e)
#             print("in send_msg")
            pass
    
    
    
    def longest_chain(self):
        size = len(self.bc)
        longest_chains = []
        if size == 0:
            return longest_chains
        last_label_nodes = self.bc[size-1]
        
        
        
        for leaf in last_label_nodes:
            chain = [leaf]
            i = size-2
            new_leaf = leaf
            while i >= 0:
                for node in self.bc[i]:
                    if self.get_prev_hash(new_leaf) == self.my_hash(node):
                        new_leaf = node
                        chain.append(new_leaf)
                        break;
                i -= 1
                
            longest_chains.append(list(reversed(chain)))
        #print('longest chain: {}'.format(longest_chains))
        if len(longest_chains) > 0:        
            selected_one = random.sample(longest_chains,1)
            selected_one = selected_one[0]
        else:
            selected_one = []
            
        return selected_one
    
    def validate_rcvd_bc(self,rcvd_bc):
        rev_bc = list(reversed(rcvd_bc))
        prev_block = rev_bc[0]
        for block in rev_bc[1:]:
            if self.get_prev_hash(prev_block) == self.my_hash(block):
                prev_block=block
            else:
                return False
            
        if self.get_prev_hash(rev_bc[-1]) != self.genesis_hash:
            return False
        
        return True
    
    def add_block_in_bc(self,block,pos):
        if pos == -1:
            if len(self.bc) == 0:
                self.bc.append([block])
            else:
                self.bc[pos+1].append(block)
                
        elif len(self.bc)-1 > pos:
            self.bc[pos+1].append(block)
        else:
            new_list = [block]
            self.bc.append(new_list)
    
    def is_valid_block(self,block):
        block_splt = block.split('#')
        block_ts = block_splt[2]
        ts = time.time()
        if not(ts-3600 <= int(block_ts) and int(block_ts) <= ts+3600):
            return False,None
        
        if self.get_prev_hash(block) == self.genesis_hash:
            return True,-1
        
        i = len(self.bc) - 1
        while i >= 0:
            for node in self.bc[i]:
                if self.get_prev_hash(block) == self.my_hash(node):
                    return True,i
        
        return False,None
    
    def gen_block(self,prev_block=None):
        if prev_block == None:  #first block
            prev_hash = self.genesis_hash
            ts = int(time.time())
            return str(prev_hash)+'#'+self.merkel_root+'#'+str(ts)
            
        prev_hash = self.my_hash(prev_block)
        ts = int(time.time())
        return str(prev_hash)+'#'+self.merkel_root+'#'+str(ts)
        
                           
                
                    
    def my_hash(self,block):
        hash_obj = hashlib.shake_256() # In assignment they recommend to use SHA-256, but is gives 256 bit output
        hash_obj.update(block.encode())
        hash_val = hash_obj.hexdigest(2)
        
        return hash_val
        
        
    def get_prev_hash(self,block):
        spt_block = block.split('#')  #block format is 'block'#prev_hash#merkel_root#ts
        hash_val = spt_block[1]
        return hash_val
    
    def waiting_time(self):
        ev = np.random.exponential()/self.lamda
        print('waiting time: {}'.format(ev))
        return ev
    
    
    def get_bc(self):
        out_conn_list = list(self.out_connection_peer_list.values())
        if len(out_conn_list) >= 2:
            random_peer_conns = random.sample(out_conn_list,2)
        else:
            random_peer_conns = out_conn_list
            
        
        msg='rfbk'
        bk_list = []
        use_full_conn = []
        for conn in random_peer_conns:
            conn.send(msg.encode())
            sleep(10)
            rcvd_msg = conn.recv(102476)
            bk = rcvd_msg.decode()
            print('recieved bk: {}'.format(bk))
            if bk != 'empty':
                print('Zero block recieved!!')
                bk_list.append(rcvd_msg.decode())
                use_full_conn.append(conn)
            sleep(2)
            
        bc_list = []
        i=0
        for conn in use_full_conn:
            msg = 'rfbc'+':'+str(bk_list[i])
            conn.send(msg.encode())
            sleep(20)
            rcvd_msg = conn.recv(102476)
            bc_list.append(rcvd_msg.decode())
            i += 1
            sleep(2)
            
        chains_len = []
        for bc in bc_list:
            if bc != '':
                chain = bc.split(';')
                chains_len.append(len(chain))
            else:
                chains_len.append(0)
                
        
        if len(chains_len) > 0:
            if max(chains_len) > 0:    # atleast one list having blockes b0---bk-1
                max_l_ind = chains_len.index(max(chains_len))
                long_chain = bc_list[max_l_ind]
                self.pending_queue.put(bk_list[max_l_ind])
                block_list = long_chain.split(';')
                
                if self.validate_rcvd_bc(block_list):
                    for block in block_list:
                        self.bc.append([block])
            
            elif len(bk_list) > 0:   #got only one block b_k
                block = bk_list[0]
                if self.validate_rcvd_bc([block]):
                    self.bc.append([block])
                    
            print('Synch with the blockchain of length: {}'.format(len(self.bc)))
            
       
        
 

       
        
        
    def server_site(self): #handle incoming connection request #done
        
        # Sockets from which we expect to read
        if self.socket_sock == None:
            print('no socket is bind')
            return
            
        self.socket_sock.listen(15)
        
        inputs = [ self.socket_sock ]

        # Sockets to which we expect to write
        outputs = [ ]
        
        # Outgoing message queues (socket:Queue)
        message_queues = {}
        
        while inputs:
            # Wait for at least one of the sockets to be ready for processing
            readable, writable, exceptional = select.select(inputs, outputs, inputs)
            
            for s in readable:
                if s is self.socket_sock:
                    conn, addr = s.accept()
                    print('New Peer connection from', addr)
                    conn.setblocking(0)
                    inputs.append(conn)  # Store newly connected peer socket
                    self.in_connection_peer_list[addr]=conn
                    message_queues[conn] = Queue()
                    
                else:
                    data = s.recv(102476)
                    self.message_reactor(data,message_queues,s,outputs)
                    
            for s in writable:
                try:
                    next_msg = message_queues[s].get_nowait()
                except:
                    # No messages waiting so stop checking for writability.
                    pass
                else:
                    print('sending %s to %s \n' % (next_msg, s.getpeername()))
                    s.send(next_msg)
                    
            for s in exceptional:
                print('handling exceptional condition for\n', s.getpeername())
                # Stop listening for input on the connection
                #inputs.remove(s)
                if s in outputs:
                    outputs.remove(s)
                s.close()

                # Remove message queue
                del message_queues[s]
                
        
    def update_q_output(self,conn,outputs,message_q=None,msg=None):
        if msg is not None:
            if conn in message_q:
                message_q[conn].put(msg.encode())
            else:
                message_q[conn] = Queue()
                message_q[conn].put(msg.encode())     
        if conn not in outputs:
            outputs.append(conn)
    
    def message_reactor(self,rcvd_msg,message_q=None,s=None,outputs=None): #done
        split_msg=rcvd_msg.decode("utf-8").split(':')  #added sap
#         print('split msg is {}'.format(split_msg))        
        
        if split_msg[0]== 'Peer List Reply':  #gives the available list of connected peers
            self.requested_peer_list.append(split_msg[1])
            
        elif split_msg[0] == 'rfbk':
            print('Request for bk')
            long_chain = self.longest_chain()
            if len(long_chain) == 0:
                msg = 'empty'
            else:
                msg = str(long_chain[-1])
            self.update_q_output(s,outputs,message_q,msg)
            
        elif split_msg[0] == 'rfbc':
            print('Request for b0 --- bk-1')
            bk = split_msg[1]
            bc_upto_k = self.get_bc_upto_k(bk)
            if len(bc_upto_k) == 0:
                msg = ''
            else:
                msg = ';'.join(bc_upto_k)
            self.update_q_output(s,outputs,message_q,msg)
            
        elif split_msg[0] == 'block':
            block = split_msg[1]
            if not self.is_block_exist(block):
                print('New block arrived: {}'.format(block))
                self.pending_queue.put(block)
            
        #else: #gossip reciept 
        #    print('invalid message_reacter: {}'.format(rcvd_msg.decode()))
        
        
        
    def generate_message(self,msg_type='goss'): #done
        msg_type=msg_type.lower()
        
        if msg_type=='plrq': #peer list Request
            return 'Peer List Request:'+str(self.ip)+"#"+str(self.port)
    
        elif msg_type=='regi': #seed register message
            return 'Register:'+str(self.ip)+'#'+str(self.port)
        
        else:
            return 'Invalid generate request'
        
        
    def get_bc_upto_k(self,bk):
        reversed_bc = list(reversed(self.bc))
        prev_block = bk
        is_matched = False
        chain = []
        for i in range(0,len(reversed_bc)):
            for block in reversed_bc[i]:
                if block == bk:
                    is_matched = True
                    break
                elif is_matched:
                    if self.get_prev_hash(prev_block) == self.my_hash(block):
                        chain.append(block)
                        prev_block = block
                        break
        return list(reversed(chain))
    
    def is_block_exist(self,recvd_block):
        for level in list(reversed(self.bc)):
            for block in level:
                if block == recvd_block:
                    return True
        
        return False
        
        
                    
                    
                    
 
                

        
















    def client_site(self):
        
        inputs = []  
        outputs = []
        
        message_queues = {}
            
        print('inserting into event loop')
        self.reset_mining_timer()
        while True:
            self.resolving_pending_queue(inputs, outputs, message_queues)
            self.creating_and_mining_block(inputs, outputs, message_queues)
    


    def handle_io_events(self,inputs,outputs,message_queues):
        readable, writable, exceptional = select.select(inputs, outputs, inputs)
            
        for s in readable:
            print('Something wrong on client site')
                    
        for s in writable:
            try:
                next_msg = message_queues[s].get_nowait()
            except:
                # No messages waiting so stop checking for writability.
                print('Exception: size of message_queue: {}, size of queue: {}'.format(len(message_queues),message_queues[s].qsize()))
                del message_queues[s]
                outputs.remove(s)
                pass
            else:
                print('Client: sending %s to %s \n' % (next_msg, s.getpeername()))
                s.send(next_msg)
                if s not in inputs:
                    inputs.append(s)
                
        for s in exceptional:
            print('handling exceptional condition for\n', s.getpeername())
            # Stop listening for input on the connection
            inputs.remove(s)
            if s in outputs:
                outputs.remove(s)
            s.close()

            # Remove message queue
            del message_queues[s]

    def update_q_output_client(self,conn,outputs,message_q=None,msg=None):
        if msg is not None:
            if conn in message_q:
                message_q[conn].put(msg.encode())
            else:
                message_q[conn] = Queue()
                message_q[conn].put(msg.encode())     
        if conn not in outputs:
            outputs.append(conn)
    
    def mining(self):
        #self.mining_time_bound = time.time() + self.waiting_time()
        
        start_time = time.time()
        while self.pending_queue.empty() and start_time < self.mining_time_bound:
            sleep(1)
            start_time += 1
            
    def reset_mining_timer(self):
        self.mining_time_bound = time.time() + self.waiting_time()
        
            
    def resolving_pending_queue(self,inputs, outputs,message_queues):
        print('resolving pending queue of size: {}'.format(self.pending_queue.qsize()))
        curr_bc_len = len(self.bc)
        while (not self.pending_queue.empty()):
            block = self.pending_queue.get_nowait()
            is_valid,pos = self.is_valid_block(block)
            if is_valid:
                self.add_block_in_bc(block,pos)
                new_bc_len = len(self.bc)
                if curr_bc_len < new_bc_len:
                    curr_bc_len = new_bc_len
                    pcl=list(self.out_connection_peer_list.values())
                    for conn in pcl : #sends blocks to all connected
                        msg = 'block:'+str(block)
                        self.update_q_output_client(conn,outputs,message_queues,msg)
                    self.handle_io_events(inputs, outputs, message_queues)
                    self.reset_mining_timer()
                        
        print('Pending queue size after resolving: {}'.format(self.pending_queue.qsize()))            
                        
    def creating_and_mining_block(self,inputs, outputs,message_queues):
        long_chain = self.longest_chain()
        self.mining()
        if self.pending_queue.empty():
            if len(long_chain) == 0:
                new_block = self.gen_block()
            else:
                new_block = self.gen_block(long_chain[-1])
                    
            self.add_block_in_bc(new_block,len(long_chain)-1)
            print('New created block added to the blockchain')
            self.my_block_list.append(new_block)
            msg = 'block:'+str(new_block)
            pcl=list(self.out_connection_peer_list.values())
            for conn in pcl : #sends blocks to all connected
                self.update_q_output_client(conn,outputs,message_queues,msg)
            self.handle_io_events(inputs, outputs, message_queues)        
            self.reset_mining_timer()
                
            
       
            
        
            
            
        
        
        
        


         
        

        
        

if __name__ == "__main__":
    ip = '127.0.0.1' # Input seed_ip here
    port = 3053
    if len(sys.argv) > 1:
        ip = sys.argv[1]
        port = int(sys.argv[2])
        
    peer = Peer(ip,port)
    server_thread = threading.Thread(target=peer.server_site, args=()) #start listening input connections
    server_thread.start()
    sleep(20)
    peer.connect_to_config() # Connect to the peers
    sleep(10)
    peer.get_bc()
    sleep(5)
    client_thread = threading.Thread(target=peer.client_site, args=()) # Strat sending gossips and liveness msgs
    client_thread.start()
    server_thread.join()
    client_thread.join()
    
    
       
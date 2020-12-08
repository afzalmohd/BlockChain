#!/usr/bin/env python
# coding: utf-8

# In[ ]:

import csv
import socket
import threading
import select
import sys
import random
from time import sleep
import logging
from queue import Queue
import hashlib
import time
import numpy as np
import matplotlib.pyplot as plt


class Peer:
    def __init__(self,ip,port=6464,csv_file_path='config.csv',log_file="peer_log.log",intarrivaltime=5,hashing_power = 33, peer_timeout=300):
        
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
        self.peer_timeout = peer_timeout
        self.gossip_msg_number=0
        self.thread_list=[]
        self.is_First_connected = False
        self.lock = threading.Lock()
        self.bc = [] #blockchain tree as list of list
        self.my_block_list = []
        self.pending_queue = []
        self.genesis_hash = '0x9e1c'
        self.merkel_root = 'abcd'
        self.intarrivaltime = intarrivaltime
        self.intarrivaltime_axis = []
        self.mpu_axix = []
        self.frac_axis = []
        self.hashing_power = hashing_power
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
            sock.bind(('',int(self.port)))
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
        
    def get_from_pending_queue(self):
        if len(self.pending_queue) > 0:
            item = self.pending_queue[0]
            del self.pending_queue[0]
            return item
        else:
            print('pending queue is empty')



    
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
        #print('plrq msg: {}, conn list length: {}'.format(msg,len(conlist)))
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
                conn=self.out_connection_peer_list[conn_key]
            else:
                conn=self.seed_connection[conn_key]
                
            conn.send(msg.encode())
            sleep(10)
            
            data = conn.recv(102476) 
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
    
                         
                
                    
    def my_hash(self,block):
        hash_obj = hashlib.shake_256() # In assignment they recommend to use SHA-256, but is gives 256 bit output
        hash_obj.update(block.encode())
        hash_val = hash_obj.hexdigest(2)
        
        return hash_val
        
        
    def get_prev_hash(self,block):
        spt_block = block.split('#')  #block format is 'block'#prev_hash#merkel_root#ts
        hash_val = spt_block[0]
        return hash_val
    
    def waiting_time(self):
        global_lamda = 1.0/self.intarrivaltime
        lamda = self.hashing_power*global_lamda/100.0
        #print('mining lamda: {}'.format(lamda))
        ev = np.random.exponential()/lamda
        print('mining time: {}'.format(ev))
        return ev
    
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
        try:
            if not(ts-3600 <= int(block_ts) and int(block_ts) <= ts+3600):
                print('Timestamp outdated current time: {}, recieved ts: {}'.format(ts,int(block_ts)))
                return False,None
            
            if self.get_prev_hash(block) == self.genesis_hash:
                return True,-1
            
            i = len(self.bc) - 1
            while i >= 0:
                for node in self.bc[i]:
                    if self.get_prev_hash(block) == self.my_hash(node):
                        return True,i
                i = i-1
            
            return False,None
        except:
            print('Some exception')
            pass
        
        return False,None
            
    
    def gen_block(self,prev_block=None):
        if prev_block == None:  #first block
            prev_hash = self.genesis_hash
            ts = int(time.time())
            return str(prev_hash)+'#'+self.merkel_root+'#'+str(ts)
            
        prev_hash = self.my_hash(prev_block)
        ts = int(time.time())
        return str(prev_hash)+'#'+self.merkel_root+'#'+str(ts)
        
  
    
    
    
    def add_multi_block_pending_queue(self,blks):
        if blks != '':
            splt = blks.split('block:')
            for block in splt[1:]:
                if block not in self.pending_queue:
                    self.lock.acquire()
                    self.pending_queue.append(block)
                    self.lock.release()
                    
                
    
    
    def get_bc(self):
        out_conn_list = list(self.out_connection_peer_list.values())
        if len(out_conn_list) == 0:
            print('No network exist!!')
        else:
            conn = out_conn_list[0]
            msg = 'rfbk'
            conn.send(msg.encode())
            sleep(10)
            rcvd_msg = conn.recv(102476)
            bk = rcvd_msg.decode()
            print('recieved bk: {}'.format(bk))
            if bk.find('*rfbk*') >= 0: #containg bk 
                bk_list = bk.split('*rfbk*')
                bk = bk_list[1]
                if bk not in self.pending_queue:
                    self.lock.acquire()
                    self.pending_queue.append(bk)
                    self.lock.release()
                    
                self.add_multi_block_pending_queue(bk_list[2])
                
                msg = 'rfbc:'+str(bk)
                conn.send(msg.encode())
                sleep(10)
                rcvd_msg = conn.recv(102476)
                rcvd_msg = rcvd_msg.decode()
                if rcvd_msg.find('*rfbc*') >= 0:
                    bcs = rcvd_msg.split('*rfbc*')
                    upto_bk = bcs[1]
                    self.add_multi_block_pending_queue(bcs[0])
                    self.add_multi_block_pending_queue(bcs[2])
                    block_list = upto_bk.split(';')
                    if self.validate_rcvd_bc(block_list):
                        for block in block_list:
                            self.bc.append([block])
                            
                print('Synch with the blockchain of length: {}'.format(len(self.bc)))
                    
   
 

       
 

       
        
    def server_site(self): #handle incoming connection request #done
        
        # Sockets from which we expect to read
        #print('Inside server site')
        if self.socket_sock == None:
            print('no socket is bind')
            return
            
        self.socket_sock.listen(15)
        
        inputs = [ self.socket_sock ]
        
            

        # Sockets to which we expect to write
        outputs = [ ]
        
        # Outgoing message queues (socket:Queue)
        message_queues = {}
        curr_time = time.time()
        time_bound = curr_time + self.peer_timeout + 100.0
        #print('print server timeout: {}'.format(time_bound-curr_time))
        while True and curr_time < time_bound :
            curr_time  = time.time()
            if self.is_First_connected:
                #print('Out connections added: {}'.format(list(self.out_connection_peer_list.keys())))
                for conn in list(self.out_connection_peer_list.values()):
                    inputs.append(conn)
                self.is_First_connected = False    
            readable, writable, exceptional = select.select(inputs, outputs, inputs,30)
            
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
                    #sleep(2)
                    
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
                msg = '*rfbk*'+str(long_chain[-1])+'*rfbk*'
            self.update_q_output(s,outputs,message_q,msg)
            
        elif split_msg[0] == 'rfbc':
            print('Request for b0 --- bk-1')
            bk = split_msg[1]
            bc_upto_k = self.get_bc_upto_k(bk)
            if len(bc_upto_k) == 0:
                msg = 'empty'
            else:
                msg = ';'.join(bc_upto_k)
                msg = '*rfbc*'+msg+'*rfbc*'
            self.update_q_output(s,outputs,message_q,msg)
            #self.in_connection_peer_list.append(s)
            
        elif split_msg[0] == 'block':
            decoded_msg = rcvd_msg.decode()
            block_list = decoded_msg.split('block:')
            self.ad_blks_pending_queue(block_list[1:])
            '''
            block = split_msg[1]
            if not self.is_block_exist(block):
                print('New block arrived: {}'.format(block))
                self.lock.acquire()
                self.pending_queue.put(block)
                #if s not in self.in_connection_peer_list:
                #    self.in_connection_peer_list.append(s)
                self.lock.release()
                print('Released the lock on pending_queue')
                
            '''
               
            
        #else: #gossip reciept 
        #    print('invalid message_reacter: {}'.format(rcvd_msg.decode()))
        
    def ad_blks_pending_queue(self,blk_list):
        for block in blk_list:
            if block != '':
                if not self.is_block_exist(block) and (block not in self.pending_queue):
                    log = 'New block arrived: {}'.format(block)
                    print(log)
                    logging.info(log)
                    self.lock.acquire()
                    self.pending_queue.append(block)
                    self.lock.release()
                
        
    def generate_message(self,msg_type='goss'): #done
        msg_type=msg_type.lower()
        
        if msg_type=='plrq': #peer list Request
            return 'Peer List Request:'+str(self.ip)+"#"+str(self.port)
    
        elif msg_type=='regi': #seed register message
            return 'Register:'+str(self.ip)+'#'+str(self.port)
        
        else:
            return 'Invalid generate request'
        
        

        
        
                    
                    
                    
    def client_site(self):
        
        inputs = []  
        outputs = []
        
        #for conn in list(self.out_connection_peer_list.values()):
        #    inputs.append(conn)
        
        message_queues = {}
        
        time_interval = self.peer_timeout/5.0
            
        print('inserting into clients event loop')
        self.reset_mining_timer()
        curr_time = time.time()
        time_bound = curr_time + self.peer_timeout + 5.0
        time_interval_bound = curr_time + time_interval
        while True and curr_time < time_bound:
            curr_time = time.time()
            if(curr_time >= time_interval_bound):
                time_interval_bound = curr_time + time_interval
                self.intarrivaltime_axis.append(self.intarrivaltime)
                self.mpu_axix.append(self.mining_power_utilization())
                self.frac_axis.append(self.fraction_of_my_blocks())
                self.intarrivaltime += 4
                
            print('Current intarrival time: {}'.format(self.intarrivaltime))  
            self.resolving_pending_queue(inputs, outputs, message_queues)
            self.creating_and_mining_block(inputs, outputs, message_queues)
            

    def mining(self):
        #self.mining_time_bound = time.time() + self.waiting_time()
        
        start_time = time.time()
        while len(self.pending_queue) ==0 and start_time < self.mining_time_bound:
            sleep(1)
            start_time += 1
            
    def reset_mining_timer(self):
        self.mining_time_bound = time.time() + self.waiting_time()
        
            
    def resolving_pending_queue(self,inputs, outputs,message_queues):
        #print('resolving pending queue of size: {}'.format(self.pending_queue.qsize()))
        curr_bc_len = len(self.bc)
        while len(self.pending_queue) > 0:
            self.lock.acquire()
            block = self.get_from_pending_queue()
            self.lock.release()
            is_valid,pos = self.is_valid_block(block)
            if is_valid:
                self.add_block_in_bc(block,pos)
                #print('Valid block')
                new_bc_len = len(self.bc)
                if curr_bc_len < new_bc_len:
                    curr_bc_len = new_bc_len
                    msg = 'block:'+str(block)
                    pcl=list(self.out_connection_peer_list.values()) + list(self.in_connection_peer_list.values())
                    for conn in pcl : #sends blocks to all connected
                        #print('Sending recieved block: {}'.format(msg))
                        try:
                            conn.send(msg.encode())
                        except:
                            pass
                                
                        #sleep(2)
                        #self.update_q_output_client(conn,outputs,message_queues,msg)
                        #self.check_for_io(inputs,outputs,message_queues)
                        
                    self.reset_mining_timer()
                    
            else:
                print('block not valid')
                
                        
        #print('Pending queue size after resolving: {}'.format(self.pending_queue.qsize()))            
                        
    def creating_and_mining_block(self,inputs, outputs,message_queues):
        if bool(self.out_connection_peer_list) or len(self.in_connection_peer_list) > 0:
            long_chain  = self.longest_chain()
            self.mining()
            if len(self.pending_queue)==0:
                if len(long_chain) == 0:
                    new_block = self.gen_block()
                else:
                    new_block = self.gen_block(long_chain[-1])
                        
                self.add_block_in_bc(new_block,len(long_chain)-1)
                log = 'New block created: {}'.format(new_block)
                print(log)
                logging.info(log)
                
                self.my_block_list.append(new_block)
                msg = 'block:'+str(new_block)
                pcl=list(self.out_connection_peer_list.values()) + list(self.in_connection_peer_list.values())
                for conn in pcl : #sends blocks to all connected
                    #log = 'Sending new block: {}'.format(msg)
                    #logging.info(log)
                    #print('Sending new block: {}'.format(msg))
                    try:
                        conn.send(msg.encode())
                    except:
                        pass
                    
                self.reset_mining_timer()
                
                
                
    def mining_power_utilization(self):
        longest_chain = self.longest_chain()
        
        total_no_blocks=0
        for level in self.bc:
            total_no_blocks = total_no_blocks + len(level)
            
        mpu = len(longest_chain)/total_no_blocks
        return mpu
    
    def fraction_of_my_blocks(self):
        longest_chain = self.longest_chain()
        my_blcok_in_lc = [block for block in self.my_block_list if block in longest_chain]
        frac = len(my_blcok_in_lc)/len(longest_chain)
        return frac
            
            
        
                
    
                
    def start_flooding(self,flood_percent=20):
        print('Flooding started.......')
        flood_power = self.flood_power(flood_percent)
        curr_time = time.time()
        time_bound = curr_time + self.peer_timeout
        while True and curr_time < time_bound:
            curr_time = time.time()
            self.creating_and_sending_advr_block(flood_power)
        
            
        
        
        
        
    def creating_and_sending_advr_block(self,flood_power):
        if bool(self.out_connection_peer_list) > 0:
            self.adv_mining(flood_power)
            new_block = self.gen_adv_block()
            log = 'New adversariar block created: {}'.format(new_block)
            print(log)
            logging.info(log)
            msg = 'block:'+str(new_block)
            pcl=list(self.out_connection_peer_list.values())
            for conn in pcl : #sends blocks to all connected
                #print('Sending new adversarial block: {}'.format(msg))
                try:
                    conn.send(msg.encode())
                except:
                    pass
                    
                   
        
     
    def adv_mining(self,flood_power):
        time_bound = time.time() + self.flood_mining_time(flood_power)
        start_time = time.time()
        while start_time < time_bound:
            sleep(1)
            start_time += 1
        
    def flood_power(self,flood_percent):
        fld_power = (100*flood_percent)/(100 - flood_percent)  #100 is total hashing power overl all miner
        return fld_power
    
    def flood_mining_time(self,flooding_power):
        global_lamda = 1.0/self.intarrivaltime
        lamda = flooding_power*global_lamda/100.0
        #print('flooding lamda: {}'.format(lamda))
        ev = np.random.exponential()/lamda
        print('flood mining time: {}'.format(ev))
        return ev
    
    def gen_adv_block(self):
        t = 'adversar'
        hash_obj = hashlib.shake_256() # In assignment they recommend to use SHA-256, but is gives 256 bit output
        hash_obj.update(t.encode())
        prev_hash = hash_obj.hexdigest(2)
        ts = int(time.time())
        return str(prev_hash)+'#'+self.merkel_root+'#'+str(ts)
        
    
    def summarizing_data(self):
        #print('Blockchain is: {}\n\n'.format(self.bc))
        total_num_blocks = 0
        for level in self.bc:
            total_num_blocks += len(level)
            
        print('Total number of blocks in blockchain: {}'.format(total_num_blocks))
        
        
        longest_chain = self.longest_chain()
        #print('Longest chain is: {}\n\n'.format(longest_chain))
        print('Number of block in longest chain is: {}\n\n'.format(len(longest_chain)))
        #print('My blocks in blockchain are: {}\n\n'.format(self.my_block_list))
        print('Number of mine blocks in blockchain are: {}\n'.format(len(self.my_block_list)))
        
        plt.plot(self.intarrivaltime_axis,self.mpu_axix)
        plt.xlabel('intarrivaltime')
        plt.ylabel('mining power utlization')
        plt.suptitle('Plot of: {}'.format(self.port))
        plt.show()
        
        plt.plot(self.intarrivaltime_axis,self.frac_axis)
        plt.xlabel('intarrivaltime')
        plt.ylabel('fraction of block mined')
        plt.suptitle('Plot of: {}'.format(self.port))
        plt.show()
        
        
                

                
            
       

if __name__ == "__main__":
    ip = '127.0.0.1' # Input seed_ip here
    port = 3013
    is_advsarial = True
    flood_percent = 20
    peer_timeout = 600
    intarrivaltime = 2
    
    if len(sys.argv) > 4:
        ip = sys.argv[1]
        port = int(sys.argv[2])
        peer_timeout = int(sys.argv[3])
        intarrivaltime =int(sys.argv[4])
        
    peer = Peer(ip,port,intarrivaltime=intarrivaltime,peer_timeout=peer_timeout)
    server_thread = threading.Thread(target=peer.server_site, args=(), daemon=True) #start listening input connections
    server_thread.start()
    sleep(20)
    peer.connect_to_config() # Connect to the peers
    sleep(10)
    peer.get_bc()
    sleep(2)
    peer.is_First_connected = True
    client_thread = threading.Thread(target=peer.client_site, args=(), daemon=True) # Strat sending gossips and liveness msgs
    client_thread.start()
    
    sleep(5)
    advrs_thread = threading.Thread(target=peer.start_flooding, args=(flood_percent,), daemon=True)
    if is_advsarial:
        advrs_thread.start()
    
    
    server_thread.join()
    client_thread.join()
    if is_advsarial:
        advrs_thread.join()
        
    peer.summarizing_data()
        
    
    
    
       
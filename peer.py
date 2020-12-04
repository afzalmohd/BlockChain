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


class Peer:
    def __init__(self,ip,port=6464,csv_file_path='config.csv',log_file="peer_log.log"):
        
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
                    
       
        
        
    def insert_to_ML(self,message_q,msg, outputs): #done #gosspi message insertion to ML and logged
        if hash(msg) not in self.MessageList:
            splt_msg = msg.split(';')
            temp_addr = splt_msg[1]
            addr_tup = temp_addr.split('#')
            addr = (addr_tup[0],int(addr_tup[1]))                           
            
            #now = datetime.now()
            #timestamp = datetime.timestamp(now)
            #dt=datetime.fromtimestamp(timestamp)
            log='Received Gossip message:  {} from: {} '.format(msg,addr)
            print(log)
            logging.info(log) #printing first received gossip message
            fltr=lambda X: False if X==addr else True #filter function remove the address the message came from
            p_conns=list(filter(fltr,list(self.out_connection_peer_list.keys())))
            self.MessageList[hash(msg)]=[addr,p_conns]
            for i in p_conns: 
                '''
                if i in self.out_connection_peer_list :
                    print('Insert to ML: Inside peer connection: {}'.format(i))
                    conn=self.out_connection_peer_list[i]
                else:
                    conn=self.seed_connection[i]
                '''
                conn=self.out_connection_peer_list[i]
                self.update_q_output(conn,outputs,message_q,msg)
            return True
        else:
            return False #if the message is already there
        
        
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
        
        if split_msg[0]=='Liveness Request': #what to do if a liveness req comes
            print('Recived : {}'.format(rcvd_msg))
            msg=self.generate_message(msg_type='lrep',sender_timestamp=split_msg[1],sender_ip=split_msg[2])
            self.update_q_output(s,outputs,message_q,msg)

        elif split_msg[0]== 'Liveness Reply': #reduces the counter of peer node by 1
            split_lr = split_msg[3].split('#')
            sender_addr = (split_lr[0],int(split_lr[1]))
            if sender_addr in self.alive_timer: #if its in alive timer not dead
                self.alive_timer[sender_addr]=3 #reset the counter
            
            
        elif split_msg[0]== 'Dead Node': #remove the dead ip from peer connection list
            _ = self.out_connection_peer_list.pop(split_msg[1])
            
        elif split_msg[0]== 'Peer List Request':  #gives the available list of connected peers
            msg=self.generate_message(msg_type='plrp')
            self.update_q_output(s,outputs,message_q,msg)
        
        elif split_msg[0]== 'Peer List Reply':  #gives the available list of connected peers
            print('Check in plr')
            self.requested_peer_list.append(split_msg[1])
        
        elif split_msg[0]== 'Register': #append to incoming peer list
            print('Check....................')
            #self.incoming_peer_list.append(split_msg[1].split('#'))
            
        else: #gossip reciept 
            self.insert_to_ML(message_q, rcvd_msg.decode('ascii'), outputs) #inserts and sends to all peers except the message came from
        
        
        
    def generate_message(self,msg_type='goss',sender_ip=None,sender_timestamp=None,deadnode_ip=None): #done
        msg_type=msg_type.lower()
        if msg_type=='goss': #'gossip'
            self.gossip_msg_number+=1
            now = datetime.now()
            timestamp = datetime.timestamp(now)
            dt=datetime.fromtimestamp(timestamp)
            return str(dt)+";"+str(self.ip)+"#"+str(self.port)+";"+str(self.gossip_msg_number)
        
        elif msg_type=='lreq': #liveness request
            now = datetime.now()
            timestamp = datetime.timestamp(now)
            dt=datetime.fromtimestamp(timestamp)
            return 'Liveness Request'+":"+str(dt)+":"+str(self.ip)+"#"+str(self.port)
        
        elif msg_type=='lrep': #liveness reply
            #timestamp =datetime.timestamp(datetime.now())
            return 'Liveness Reply'+':'+str(sender_timestamp)+":"+str(sender_ip)+":"+str(self.ip)+"#"+str(self.port)
        
        elif msg_type=='dead': #dead node
            now = datetime.now()
            timestamp = datetime.timestamp(now)
            dt=datetime.fromtimestamp(timestamp)
            return 'Dead Node:'+str(deadnode_ip[0])+'#'+str(deadnode_ip[1])+":"+str(dt)+":"+str(self.ip)+"#"+str(self.port)
        
        elif msg_type=='plrq': #peer list Request
            return 'Peer List Request:'+str(self.ip)+"#"+str(self.port)
        
        elif msg_type=='plrp': #peer list Reply
            xx=[]
            yyy=list(self.out_connection_peer_list.keys())#.extend(self.incoming_peer_list)
            
            for i in yyy :
                xx.append(i[0]+"#"+i[1]) # format == ip#port
            rep=','.join(xx)
            return 'Peer List Reply:'+rep
        
        elif msg_type=='regi': #seed register message
            return 'Register:'+str(self.ip)+'#'+str(self.port)
        
        else:
            return 'Invalid Request'
 
                

        
















    def client_site(self):
        
        #inputs = [ self.socket_sock ]
        inputs = []  
        outputs = []
        
        message_queues = {}
        
        gossip_count=0
        #self.update_output_gossip(outputs,message_queues)
        for conn in self.in_connection_peer_list.values():
            inputs.append(conn)
        is_first_entry=True   
        print('inserting into event loop: {}'.format(inputs))
        
        while inputs or outputs or is_first_entry:
            is_first_entry=False
            if gossip_count < 10:
                gossip_count += 1
                self.update_output_gossip(outputs,message_queues)
                sleep(5)
            else:
                self.check_ifalive_client(outputs,message_queues)
                sleep(13)
            
            readable, writable, exceptional = select.select(inputs, outputs, inputs)
            
            for s in readable:
                #print('Check....readable')
                if s is self.socket_sock:
                    print('complete it')
                else:
                    data = s.recv(102476)
                    self.message_reactor_client(data,message_queues,s,outputs)
                        
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
                



    def update_output_gossip(self,outputs,message_queues):
        msg=self.generate_message_client(msg_type='goss')
        print('Client: Goss msg generated')
        pcl=list(self.out_connection_peer_list.keys())
        for i in pcl : #sends gossip msg to all peers
            conn=self.out_connection_peer_list[i]
            self.update_q_output_client(conn,outputs,message_queues,msg)
        
                

    def check_ifalive_client(self,outputs,message_queues): #done will remove the dead nodes and will send alive request to all peers:
        all_peer_connection_address=list(self.out_connection_peer_list.keys())
        dead_msgs=[] #buffer all dead nodes messages to send remaining alive nodes
        for i in all_peer_connection_address:
            if self.alive_timer[i]<=0:
                self.out_connection_peer_list.pop(i) #remove the dead node
                ded_msg=self.generate_message_client(msg_type='dead',deadnode_ip=i)
                print('Dead node msg:',ded_msg)
                logging.info(ded_msg)
                dead_msgs.append(ded_msg)
            else:
                self.alive_timer[i]-=1 #reduces life by 1
        #send the remaining peers the alive request
        msg=self.generate_message_client(msg_type='lreq')
        for i in self.out_connection_peer_list:
            conn = self.out_connection_peer_list[i]
            self.update_q_output_client(conn,outputs,message_queues,msg)
            
        #send all seeds the dead msgs:
        for msg in dead_msgs:
            for i in self.seed_connection:
                conn = self.seed_connection[i]
                self.update_q_output_client(conn,outputs,message_queues,msg)
 

    
    
            

    def insert_to_ML_client(self,message_q,msg, outputs): #done #gosspi message insertion to ML and logged
        if hash(msg) not in self.MessageList:
            splt_msg = msg.split(';')
            addr = splt_msg[1]
            #now = datetime.now()
            #timestamp = datetime.timestamp(now)
            #dt=datetime.fromtimestamp(timestamp)
            log='Client: Received Gossip message:  {} '.format(msg)
            print(log)
            logging.info(log) #printing first received gossip message
            fltr=lambda X: False if X==addr else True #filter function remove the address the message came from
            p_conns=list(filter(fltr,list(self.out_connection_peer_list.keys())))
            self.MessageList[hash(msg)]=[addr,p_conns]
            for i in p_conns:
                '''
                if i in self.out_connection_peer_list :
                    print('Inside peer connection')
                    conn=self.out_connection_peer_list[i]
                else:
                    conn=self.seed_connection[i]
                '''
                conn=self.out_connection_peer_list[i]
                self.update_q_output_client(conn,outputs,message_q,msg)
            return True
        else:
            return False #if the message is already there
        
        
    def update_q_output_client(self,conn,outputs,message_q=None,msg=None):
        if msg is not None:
            if conn in message_q:
                message_q[conn].put(msg.encode())
            else:
                message_q[conn] = Queue()
                message_q[conn].put(msg.encode())     
        if conn not in outputs:
            outputs.append(conn)
    
    def message_reactor_client(self,rcvd_msg,message_q=None,s=None,outputs=None): #done
        split_msg=rcvd_msg.decode("utf-8").split(':')  #added sap
#         print('split msg is {}'.format(split_msg))
        
        if split_msg[0]=='Liveness Request': #what to do if a liveness req comes
            msg=self.generate_message_client(msg_type='lrep',sender_timestamp=split_msg[1],sender_ip=split_msg[2])
            self.update_q_output_client(s,outputs,message_q,msg)

        elif split_msg[0]== 'Liveness Reply': #reduces the counter of peer node by 1
            print('Client: Recieved liveness message: {}'.format(rcvd_msg))
            split_lr = split_msg[3].split('#')
            sender_addr = (split_lr[0],int(split_lr[1]))
            if sender_addr in self.alive_timer: #if its in alive timer not dead
                self.alive_timer[sender_addr]=3 #reset the counter
            
            
        elif split_msg[0]== 'Dead Node': #remove the dead ip from peer connection list
            _ = self.out_connection_peer_list.pop(split_msg[1])
            
        elif split_msg[0]== 'Peer List Request':  #gives the available list of connected peers
            msg=self.generate_message_client(msg_type='plrp')
            self.update_q_output_client(s,outputs,message_q,msg)
        
        elif split_msg[0]== 'Peer List Reply':  #gives the available list of connected peers
            print('Check in plr')
            self.requested_peer_list.append(split_msg[1])
        
        elif split_msg[0]== 'Register': #append to incoming peer list
            print('Check....................')
            #self.incoming_peer_list.append(split_msg[1].split('#'))
            
        else: #gossip reciept 
            self.insert_to_ML_client(message_q, rcvd_msg.decode('ascii'), outputs) #inserts and sends to all peers except the message came from
        
        
        
    def generate_message_client(self,msg_type='goss',sender_ip=None,sender_timestamp=None,deadnode_ip=None): #done
        msg_type=msg_type.lower()
        if msg_type=='goss': #'gossip'
            self.gossip_msg_number+=1
            now = datetime.now()
            timestamp = datetime.timestamp(now)
            dt=datetime.fromtimestamp(timestamp)
            return str(dt)+";"+str(self.ip)+"#"+str(self.port)+";"+str(self.gossip_msg_number)
        
        elif msg_type=='lreq': #liveness request
            now = datetime.now()
            timestamp = datetime.timestamp(now)
            dt=datetime.fromtimestamp(timestamp)
            return 'Liveness Request'+":"+str(dt)+":"+str(self.ip)+"#"+str(self.port)
        
        elif msg_type=='lrep': #liveness reply
            #timestamp =datetime.timestamp(datetime.now())
            return 'Liveness Reply'+':'+str(sender_timestamp)+":"+str(sender_ip)+":"+str(self.ip)+"#"+str(self.port)
        
        elif msg_type=='dead': #dead node
            now = datetime.now()
            timestamp = datetime.timestamp(now)
            dt=datetime.fromtimestamp(timestamp)
            return 'Dead Node:'+str(deadnode_ip[0])+'#'+str(deadnode_ip[1])+":"+str(dt)+":"+str(self.ip)+"#"+str(self.port)
        
        elif msg_type=='plrq': #peer list Request
            return 'Peer List Request:'+str(self.ip)+"#"+str(self.port)
        
        elif msg_type=='plrp': #peer list Reply
            xx=[]
            yyy=list(self.out_connection_peer_list.keys())#.extend(self.incoming_peer_list)
            
            for i in yyy :
                xx.append(i[0]+"#"+i[1]) # format == ip#port
            rep=','.join(xx)
            return 'Peer List Reply:'+rep
        
        elif msg_type=='regi': #seed register message
            return 'Register:'+str(self.ip)+'#'+str(self.port)
        
        else:
            return 'Invalid Request'           
    
                
                
                
                
                
                
                




        

if __name__ == "__main__":
    ip = '127.0.0.1' # Input seed_ip here
    port = 3013
    if len(sys.argv) > 1:
        ip = sys.argv[1]
        port = int(sys.argv[2])
        
    peer = Peer(ip,port)
    server_thread = threading.Thread(target=peer.server_site, args=())
    server_thread.start()
    sleep(20)
    peer.connect_to_config()
    sleep(10)
    client_thread = threading.Thread(target=peer.client_site, args=())
    client_thread.start()
    server_thread.join()
    client_thread.join()
    
    
       
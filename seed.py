#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import csv
from datetime import datetime
import socket
import threading
from queue import Queue
import select
import sys
from time import sleep


# In[ ]:


class Seed:
    def __init__(self,ip,port=6464):
        self.MessageList = []
        self.peer_list = [] #connection of the port and ip
        self.connection_list = {}
        self.ip=ip
        self.port=port
        self.socket= socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.working=False
        self.queue = Queue()
        self.message_queues = {}
        self.msg_number=0
        self.bind_sock()

    def bind_sock(self):
        """
        Binding the socket and listening to connections
        """
        try:
            self.socket.setblocking(0)
            self.socket.bind((self.ip,int(self.port))) #bind scoket
            self.socket.listen(5)  #listen for connections
            print("Seed created and binded to {}:{} \n".format(self.ip,self.port))
            
            #starts event loop
            self.start_event_loop()

            
        except Exception as e:
            print("binding to {}:{} has failed".format(self.ip,self.port))
            print('Sock binding exception is {}'.format(e))
            
            
    def start_event_loop(self):
        """
        An asynchronous event loop that handles all Seed activities:
        1. Handles Registration request from a Peer and connects it to P2P network
        2. Sends Peer list on request "plrq" from a Peer :- "plrq" stands for Peer List request 
        3. Writes to screen and an output file about new Peer connections and Dead Node requests
        4. When seed receives dead node request of the form :
            Dead Node:<DeadNode.IP>:<DeadNode.Port>:<self.timestamp>:<self.IP>
            
            The seed node removes the "dead-node" from the network
        """
        # Sockets from which we expect to read
        inputs = [ self.socket ]

        # Sockets to which we expect to write
        outputs = [ ]
        
        # Outgoing message queues (socket:Queue)
        message_queues = {}
        
        print('Server started listening for Peers \n')
        
        while inputs:
            # Wait for at least one of the sockets to be ready for processing
            readable, writable, exceptional = select.select(inputs, outputs, inputs)
            
            # Handle inputs
            for s in readable:
                
                if s is self.socket:
                    # A "readable" server socket is ready to accept a connection
                    connection, client_address = s.accept()
                    print('New Peer connection from', client_address)
                    
                    #Write to output file
                    with open("outputfile.txt", "a") as file:
                        file.write("Connection request from new Peer "+str(client_address) + "\n")
                    file.close()
                        
                    connection.setblocking(0)
                    inputs.append(connection)  # Store newly connected peer socket
                    
                    #Store format ('10.119.2.95',8080):<socket> as dict
                    self.connection_list[(str(client_address[0]),str(client_address[1]))] = connection

                    # Give the connection a queue for data we want to send and initialize its liveness check count to 3
                    message_queues[connection] = Queue()
                    
                else:
                    data = s.recv(102476)
                    decodedData = data.decode("utf-8")
                    splitData = list([""])
                    
                    if data:
                        # A readable client socket has data
                        print('received "{}" from {}\n'.format(decodedData, s.getpeername()))
                        
                        try:
                            splitData = decodedData.split(':')
                        except:
                            pass
                        
                        #If a peer request for registration in P2P network
                        if splitData[0] == "Register":
                            peer = splitData[1].split('#')
                            peer = (peer[0],int(peer[1]))
                            self.peer_list.append(peer)
                            #Write to output file
                            with open("outputfile.txt", "a") as file:
                                file.write("Registration request from Peer "+str(peer) + "\n")
                            file.close()
                            
                        
                        # If a Peer List request is received from peer,then send peer list
                        if splitData[0] == "Peer List Request":
                            print('Sending peer list')
                            msg = []
                            for peer in self.peer_list:
                                msg.append(str(peer[0]) + "#" + str(peer[1]))
                                
                            rep = 'Peer List Reply:'+ ','.join(msg)
                            message_queues[s].put(rep.encode())
                            
                        #Delete dead node request
                        if splitData[0] == 'Dead Node':
                            try:
                                with open("outputfile.txt", "a") as file:
                                    file.write("Dead Node Request: \n")
                                    file.write(decodedData + " from " + str(splitData[3]) +"\n")
                                file.close()
                                for key in self.connection_list.keys():
                                    if key[0] == splitData[1] and key[1] == splitData[2]:
                                        #Close connection with dead-node
#                                         inputs.remove(self.connection_list[key])
#                                         dead_node = self.connection_list[key]
#                                         if dead_node in outputs:
#                                            outputs.remove(dead_node)
#                                         dead_node.close()
#                                         del message_queues[dead_node]
                                        
                                        #Remove dead node
                                        print('Dead node request for {}: {}'.format(key[0],key[1]))
                                        self.peer_list.remove((key[0],int(key[1])))
                                        remove_dead_node_message = "Removed " + key[0] + ":" + key[1]+" on dead node request"
                                        message_queues[s].put(remove_dead_node_message.encode())
                                        del self.connection_list[key]
                                        break
                            except Exception as e:
                                print(e)
                                pass
                                    
                                    
                        # Add output channel for response
                        if s not in outputs:
                            outputs.append(s)
                            
            # Handle outputs
            for s in writable:
                try:
                    next_msg = message_queues[s].get_nowait()
                except:
                    # No messages waiting so stop checking for writability.
                    pass
                else:
                    print('sending %s to %s \n' % (next_msg, s.getpeername()))
                    s.send(next_msg)
                    
                    
            # Handle "exceptional conditions"
            for s in exceptional:
                print('handling exceptional condition for\n', s.getpeername())
                # Stop listening for input on the connection
                inputs.remove(s)
                if s in outputs:
                    outputs.remove(s)
                s.close()

                # Remove message queue
                del message_queues[s]



if __name__ == "__main__":
    seed_ip = '127.0.0.1' # Input seed_ip here
    seed_port = 3010
    if len(sys.argv) > 1:
        seed_ip = sys.argv[1]
        seed_port = int(sys.argv[2])
    Seed(seed_ip,seed_port)
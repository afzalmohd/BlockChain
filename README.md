# BlockChain

		__________Seed:__________
seed.py stores the code for a node of type Seed.
It binds to an IP:port and starts its asynchronous event loop.

Functions:

- It listens for connection requests from multiple Peers.

- It listens for registration request into the P2P network and send the
peer list on Peer List request from a Peer.

- It listens for dead node requests from fellow Peers and remove the
requested dead node to be removed.

Instructions:
Run the seed.py on a certain Ip:port
by changing the seed_ip and seed_port in the seed.py file
Furthermore,write your seed_ip and seed_port in 'config.csv' for it 
to be read by peers whilst sending connection requests.

		_______________Peer:________________
peed.py stores the code for a node of type Peer.

Functions:
- It creates a connection with a randomly picked seed(out of [n/2]+1)
on a outgoing connection port and bind to an incoming port for listening to 
other peer connections.

- It requests Peer list from Seeds,and on receiving , forms the union of all
Peer list and sends gossip messages to those peers

- Additionally, each peer sends a Liveness request to every other connected peer
after 13 seconds and if it doesn't receive a a reply on those requests,
it send a Dead Node request to the connected Seeds.


			----------END--------


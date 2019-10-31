# RAFT--distributed-storage-system
A distributed storage system built based on RAFT paper
I assume all the servers are running on localhost.

start a server:
python udp_server.py <port> <start port> <num of servers>

For example, If only one server with port 8000:
open 1 terminal and enter the following command
python udp_server.py 8000 8000 1

To establish a server group with 5 servers and corresponding ports 8000,8001,8002,8003,8004:
open 5 terminals and enter the following commands on each terminal
python udp_server.py 8000 8000 5
python udp_server.py 8001 8000 5
python udp_server.py 8002 8000 5
python udp_server.py 8003 8000 5
python udp_server.py 8004 8000 5

Note: Make sure you use consecutive ports for servers. The system only recognizes servers from port = start_port to start_port+num_of_servers



start a client:
python udp_client.py <port to request>

For example, to send a request to port 8000: python udp_client.py 8000
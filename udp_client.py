import socket
import sys
import json

port = int(sys.argv[1])

# Create a UDP socket
sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

server_address = ('localhost', port)
put1 = {'type': 'put', 'payload': {'key': 'aaa', 'value': 'aa123'}}
get1 = {'type': 'get', 'payload': {'key': 'aaa'}}
put2 = {'type': 'put', 'payload': {'key': 11, 'value': [1,2]}}
get2 = {'type': 'get', 'payload': {'key': 11}}

collect = [put1,get1,put2,get2]

try:
    for content in collect:
        sock.settimeout(2)
        # Send data
        #print('sending {!r}'.format(put))
        sent = sock.sendto(json.dumps(content).encode('utf-8'), server_address)

        # Receive response
        #print('waiting to receive')
        data, server = sock.recvfrom(4096)
        print('received {!r}'.format(data))
        #print(server)

except:
    print("timeout")
finally:
    print('closing socket')
    sock.close()
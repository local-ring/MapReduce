import os 
import sys
import json

import zmq
from collections import defaultdict

def wordcount(data):
    """
    Count the number of words in a given text.
    """
    result = {}
    for kvpair in data:
        key, value = kvpair.split()
        if key in result:
            result[key] += int(value)
        else:
            result[key] = int(value)

    return result


def invertindex(data):
    """
    Create an inverted index for a given text.
    """
    result = defaultdict(lambda: defaultdict(int)) # we use defaultdict to simplify the code
    for kvpair in data:

        key, document, value = kvpair.split()
        result[key][document] += int(value) 

    return result

if __name__ == '__main__':
    reducerPull, reducerPush, controlPull, controlPush, id = sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4], sys.argv[5]
    context = zmq.Context()

    # receive the work type from the master
    controlPullSocket = context.socket(zmq.SUB)
    controlPullSocket.connect(controlPull)
    controlPullSocket.setsockopt_string(zmq.SUBSCRIBE, '')
    workType = None
    while not workType:
        workType = controlPullSocket.recv().decode()
    print(f"Reducer {id} received the work type")
    
    controlPushSocket = context.socket(zmq.PUSH)
    controlPushSocket.connect(controlPush)
    controlPushSocket.send(b'ready')


    pullSocket = context.socket(zmq.DEALER)
    pullSocket.setsockopt_string(zmq.IDENTITY, id) # use id number as the identity
    pullSocket.connect(reducerPull)

    receiveAck = False

    while not receiveAck:
        pullSocket.send(b'hi')
        ack = pullSocket.recv()
        if ack == b'ack':
            receiveAck = True

    # receive the work type from the master
    # controlSocket = context.socket(zmq.DEALER)
    # controlSocket.connect(controlPull)
    # controlSocket.send(b'worker')
    # while not workType:
    #     workType = controlSocket.recv().decode()

    # controlSocket.send(b'ready')
    

    tempDataPath = f'temp/data_reducer_{id}_t3.txt'
    with open(tempDataPath, 'w') as f:
        while 1:
            data = pullSocket.recv()
            # print(data)
            if data == b'END_OF_DATA':
                break
            f.write(data.decode())

    # sort the data according to the key
    with open(tempDataPath, 'r') as f:
        data = f.readlines()

    if workType == 'wordcount':
        result = wordcount(data)
    elif workType == 'invertindex':
        result = invertindex(data)

    pushSocket = context.socket(zmq.PUSH)
    pushSocket.connect(reducerPush)
    pushSocket.send_string(json.dumps(result))
    print(f"Reducer {id} has finished the work")

    sys.exit(0)
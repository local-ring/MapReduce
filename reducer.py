import os 
import sys
import json

import zmq

def wordcount(data):
    """
    Count the number of words in a given text.
    """
    words = data.split()
    return {word: words.count(word) for word in words}

def invertindex(data):
    """
    Create an inverted index for a given text.
    """
    words = data.split()
    return {word: [i for i, w in enumerate(words) if w == word] for word in set(words)}

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


    pullSocket = context.socket(zmq.PULL)
    pullSocket.connect(reducerPull)
    print(f"Reducer {id} is ready to receive the data")

    # receive the work type from the master
    # controlSocket = context.socket(zmq.DEALER)
    # controlSocket.connect(controlPull)
    # controlSocket.send(b'worker')
    # while not workType:
    #     workType = controlSocket.recv().decode()

    # controlSocket.send(b'ready')
    


    tempDataPath = f'data_reducer_{id}.txt'
    with open(tempDataPath, 'w') as f:
        data = pullSocket.recv_string()
        f.write(data)
        print(f"Reducer {id} has received the data")

    # sort the data according to the key
    with open(tempDataPath, 'r') as f:
        data = f.readlines()
    data.sort()

    if workType == 'wordcount':
        result = wordcount("".join(data))
    elif workType == 'invertindex':
        result = invertindex("".join(data))

    pushSocket = context.socket(zmq.PUSH)
    pushSocket.connect(reducerPush)
    pushSocket.send_string(json.dumps(result))
    print(f"Reducer {id} has finished the work")

    sys.exit(0)
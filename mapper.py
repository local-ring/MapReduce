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
    mapperPull, mapperPush, controlPull, controlPush, id = sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4], sys.argv[5]
    context = zmq.Context()

    controlPullSocket = context.socket(zmq.SUB)
    controlPullSocket.connect(controlPull)
    controlPullSocket.setsockopt_string(zmq.SUBSCRIBE, '') # subscribe to all topics
    workType = None
    while not workType:
        workType = controlPullSocket.recv().decode()
    
    print(f"Mapper {id} received the work type")

    controlPushSocket = context.socket(zmq.PUSH)
    controlPushSocket.connect(controlPush)
    controlPushSocket.send(b'ready')


    pullSocket = context.socket(zmq.PULL)
    pullSocket.connect(mapperPull)
    print(f"Mapper {id} is ready to receive the data")

    # receive the work type from the master
    # controlSocket = context.socket(zmq.DEALER)
    # controlSocket.connect(controlPull)
    # controlSocket.send(b'worker')
    # while not workType:
    #     workType = controlSocket.recv().decode()
    
    # controlSocket.send(b'ready')
    # print(f"Mapper {id} received the work type")

    result = []

    if workType == 'wordcount':
        data = pullSocket.recv_string()
        result = wordcount(data)
    elif workType == 'invertindex':
        data = pullSocket.recv_string()
        result = invertindex(data)

        
    pushSocket = context.socket(zmq.PUSH)
    pushSocket.connect(mapperPush)
    # print(f"Mapper {id} is ready to send the data")
    pushSocket.send_string(json.dumps(result))
    print(f"Mapper {id} has finished the work")

    sys.exit(0)


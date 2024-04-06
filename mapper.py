import os 
import sys
import json

import zmq

def wordcount(data, path):
    """
    Count the number of words in a given text.
    """
    words = data.split()
    # for each word, generate a key-value pair (word, "1")
    kvpairs = [(word.lower() , "1") for word in words] # I really want to omit value 1 here because it is stupid
    with open(path, 'a') as f: # write into the local disk
        for kvpair in kvpairs:
            f.write(kvpair[0] + " " + kvpair[1] + "\n")

    return 1

def invertindex(data, path):
    """
    Create an inverted index for a given text.
    """
    data = json.loads(data)
    # print(data)

    for file_content in data:
        filename = file_content['filename']
        lines = file_content['content']
        words = []
        for line in lines:
            words.extend(line.split())
        kvpairs = [(word.lower(), filename, "1") for word in words]
        with open(path, 'a') as f:
            for kvpair in kvpairs:
                f.write(kvpair[0] + " " + kvpair[1] + " " + kvpair[2] + "\n")

    return 1

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
    path = f'temp/data_mapper_{id}_t4.txt'
    if workType == 'wordcount':
        data = pullSocket.recv_string()
        wordcount(data, path)
    elif workType == 'invertindex':
        data = pullSocket.recv_string()
        invertindex(data, path)

        
    pushSocket = context.socket(zmq.PUSH)
    pushSocket.connect(mapperPush)
    # print(f"Mapper {id} is ready to send the data")
    with open(path, 'r') as f:
        data = f.readlines()
        for line in data:
            pushSocket.send_string(line)
    
    # send the end signal to shuffler
    pushSocket.send_string("END_OF_DATA") # since we converted all words to lowercase, so we can use this as the end signal without worrying the signal is a word
    print(f"Mapper {id} has finished the work")

    sys.exit(0)


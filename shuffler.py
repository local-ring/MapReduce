import zmq
import sys


if __name__ == '__main__':

    shufflerPull, shufflerPush, numMappers, numReducers = sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4]
    context = zmq.Context()
    pullSocket = context.socket(zmq.PULL)
    pullSocket.bind(shufflerPull)
    print(f"Shuffler is ready to receive the data")

    pushSocket = context.socket(zmq.ROUTER) # we need to send same key data to the same reducer, so use ROUTER
    pushSocket.bind(shufflerPush)

    numMappers = int(numMappers)
    numReducers = int(numReducers)

    endMessage = 0
    with open('temp/data_shuffler_t2.txt', 'w') as f:
        while endMessage < numMappers:
            data = pullSocket.recv_string()
            if data == 'END_OF_DATA':
                endMessage += 1
            else:
                f.write(data)

        print(f"Shuffler has received all the data")

    # sort the data according to the key
    with open('temp/data_shuffler.txt', 'r') as f:
        data = f.readlines()
    # print(data)

    # data.sort()
        
    # register the reducers
    reducers = {}
    while 1: # listen to the reducers until all reducers are registered
        id, message = pushSocket.recv_multipart()
        if message.decode() == "hi" and id not in reducers:
            reducers[id] = "registered"
            pushSocket.send_multipart([id, b'ack'])
            if len(reducers) == numReducers:
                break

    # send the data to the reducers
    # each reducer will always get complete data with the same key
    for kvpair in data:
        key, value = kvpair.split()
        reducerID = hash(key) % numReducers
        pushSocket.send_multipart([str(reducerID).encode(), kvpair.encode()])
    
    for i in range(numReducers):
        pushSocket.send_multipart([str(i).encode(), b'END_OF_DATA'])

    print(f"Shuffler has sent the data to the reducers")

    sys.exit(0)


import zmq
import sys


if __name__ == '__main__':

    shufflerPull, shufflerPush, numMappers, numReducers = sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4]
    context = zmq.Context()
    pullSocket = context.socket(zmq.PULL)
    pullSocket.bind(shufflerPull)
    print(f"Shuffler is ready to receive the data")

    pushSocket = context.socket(zmq.PUSH)
    pushSocket.bind(shufflerPush)

    numMappers = int(numMappers)
    numReducers = int(numReducers)

    tempDataPath = 'data_shuffler.txt'
    with open('data.txt', 'w') as f:
        for i in range(numMappers):
            data = pullSocket.recv_string()
            f.write(data)
            f.write('\n')
        print(f"Shuffler has received the data")

    # sort the data according to the key
    with open('data.txt', 'r') as f:
        data = f.readlines()
    data.sort()
    

    # send the data to the reducers
    for i in range(numReducers):
        start = i * len(data) // numReducers
        end = (i + 1) * len(data) // numReducers
        pushSocket.send_string("".join(data[start:end]))

    print(f"Shuffler has sent the data to the reducers")

    sys.exit(0)


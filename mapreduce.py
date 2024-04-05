import zmq
import json
import os, sys, subprocess, threading
from utils import zmq_addr
import time

"""
There are two main applications for this script:
- word count
- inverted index: recorde which documents does a word appear in
"""

"""
Nodes and taskes are implemented as conventional OS processes, and the communication between them is done using network-based communication (because we want the application to be able to run on multiple machines).
"""


"""
The master node is responsible for:
- spawn the map and reduce tasks
    to do this:
        - it needs to know the number of mappers and reducers   
        - it assign the ID and port number to each task

- control and coordinate all other processes (TODO: be more specific)

# TODO: look at the configuration options in Hadoop

The user can only interact with the master node through a well-defined API (TODO: detail of the API)
- maybe long-running HTTP server, 
- or the most naive more, parse the configuration file ann get the parameters from there
"""


class MasterNode:
    def __init__(self, ports=None):
        """
        Here we need six fixed ports for the following:
            - master node: push to mappers, pull from reducers, 
            - shuffler: push to reducers, pull from mappers
            - control (mainly to tell workers which type of work to do and track the progress)
        """     

        if ports is None:
            ports = [5555, 5556, 5557, 5558, 5559, 6000]

        self.masterPull, self.masterPush, self.shufflerPull, self.shufflerPush, self.controlPull, self.controlPush = ports
        self.mapperPull = self.masterPush
        self.reducerPull = self.shufflerPush
        self.reducerPush = self.masterPull
        self.mapperPush = self.shufflerPull
        
        self.numMappers = 0
        self.numReducers = 0


    def initiate(self, numMappers, numReducers):
        """
        Initiate the cluster, start the mappers and reducers processes
        
        The master node need to pass the following information to the mappers:
        - the address of the master node push assigned data
        - the address of the shuffler pull sockets for the mappers
        - the ID of the mapper 

        The master node need to pass the following information to the reducers:
        - the address of the master node pull (to get the final result and combine)
        - the address of the shuffler push sockets to the reducers
        - the ID of the reducer

        The master node will also start the shuffle process, passing
        - the address of the master
        - the address it will be pulling the data from mappers
        - the address it will be pushing the data to the reducers

    """
        self.numMappers = numMappers
        self.numReducers = numReducers

        context = zmq.Context()

        self.controlPullSocket = context.socket(zmq.PULL)
        self.controlPullSocket.bind(zmq_addr(self.controlPull))

        self.controlPushSocket = context.socket(zmq.PUB) # we need to send the work type to all workers, not push-pull pattern anymore
        self.controlPushSocket.bind(zmq_addr(self.controlPush))


        self.pushSocket = context.socket(zmq.PUSH)
        self.pushSocket.bind(zmq_addr(self.masterPush))

        self.pullSocket = context.socket(zmq.PULL)
        self.pullSocket.bind(zmq_addr(self.masterPull))

        # self.controlSocket = context.socket(zmq.ROUTER)
        # self.controlSocket.bind(zmq_addr(self.control))


        # start the shuffle process
        subprocess.Popen(['python', 
                          'shuffler.py', 
                          zmq_addr(self.shufflerPull), 
                          zmq_addr(self.shufflerPush), 
                          str(self.numMappers),
                          str(self.numReducers)])

        time.sleep(2) # wait for the shuffler to start

        # start the mappers and reducers
        for i in range(numMappers):    
            subprocess.Popen(['python', 
                              'mapper.py', 
                              zmq_addr(self.mapperPull, interface='localhost'), 
                              zmq_addr(self.mapperPush, interface='localhost'), 
                              zmq_addr(self.controlPush, interface='localhost'),
                              zmq_addr(self.controlPull, interface='localhost'),
                              str(i)])
        for j in range(numReducers):
            subprocess.Popen(['python', 
                              'reducer.py', 
                              zmq_addr(self.reducerPull, interface='localhost'), 
                              zmq_addr(self.reducerPush, interface='localhost'),
                              zmq_addr(self.controlPush, interface='localhost'),
                              zmq_addr(self.controlPull, interface='localhost'),
                              str(j)])
                   
        print(f"The cluster {int(os.getpid())} has been initiated")       
        return int(os.getpid())
    
    def mapreduce(self, inputData, outputLocation, workType):
        """
        This function is responsible for 
            - splitting the context and 
            - sending it to the mappers
        Here for load balancing, we use multithreading to send the data to the mappers instead of sending the data to the mappers one by one according to the order of list which will cause some mappers to be idle
        """

        # before starting the process, we need to send work type to the mappers and reducers
        time.sleep(1) # wait for the workers to start
        self.controlPushSocket.send(workType.encode())

        readyWorkers = 0
        numWorkers = self.numMappers + self.numReducers

        while readyWorkers < numWorkers:
            message = self.controlPullSocket.recv().decode()
            if message == 'ready':
                readyWorkers += 1

        # for i in range(numWorkers):
            # workerID, _ = self.controlSocket.recv_multipart()
            # print(f"Sending work type to {workerID}")
            # self.controlSocket.send_multipart([workerID, workType.encode()])

        # collect ready messages from the workers, when all ready, which means all know what they do
        # start sending the data
        # while readyWorkers < numWorkers:
        #     workerID, message = self.controlSocket.recv_multipart()
        #     if message == b'ready':
        #         readyWorkers += 1

        print("All workers are ready!")
        

        with open(inputData, 'r') as f:
            data = f.readlines()

        numLines = len(data)
        numLinesPerMapper = numLines // self.numMappers

    
        def sendToMapper(data):
            data = "".join(data)
            self.pushSocket.send_string(data)

        for i in range(self.numMappers):
            start = i * numLinesPerMapper
            end = (i + 1) * numLinesPerMapper if i != self.numMappers - 1 else numLines # sorry for the last mapper, maybe more lines than the others
            thread = threading.Thread(target=sendToMapper, args=(data[start:end],))
            thread.start()


        results = []
        # receive a predefined number of messages from the reducers
        for i in range(self.numReducers):
            message = self.pullSocket.recv_string()
            result = json.loads(message)
            results.append(result)
        with open(outputLocation, 'w') as f:
            for result in results:
                for key, value in result.items():
                    f.write(f"{key}: {value}\n")

    """
    destroy the cluster
    """
    def destroy(self):
        pass


if __name__ == '__main__':
    # config = input('Enter the configuration file: ')
    if len(sys.argv) != 2:
        print("Usage: python mapreduce.py <config>")
        sys.exit(1)
    
    config = sys.argv[1]
    with open(config, 'r') as f:
        config = json.load(f)
    
    """
    We need to pass the following information to the master node:
    - the number of mappers
    - the number of reducers
    - the type of work
    - the input data
    - the output location
    - the ports for the master node and the shuffler
    """

    master = MasterNode(config['ports'])
    master.initiate(config['numMappers'], config['numReducers'])
    master.mapreduce(config['inputData'], config['outputLocation'], config['workType'])
    master.destroy()
    
    

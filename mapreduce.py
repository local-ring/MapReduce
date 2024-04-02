import zmq
import json

import subprocess   
import os
import threading

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
    def __init__(self,
                 numMappers,
                 numReducers,
                 Addrs):
        if len(Addrs) != numMappers + numReducers + 1:
            raise ValueError('The number of addresses does not match the number of mappers and reducers')
        self.mapperAddrs = Addrs[:numMappers]
        self.reducerAddrs = Addrs[numMappers:]       
        self.shufflerAddr = Addrs[-1]
                
        self.clusterID = self.initiate(numMappers, numReducers, Addrs)


        if self.workType not in ['word-count', 'inverted-index']:
            raise ValueError('The work type is not supported')
        elif self.workType == 'word-count':
            self.mapFunction = 'map_wc'
            self.reduceFunction = 'reduce_wc'
        else:
            self.mapFunction = 'map_ii'
            self.reduceFunction = 'reduce_ii'



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
    - the address it will be pulling the data from mapper
    - the address it will be pushing the data to the reducers

    """
    def initiate(self, numMappers, numReducers):
        for i in range(numMappers):
            subprocess.Popen(['python', 
                              'mapper.py', 
                               self.mapperAddrs[i], 
                               self.shufflerAddr,
                               str(i)])

        for i in range(numReducers):
            subprocess.Popen(['python', 
                              'reducer.py', 
                               self.reducerAddrs[i],                   
                               str(i)])
            
        # start the shuffle process
        self.shuffleProcess = subprocess.Popen(['python', 'shuffler.py', self.shufflerAddr, str(numReducers)])
        
        print(f"The cluster {int(os.getpid())} has been initiated")

        return int(os.getpid())
    
    def mapreduce(self, inputData, mapFunction, reduceFunction, outputLocation):
        # send the map function to the mappers
        for i in range(len(self.mapperAddrs)):
            threading.Thread(target=self.sendtoMapper, args=(mapFunction, i)).start()
        
        # send the reduce function and output location to the reducers
        for i in range(len(self.reducerAddrs)):
            threading.Thread(target=self.sendtoReducer, args=(reduceFunction, outputLocation, i)).start()
        
        # send 

        # once receiving the ready signal from the mappers, start sending the data to the mappers
    """
    This function is responsible for 
        - splitting the context and 
        - sending it to the mappers

    Here for load balancing, we use multithreading to send the data to the mappers instead of sending the data to the mappers one by one according to the order of list which will cause some mappers to be idle
    """
    def map(self, context):
        with open(context, 'r') as f:
            data = f.readlines()
        numLines = len(data)
        numLinesPerMapper = numLines // len(self.mapperAddrs)
        threads = []
        for i in range(len(self.mapperAddrs)):
            start = i * numLinesPerMapper
            end = (i + 1) * numLinesPerMapper
            if i == len(self.mapperAddrs) - 1: # sorry to the last mapper, more data for you (sometime less)
                end = numLines
            thread = threading.Thread(target=self.sendtoMapper, args=(data[start:end], i))
            thread.start()
            threads.append(thread)
        
    # send the assigned data to the mappers
    def sendtoMapper(self, data, mapperID):
        context = zmq.Context() # create a context
        socket = context.socket(zmq.PUSH)
        socket.connect(self.mapperAddrs[mapperID])
        for line in data:
            socket.send_string(line)
        socket.close()
        context.term() # close the context
        

    def sendtoReducer(self, data, reducerID):
        pass



    """
    destroy the cluster
    """
    def destroy(self):
        pass

def initCluster(numMappers, numReducers):
    master = MasterNode(numMappers, numReducers)
    return master

if __name__ == '__main__':
    config = input('Enter the configuration file: ')
    with open(config, 'r') as f:
        config = json.load(f)
    
    master = initCluster(config['numMappers'], config['numReducers'])
    

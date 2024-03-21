import zmq

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
- control and coordinate all other processes (TODO: be more specific)

The user can only interact with the master node through a well-defined API (TODO: detail of the API)
- maybe long-running HTTP server, 
- or the most naive more, parse the configuration file ann get the parameters from there
"""

class MasterNode:
    def __init__(self,
                 numMappers,
                 numReducers):
        self.numMappers = numMappers
        self.numReducers = numReducers

    # TODO: look at the configuration options in Hadoop
    def run(self,
            inputFiles,
            mapFunction,
            reduceFunction,
            outputDir):
        pass


    """
    destroy the cluster
    """
    def destroy(self):
        pass

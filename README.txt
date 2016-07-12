Typing "make" will compile both the parallel and serial program. 

Run commands:

To run the serial program type:
$ serial <graphFile>

To run the parallel program type:
$ ./pagerank <graph file> <Partition File>
-use "4M-graph.txt.part.4" or "200K-graph.txt.part.4" as the partition file. Partition file determines the amount of threads to be used.  

Example:
$ ./pagerank 4M-graph.txt 4M-graph.txt.part.4

The results will be printed out in "pagerank.result".

I start my time right before I start all threads to loop and calculate the normal values. The start time can be found on line 308. Before line 308, the data structures are created and populated. Once everthing is populated then the computations can be performed, which is where the timer is started. 

The timer ends right when the change in normal values is less than 10^{-7} and all threads are finished. The finish can be found on line 317.
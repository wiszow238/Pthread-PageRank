CC=mpicc

all: serial pagerank

serial: pagerank_serial.c
				$(CC) pagerank_serial.c -o serial

pagerank: pagerank_thread.c
				$(CC) pagerank_thread.c -o pagerank

clean:
				rm pagerank serial
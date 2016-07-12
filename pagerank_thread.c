#include <stdio.h> 
#include <string.h>
#include <stdlib.h> 
#include <pthread.h>
#include <ctype.h>
#include <math.h>
#include <stdbool.h>
#include <sys/time.h>

typedef struct { 
   double* values;   //Array of node values in one row
   long int* col;    //Array of col_inds in one row
   long int vcount;  //Number of nodes in one row
   int rowId;
} rowData;

struct row {
   rowData* rowary;  //Stores data for each row
   int rcount;       //Number of rows
};

struct pNode {
   int val;
   struct pNode *next;
   struct pNode *last;
};

struct cNode {
   int val;
   int numofReq;
   struct pNode *reqLink;
   struct cNode *next;
};

typedef struct {
   double val;
   int rowId;
}  mValue;

int max_threads, numberofRows;
void *compute (void *);
pthread_mutex_t sumLock;

//http://pages.cs.wisc.edu/~travitch/pthreads_primer.html
pthread_barrier_t barr;

double *multiplyArray;
double totalSum, norm;
//double *totalSumAtEachPartition;
struct row *partitionedMatrixData;

int main(int argc, char **argv) { 
   struct timeval start, end;

   //Read partition file
   char *dot = strrchr(argv[2], '.');
   if (!dot || dot == argv[2]) dot = "";
   else dot = dot + 1;
   
   int partitionCount = 0;
   //determine the number of partitions
   if (isdigit(dot[0])) {
      partitionCount = atoi(dot);
   } else {
      printf("INVALID PARTITION FILE\n");
      exit(1);
   }

   max_threads = partitionCount;
   pthread_t p_threads[max_threads];
   pthread_attr_t attr;
   pthread_attr_init(&attr);
   pthread_mutex_init(&sumLock, NULL);

   pthread_barrier_init(&barr, NULL, max_threads);

   FILE *pFile = fopen(argv[1], "r");
   if (pFile==NULL) {
      printf("File error");
      fputs ("File error", stderr); 
      exit(1);
   }

   int size;
   fseek (pFile, 0, SEEK_END);
   size = ftell(pFile);
   rewind (pFile);

   int i = 0;
   char st[20];
   int readRowsPtrs = 0;
   long int* rownum = (long int *) malloc(size * sizeof(long int));

   printf("READING LAST LINE\n");
   while (fscanf(pFile, "%20s", st) != EOF) {
      if (readRowsPtrs == 1) {
         long int v;
         v = strtol(st, NULL, 0);
         rownum[i] = v;
         i++;
      }

      if (strcmp(st, "row_ptrs:") == 0) {
         readRowsPtrs = 1;
      }
   }
   rewind(pFile);
   printf("LAST LINE READ\n");

   struct row matrixdata;
   matrixdata.rowary = malloc(i * sizeof(rowData));
   matrixdata.rcount = i-1;
   
   int *outGoingEdgeCount = (int *) malloc(matrixdata.rcount * sizeof(int));

   int c;
   for (c=0; c < matrixdata.rcount; c++) { //i is the total amount of rows in the matrix
      long int rowSize;
      rowSize = rownum[c+1] - rownum[c];

      matrixdata.rowary[c].values = malloc(rowSize * sizeof(double));
      matrixdata.rowary[c].col = malloc(rowSize * sizeof(long int));
      matrixdata.rowary[c].vcount = rowSize;

      outGoingEdgeCount[c] = 0;
   }
   free(rownum);

   i = 0;
   c = 0;      //Matrix row incrementor
   char str[20];
   int newline = 0;
   char ch;
   rewind(pFile);

   printf("READING col_inds LINE\n");
   readRowsPtrs = 0;
   while (fscanf(pFile, "%20s", st) != EOF) {
      if (strcmp(st, "row_ptrs:") == 0) {
         break;
      }

      if (readRowsPtrs == 1) {
         int v;
         v = strtol(st, NULL, 0);
         outGoingEdgeCount[v] = outGoingEdgeCount[v] + 1;
      }

      if (strcmp(st, "col_inds:") == 0) {
         readRowsPtrs = 1;
      }
   }
   rewind(pFile);
   printf("col_inds LINE READ\n");

   printf("POPULATING MATRIX\n");
   while (fscanf(pFile, "%s%c", str, &ch) != EOF && newline < 3) {
      if(isdigit(str[0])) {
         if(i == matrixdata.rowary[c].vcount) {
            i = 0;
            c++;
         }
         if(c > matrixdata.rcount) {
            printf("Number of rows is more than allocated rows\n");
            exit(1);
         }

         //Determine what the current values represent
         if(newline == 1) {
            //Convert String values to double strtod 
            double v;
            v = strtod(str, NULL);
            matrixdata.rowary[c].values[i] = v;
         } else if (newline == 2) { //if(strcmp(header, "col_inds:") == 0) {
            //Convert String values to int strtol
            long int v;
            v = strtol(str, NULL, 0);
            matrixdata.rowary[c].col[i] = v;
            matrixdata.rowary[c].values[i] = matrixdata.rowary[c].values[i]/outGoingEdgeCount[v];
         } 
         i++; //Counts the total number of nodes
      } else {
         c=0;
         i=0;
         newline++;
      }
      
      if (ch == '\n') { //END OF LINE
         c = 0;
         i = 0;
      }
   }

   printf("END OF FILE\n");
   fclose(pFile);
   printf("FILE CLOSED\n");
   free(outGoingEdgeCount);

   //Array of linkedlists. Array determines partition location. Linkedlist is nodes belonging to partition
   struct pNode *partitionArray = malloc(partitionCount * sizeof(struct pNode));
   int *sizeofEachPartition= (int *) malloc(partitionCount * sizeof(int));

   //Initialize linked list
   for (i=0;i<partitionCount;i++){
      partitionArray[i].next = 0;
      partitionArray[i].last = 0;
      partitionArray[i].val = -1;
      sizeofEachPartition[i] = 0;
   }

   //Open partition file. Parameter 2 is partition file
   FILE *partFile = fopen(argv[2], "r");
   if (partFile==NULL) {
      printf("File error");
      fputs ("File error", stderr); 
      exit(1);
   }

   i=0;
   c=0;
   //Read Partition file and store where rows should be broken up into
   //c is the partition location. i is the node or line number. 
   printf("READING PARTITION FILE\n");
   while (fscanf(partFile, "%d", &c) != EOF) {
      //c is the processor number
      //Go to processor linked list
      struct pNode *newNode = &partitionArray[c];
      struct pNode *firstNode = &partitionArray[c];
      

      if (firstNode->last != 0) {
         //Go to last node
         newNode = firstNode->last;
      } 
      if (newNode->val != -1) {
         if (newNode != 0) {
            while (newNode->next != 0){
               newNode = newNode->next;
            }
         }

         //Add new node to linked list
         newNode->next = malloc(sizeof(struct pNode));
         newNode = newNode->next;
         if (newNode == 0){
            printf("NO MEMORY\n");
            exit(1);
         }
      }
      
      newNode->next = 0;
      newNode->val = i; //i is the line number
      firstNode->last = newNode;
      sizeofEachPartition[c] = sizeofEachPartition[c] + 1;
      i++;
   }
   printf("FINISHED READING PARTITION FILE\n");
   fclose(partFile);
   
   //Contains partitioned matrix Data
   partitionedMatrixData = malloc(partitionCount * sizeof(struct row));
   for (i=0; i<partitionCount; i++) {
      partitionedMatrixData[i].rowary = malloc(sizeofEachPartition[i] * sizeof(rowData));
      partitionedMatrixData[i].rcount = sizeofEachPartition[i];
   }
   free(sizeofEachPartition);

   printf("PARTITION MATRIX\n");
   //Partition the Matrix. Going through each partition
   for (i=0;i<partitionCount;i++){
      struct pNode *newNode = &partitionArray[i];
      //newNode->val contains the row number (node id)
      int rc = 0;
      while (newNode != NULL) {
         partitionedMatrixData[i].rowary[rc].values = malloc(matrixdata.rowary[newNode->val].vcount * sizeof(double));
         partitionedMatrixData[i].rowary[rc].col = malloc(matrixdata.rowary[newNode->val].vcount * sizeof(long int));
         for (c=0; c<matrixdata.rowary[newNode->val].vcount; c++){
            partitionedMatrixData[i].rowary[rc].values[c] = matrixdata.rowary[newNode->val].values[c]; 
            partitionedMatrixData[i].rowary[rc].col[c]    = matrixdata.rowary[newNode->val].col[c];
         }
         partitionedMatrixData[i].rowary[rc].vcount = matrixdata.rowary[newNode->val].vcount;
         partitionedMatrixData[i].rowary[rc].rowId  = newNode->val;
         newNode = newNode->next;
         rc++;
      }
   }
   numberofRows = matrixdata.rcount;
   for (c=0; c < matrixdata.rcount; c++) {    
      free(matrixdata.rowary[c].col);
      free(matrixdata.rowary[c].values);
   }
   free(matrixdata.rowary);
   free(partitionArray);
   printf("MATRIX PARTITIONED\n");
   
   printf("INITIALIZE MULTIPLY ARRAY\n");
   //multiplyArray = (double *) malloc(numberofRows + (numberofRows * 50) * sizeof(double));
   multiplyArray = (double *) malloc(numberofRows * sizeof(double));
   for (c=0; c < numberofRows; c++) {
      multiplyArray[c] = 1/(double)numberofRows;
   }
   printf("MULTIPLY ARRAY CREATED\n");
   printf("CALCULATING NORMS\n");
   
   //totalSumAtEachPartition = (double *) malloc(partitionCount * sizeof(double));
   totalSum = 0;
   gettimeofday(&start, NULL);
   for(i=0; i<max_threads; i++) {
      int *arg = malloc(sizeof(*arg));
      *arg = i;
      pthread_create(&p_threads[i], &attr, compute, arg);
   }
   for (i=0; i<max_threads; i++) {
      pthread_join(p_threads[i], NULL);
   }
   gettimeofday(&end, NULL);
   //free(totalSumAtEachPartition);
   
   printf("normalizedValue = %f\n", norm);
   long runTime = (end.tv_sec * 1000000 + end.tv_usec) - (start.tv_sec * 1000000 + start.tv_usec);
   printf("TIME: %ld\n", runTime);

   FILE *fp;
   fp = fopen("pagerank.result", "w+");
   fprintf(fp,"time: %ld\n", runTime);
   fprintf(fp,"node_Id pagerank \n");
   for (c=0; c < numberofRows; c++) {
      fprintf(fp, "%d %.12f\n", c, multiplyArray[c]);
   }
   fclose(fp);
   
   for (i=0; i<partitionCount; i++) {
      for (c=0; c<partitionedMatrixData[i].rcount; c++) {
         free(partitionedMatrixData[i].rowary[c].col);
         free(partitionedMatrixData[i].rowary[c].values);
      }
      free(partitionedMatrixData[i].rowary);
   }
   free(partitionedMatrixData);
   free(multiplyArray);
}

void *compute(void *s){
   int pnum = *((int *) s);
   double *resultArray = (double *) malloc(numberofRows * sizeof(double));
   double *normalValue = (double *) malloc(2 * sizeof(double));
   int c, i;
   // int a = 0;
   struct row *pData;
   double partitionSum = 0;
   normalValue[1] = 0;
   pData = &partitionedMatrixData[pnum];

   // totalSumAtEachPartition[pnum] = 0;
   do {
      //pData = &partitionedMatrixData[pnum];
      partitionSum = 0;
      for (c=0; c < pData->rcount; c++) {
         double rowSum = 0;
         //Loop through each column
         for (i=0; i < pData->rowary[c].vcount; i++) {
            //printf("pData->rowary[c].col[i]: %li\n", pData->rowary[c].col[i] + (pData->rowary[c].col[i]*255));
            // rowSum = rowSum + (pData->rowary[c].values[i] * multiplyArray[pData->rowary[c].col[i] + (pData->rowary[c].col[i]*50)]);
            rowSum = rowSum + (pData->rowary[c].values[i] * multiplyArray[pData->rowary[c].col[i]]);
         }
         resultArray[pData->rowary[c].rowId] = rowSum;
         partitionSum = partitionSum + pow(rowSum, 2);
      }
      // totalSumAtEachPartition[pnum] = partitionSum;

      pthread_mutex_lock(&sumLock);
      totalSum = totalSum + partitionSum;
      pthread_mutex_unlock(&sumLock);

      pthread_barrier_wait(&barr);
      if (pnum == 0){
         // double totalSum2 = 0;
         // for (i=0; i<max_threads; i++) {  //Alternative is to have a global sum and use locks. 
         //    totalSum2 = totalSum2 + totalSumAtEachPartition[i];
         //    totalSumAtEachPartition[i] = 0;
         // }
         // norm = sqrt(totalSum2);

         norm = sqrt(totalSum);
         totalSum = 0;
      }
      // a++;
      normalValue[0] = normalValue[1];

      pthread_barrier_wait(&barr);
      
      normalValue[1] = norm;
      for (c=0; c<pData->rcount; c++) {
         multiplyArray[pData->rowary[c].rowId] = resultArray[pData->rowary[c].rowId];
         resultArray[pData->rowary[c].rowId] = 0;
      }

      pthread_barrier_wait(&barr);
   } while(normalValue[0] == 0 || ((normalValue[0] - normalValue[1]) > 10E-7) || ((normalValue[1] - normalValue[0]) > 10E-7));
   free(s);
   free(normalValue);
   free(resultArray);
   pthread_exit(0);
}
# Papadopo - A parallel search engine on a set of text documents #

### What is this repository for? ###
This repository provides a parallel solution for two information retrival operations:
* building an inverted index
* processing a query (using the [vector space model](https://en.wikipedia.org/wiki/Vector_space_model))

### How do I get set up? ###
All you need is java 7 (or later)

---

## How it words: ##

### Index Building ###
##### Phase1) Creating Records \<Term, Document, Frequency\>: #####
* Read next document and store its words in an ArrayList
* Cut the ArrayList into almost equal pieces and pass them to IndexWorker threads
  * Each IndexWorker thread will create records of \<Term, Document, Frequency\>
* Write the records returned from each worker to disk (There will be as many Record files as there
are Cores-Threads)
* Repeat until there are no more documents

##### Phase2) Sorting and Merging the Record files: #####
* Pass to each Sorter Thread one Record File so that it creates sorted files
  * Approximation of memory limitation(similar management as described below in phase 3)
  * QuickSort records each time the memory limit is reached and store in as many files as needed until all records of the file have been sorted
  * Return the filenames of the produced sorted files so that they get merged into 1
* Merge the sorted files produced by the threads
  * Create half as many Merge Threads as there are files to merge
  * Pass 2 sorted files to each Merge Thread so that it returns 1 merged sorted file
  * Continue until 1 merged file has been produced in a single phase
  
##### Phase3) Building the index: #####
* Approximation of memory limitation using number of total records
  * Check the size of the file with the sorted records
  * Check available memory
  * Set a limit of records read from file which when reached the part of the index currently in memory will be stored to disk (while removing last term’s occurrence and placing it in the next part of the index so that one term’s complete occurrence list is in one and only one part of the index)
* If memory limitation does not let us hold the index in memory
  * Read sorted records from file and hash them to index until limit is reached
  * Store the part of the index to disk
  * Continue until all sorted records have been read
* If index can be built completely in memory
  * Read all sorted records and hash them to index
  
---
  
### Query Processing ###
##### Phase1) Computing the weight vectors: #####
For the documents:
* Read a document and store all its words in an ArrayList.
* Divide this ArrayList to “chunks” of words and give a “chunk” to each thread.
* Pass a shared HashMap (vector) and a SharedDouble (norm) to each thread.
* A thread fills the shared HashMap (vector) with the weights of the words that appear in the “chunk” that it took.
* Once all threads are joint, the shared structures contain the vector and it's norm

The above operations are performed for all documents (only the norms are stored)

For the query:

The query's weight vector and norm are computed in the exact same way as a document's vector and
norm with the only difference that “tf” is computed using a HashMap that contains the frequency of
each query word inside the query (the weight vector and its norm, are both stored).

##### Phase2) Computing similarities: #####
* Divide the query in “chunks” of words and give a “chunk” to each thread.
* Pass a shared HashMap (that maps document to similarity) to each thread.
* A thread reads all words of its “chunk” and for each document in which a query word appears, the thread adds something to this document's similarity with the query.
* Once all threads are joint, the shared HashMap contains the computed similarities.

---

### Extra Feature: Query expansion using stemming ###
If first argument of the program when launched is “True” (ignoring case), a stemmer will be used using the dictionary of WordNet.

For example:
  <pre>query         : "shining"
expanded query: "shining shine shin"</pre>

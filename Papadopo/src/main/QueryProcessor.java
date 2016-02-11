package main;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.Map.Entry;
import java.util.StringTokenizer;

public class QueryProcessor {
		
	public InvertedIndex index;
	public HashMap<Integer,Double> norms;
	
	/**Maps docID to similarity (with a query)*/
	public Map<Integer,Double> similarity;
	
	public int queryID = -1;
	public double queryNorm;
	public Map<String,Double> queryVector;
	
	/**A simple frequency counter for every word in the query*/
	public HashMap<String,Integer> queryFrequencies;
	
	public QueryProcessor()
	{
		
		norms = new HashMap<Integer,Double>();
		similarity = new HashMap<Integer,Double>();
		index = new InvertedIndex();
		similarity = Collections.synchronizedMap(new HashMap<Integer,Double>());
		
		//Make the Inverted Index
		index.buildIndex();
	}
	
	/**
	 * Counts the word frequency for each word in the query and computes the "vector" and norm of the query.
	 * @param queryString : the full text of the query (all it's words)
	 * @return the total number of words, the query has
	 * */
	public int setQuery(List<String> query){	
		
		//Count the frequency of each word inside the query
		queryFrequencies = new HashMap<String,Integer>();

		//Count the words of the query
		int totalWords = 0;
		Iterator<String> words = query.iterator();
		while(words.hasNext()){
			String word = words.next();
			if(queryFrequencies.containsKey(word)){
				int oldFreq = queryFrequencies.get(word);
				queryFrequencies.put(word, oldFreq+1);
			}else{
				queryFrequencies.put(word, 1);
			}
			totalWords++;
		}
		
		int threads = 2;
		
		//Declare the "shared" HashMap and norm in which the threads are going to write.
		Map<String,Double> sharedVector = Collections.synchronizedMap(new HashMap<String,Double>());
		SharedDouble sharedNorm = new SharedDouble();
		
		distributeToThreads("query vector",queryID,query,totalWords,sharedVector,sharedNorm,threads);
		
		queryNorm = Math.sqrt(sharedNorm.get());
		queryVector = sharedVector;
		System.out.println("vector[query]: "+queryVector);
		System.out.println("norm[query]: squareRoot(Σ(weight^2)) = "+queryNorm);
		
		return totalWords;
	}
	
	/**
	 * Starts some threads each one of which, executes computations on a given "chunk" of the document and stores it's results in the shared structures that are passed as parameters.
	 * @param whatComputation : Accepted values are: "document vector" or "query vector" or "similarities". This parameter defines what operation is to be applied on the data.
	 * @param docID : the id of the document
	 * @param document : a list of all words inside a document (or query)
	 * @param totalWords : the total number of words this document has (the size of the "document" list)
	 * @param sharedVector : the map where all threads are going to write their computations simultaneously
	 * @param sharedNorm : a double number that is incremented by any thread
	 * @param threads : the number of threads we want to distribute the computations to
	 * */
	@SuppressWarnings("unchecked")
	public <K,V> void distributeToThreads(String whatComputation,int docID ,List<String> document, int totalWords, Map<K,V> sharedVector, SharedDouble sharedNorm, int threads){  
		
		//Define how many words each thread will take.
		int wordsPerThread = totalWords/threads;
		int wordsForFirstThread = wordsPerThread;
		wordsForFirstThread += (totalWords%threads);
		int start=0;
		int end=wordsForFirstThread;
		
		//Initialize the threads
		ArrayList<Thread> myThreads = new ArrayList<Thread>();
		
		//Give a portion to the First thread
		System.out.println("\n---------------------------------------------------------------\n"+"portion: ["+start+","+end+")");
//		System.out.println("Thread1 takes: "+document.subList(start, end));
		
		if(whatComputation.equals("document vector")){
			
			myThreads.add(new Thread(new VectorChunkCalculator((Map<String,Double>)sharedVector, sharedNorm, document.subList(start, end), docID , index)));
		}else if(whatComputation.equals("query vector")){
			
			myThreads.add(new Thread(new QueryVectorChunkCalculator((Map<String,Double>)sharedVector, sharedNorm, document.subList(start, end), docID , index,queryFrequencies)));
		}else if(whatComputation.equals("similarities")){
			
			myThreads.add(new Thread(new SimilaritiesChunkCalculator((Map<Integer,Double>)sharedVector, document.subList(start, end), index, norms, queryVector, queryNorm)));
		}
		
		//if the First thread left some words
		if(end<totalWords){
			
			//Give a portion to the Rest of the threads
			for(int t=0;t<threads-1;t++){
				
				start = end;
				end += wordsPerThread;
				System.out.println("\n---------------------------------------------------------------\n"+"portion: ["+start+","+end+")");
//				System.out.println("Thread"+(t+2)+" takes: "+document.subList(start, end));
				
				if(whatComputation.equals("document vector")){
					
					myThreads.add(new Thread(new VectorChunkCalculator((Map<String,Double>)sharedVector, sharedNorm, document.subList(start, end), docID , index)));
				}else if(whatComputation.equals("query vector")){
					
					myThreads.add(new Thread(new QueryVectorChunkCalculator((Map<String,Double>)sharedVector, sharedNorm, document.subList(start, end), docID , index,queryFrequencies)));
				}else if(whatComputation.equals("similarities")){
					
					myThreads.add(new Thread(new SimilaritiesChunkCalculator((Map<Integer,Double>)sharedVector, document.subList(start, end), index, norms, queryVector, queryNorm)));
				}
			}
		}
		
		//Start all threads
		Iterator<Thread> threadsIt = myThreads.iterator();
		while(threadsIt.hasNext()){
			
			Thread t = threadsIt.next();
			t.start();
		}
		
		//Join all threads.
		threadsIt = myThreads.iterator();
		while(threadsIt.hasNext()){
			
			Thread t = threadsIt.next();
			try {t.join();}
			catch (InterruptedException e) {}
		}
	}
	
	public static void main(String[] args){
		
		System.out.println("Remember: N  = #documents (without query)\n          nt = #documents that contain word (without query)");
		System.out.println("Remember: weight = [1+ln(freq)]*ln(1+N/nt)\n\n");
		
		int threads = 4;
		int threadsForSimilarity = 2;
		
//		String docs[] = 
//		{			
//			"ο κομήτης του Χάλλεϋ μας επισκέπτεται περίπου κάθε εβδομήντα έξι χρόνια",
//			"ο κομήτης του Χάλλεϋ ανακαλύφθηκε από τον αστρονόμο Έντμοντ Χάλλεϋ",
//			"ένας κομήτης διαγράφει ελλειπτική τροχιά",
//			"ο πλανήτης Άρης έχει δύο φυσικούς δορυφόρους το Δείμο και το Φόβο",
//			"ο πλανήτης Δίας έχει εξήντα τρείς γνωστούς φυσικούς δορυφόρους",
//			"ο Ήλιος είναι ένας αστέρας",
//			"ο Άρης είναι ένας πλανήτης του ηλιακού μας συστήματος"
//		};

		
		QueryProcessor qp = new QueryProcessor();
		
		//Set the query
		ArrayList<String> query = new ArrayList<String>();
		query.add("comet");
		query.add("orbit");
		int totalQueryWords = qp.setQuery(query);
		
		System.out.println("================================================================================================================================\nComputing vector weights and norm for each document. (a vector contains weights for ALL words of the document) ..."); 	
		
		//For each document
		for(int docID=1;docID<qp.index.getNumberOfDocuments();docID++){
			
			String filename=docID+".txt";// Reading files named [id].txt ( example : 1.txt )
			
			//Add all words of this document to an ArrayList
			ArrayList<String> words = new ArrayList<String>();
			Scanner scanner;
			try {
				scanner = new Scanner(new File(filename));
				while(scanner.hasNext()){
					String word = scanner.next();
					words.add(qp.index.processWord(word));
				}
			} catch (FileNotFoundException e) {}
			
//			System.out.println("\n\n\n\n\n\n"+" doc"+docID+"\n"+words);
			
			//Compute the total number of words this document has.
			int totalWords = qp.index.getSizeOfDocument(docID);
			
			//Declare the "shared" HashMap and norm in which all threads will write (this HashMap is the "vector" that contains word weights inside this document).
			Map<String,Double> sharedVector = Collections.synchronizedMap(new HashMap<String,Double>());
			SharedDouble sharedNorm = new SharedDouble();

			//Distribute the vector computation to some threads
			qp.distributeToThreads("document vector",docID, words, totalWords, sharedVector, sharedNorm, threads);
			
			//Store norm (dump the vector)
			qp.norms.put(docID, Math.sqrt(sharedNorm.get()));
			
			System.out.println("vector["+docID+"]: "+sharedVector);
			System.out.println("norm["+docID+"]: squareRoot(Σ(weight^2)) = "+qp.norms.get(docID));
			
		}
		
		System.out.println("\n\n\n================================================================================================================================\nComputing similarity ...\n");
		
		qp.distributeToThreads("similarities", -999, query, totalQueryWords, qp.similarity, null, threadsForSimilarity);
		
		System.out.println("\n\n\n\n");
		
		//Print similarities
		for (Entry<Integer, Double> similarities : qp.similarity.entrySet()){
			
			System.out.println("similarity("+similarities.getKey()+",query): "+similarities.getValue());
		}

	}

}

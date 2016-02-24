package main;

import java.io.BufferedInputStream;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.URL;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.StringTokenizer;
import java.util.Map.Entry;
import java.util.PriorityQueue;

import edu.mit.jwi.Dictionary;
import edu.mit.jwi.IDictionary;
import edu.mit.jwi.morph.WordnetStemmer;



public class QueryProcessor {
		
	private int cores = Runtime.getRuntime().availableProcessors();
	public int threadsForVector = 2*cores;
	public int threadsForSimilarity = 2*cores;
	public int threadsForQuery = 2*cores;
	public String queryAnswersFilename = "answers.txt";
	public String timeFilename = "time.txt";
	public static String pathToDocumentsFolder = "documents/";
	public static String pathToResultsFolder = "";
	
	public InvertedIndex index;
	public HashMap<Integer,Double> norms;	
	/**Maps docID to similarity (with a query)*/
	public Map<Integer,Double> similarity;
	public double queryNorm;
	public Map<String,Double> queryVector;
	/**A simple frequency counter for every word in the query*/
	public HashMap<String,Integer> queryFrequencies;
	public IDictionary dictionary;//(not loaded in RAM)
	//<RAM>//IRAMDictionary dictionary;  //(loaded in RAM) (JVM Heap size increase may be required)
	WordnetStemmer stemmer;
	boolean useQueryExpansion;
	
	public QueryProcessor(boolean useQueryExpansion)
	{
		norms = new HashMap<Integer,Double>();	
		index = new InvertedIndex();
		this.useQueryExpansion = useQueryExpansion;
		timeFilename = pathToResultsFolder.concat(timeFilename);
		queryAnswersFilename = pathToResultsFolder.concat(queryAnswersFilename);
		
		//Open the WordNet dictionary (this operation is not timed by the timer)
		if(useQueryExpansion){
		
			URL url;
			try {
				url = new URL ("file",null,"dict");
				dictionary = new Dictionary (url);
				dictionary.open();
		
				//<RAM>//dictionary = new RAMDictionary(url ,ILoadPolicy.NO_LOAD);
				//<RAM>//dictionary.load(true);
				//<RAM>//} catch (IOException | InterruptedException e1) {}
			} catch (IOException e1) {}
			
			//Initialize the stemmer (this operation is not timed by the timer)
			stemmer = new WordnetStemmer(dictionary);
		}
		
		//Start timing
		long start = System.nanoTime();
		
		//Make the Inverted Index
		index.buildIndex();
		
		//How much time did this operation take to complete (in seconds)?
		double totalTime = (System.nanoTime()-start)/Math.pow(10, 9);
		try(BufferedWriter writer=new BufferedWriter(new FileWriter(timeFilename,true)))
		{
			System.out.println("Index built in "+new DecimalFormat("#.##").format(totalTime)+" seconds");
			writer.write("Index built in "+new DecimalFormat("#.##").format(totalTime)+" seconds\n");
		}catch (IOException e) {e.printStackTrace();}
	}
	
	/**
	 * Computes the "vector" and norm of the query.
	 * @param queryID : the id of this query
	 * @param queryString : the full text of the query (all it's words)
	 * @param totalWords : the total number of words on this query
	 * */
	public void computeQueryVectorAndNorm(int queryID, List<String> query, int totalWords){	
		
		//Start timing
		long start = System.nanoTime();
		
		//Declare the "shared" HashMap and norm in which the threads are going to write.
		Map<String,Double> sharedVector = Collections.synchronizedMap(new HashMap<String,Double>());
		SharedDouble sharedNorm = new SharedDouble();
		
		distributeToThreads("query vector",queryID,query,totalWords,sharedVector,sharedNorm,threadsForQuery);
		
		queryNorm = Math.sqrt(sharedNorm.get());
		queryVector = sharedVector;
		System.out.println("vector[query]: "+queryVector);
		System.out.println("norm[query]: squareRoot(Σ(weight^2)) = "+queryNorm);
		
		//How much time did this operation take to complete (in seconds)?
		double totalTime = (System.nanoTime()-start)/Math.pow(10, 9);
		try(BufferedWriter writer=new BufferedWriter(new FileWriter(timeFilename,true)))
		{
			System.out.println("\nqueryID:"+queryID+" vector norm in "+new DecimalFormat("#.##").format(totalTime)+" seconds");
			writer.write("\nqueryID:"+queryID+" vector norm in "+new DecimalFormat("#.##").format(totalTime)+" seconds\n");
		}catch (IOException e) {e.printStackTrace();}
		
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
	
	/**
	 * Reads every document file, stores all it's words, distributes some words to each thread
	 * and each thread computes and fills a part of this document's weights vector (only the norm of the vector is stored).
	 * @param threads : the number of threads doing the vector computations.
	 * @param writeNormsToFile : if true, then the "norms" HashMap is written in a file named "norms" for future use (this way the norms are not re-computed in future use). 
	 * */
	public void computeAllDocumentNorms(int threads, boolean writeNormsToFile){
		
		//Start timing
		long start = System.nanoTime();
		
		//For each document
		for(int docID=1;docID<index.getNumberOfDocuments();docID++){
			
			String filename = pathToDocumentsFolder+docID+".txt";// Reading files named [id].txt ( example : 1.txt )
			
			//Add all words of this document to an ArrayList
			ArrayList<String> words = new ArrayList<String>();
			Scanner scanner;
			try {
				scanner = new Scanner(new File(filename));
				while(scanner.hasNext()){
					String word = scanner.next();
					words.add(index.processWord(word));
				}
			} catch (FileNotFoundException e) {}
			
			//Compute the total number of words this document has.
			int totalWords = index.getSizeOfDocument(docID);
			
			//Declare the "shared" HashMap and norm in which all threads will write (this HashMap is the "vector" that contains word weights inside this document).
			Map<String,Double> sharedVector = Collections.synchronizedMap(new HashMap<String,Double>());
			SharedDouble sharedNorm = new SharedDouble();

			//Distribute the vector computation to some threads
			distributeToThreads("document vector",docID, words, totalWords, sharedVector, sharedNorm, threads);
			
			//Store norm (dump the vector)
			norms.put(docID, Math.sqrt(sharedNorm.get()));
			
			System.out.println("vector["+docID+"]: "+sharedVector);
			System.out.println("norm["+docID+"]: squareRoot(Σ(weight^2)) = "+norms.get(docID));
		}
		
		//How much time did this operation take to complete (in seconds)?
		double totalTime = (System.nanoTime()-start)/Math.pow(10, 9);
		try(BufferedWriter writer=new BufferedWriter(new FileWriter(timeFilename,true)))
		{
			System.out.println("all vector norms: "+new DecimalFormat("#.##").format(totalTime)+" seconds");
			writer.write("all vector norms:  "+new DecimalFormat("#.##").format(totalTime)+" seconds\n");
		}catch (IOException e) {e.printStackTrace();}
		
		if(writeNormsToFile){
			
			//Write "norms" to binary file
			try(ObjectOutputStream out=new ObjectOutputStream(new FileOutputStream("norms")))
			{
				out.writeObject(norms);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
	
	/**
	 * Reads a binary file named "norms" that contains a HashMap with the norm of each document's vector.
	 * This function's only purpose is to prevent multiple computations of the vectors' norms if the program
	 * is executed multiple times.
	 * */
	@SuppressWarnings("unchecked")
	public void readAllDocumentVectorNorms(){
		
		try(ObjectInputStream in=new ObjectInputStream(new BufferedInputStream(new FileInputStream("norms"),16*1024)))
		{
			norms= (HashMap<Integer,Double>) in.readObject();//Bring it to memory
		} catch (Exception e) {e.printStackTrace();} 
	}
	
	/**
	 * Finds all stems (of any part of speech) for a given word (including the word itself).
	 * Since the given word is inclusive, null is never returned.
	 * @param word : a word to get stems for
	 * @return all distinct stems of this word (this word is included)
	 * */
	public ArrayList<String> getStems(String word){
		
		ArrayList<String> stems = new ArrayList<String>();
		
		//Get all stems of this word
		WordnetStemmer stemmer = new WordnetStemmer(dictionary);
		Iterator<String> stemsIt = stemmer.findStems(word, null).iterator();
		while(stemsIt.hasNext()){
			String stem = stemsIt.next();
			if(!stem.equals(word)){
				stems.add(stem);
			}
		}
		stems.add(word);
		return stems;
	}
	
	/**
	 * Reads and stores all query words,
	 * distributes them to some threads that compute the vector of this query,
	 * computes the similarity of this query with the documents (whose vector norm is known)
	 * and prints the topK similarities.
	 * @param queryID : the id of this query
	 * @param queryString : a String containing all words of the query
	 * @param topK : the total number of results we want to get (at max)
	 * */
	public void makeQuery(int queryID, String queryString, int topK){
				
		//Start timing
		long start = System.nanoTime();
		
		//Count the frequency of each word inside the query
		queryFrequencies = new HashMap<String,Integer>();
		
		//Add all words of this query to an ArrayList
		ArrayList<String> query = new ArrayList<String>();
		int totalQueryWords = 0;
		StringTokenizer line = new StringTokenizer(queryString);
		
		if(useQueryExpansion){
			while(line.hasMoreTokens()){
				String word = index.processWord(line.nextToken());
					
				//Stem each word of the query and process all resulting words
				Iterator<String> stems = getStems(word).iterator();
				while(stems.hasNext()){
					
					String stem = stems.next();
					
					//Process each stem word of the query
					query.add(index.processWord(stem));
					if(queryFrequencies.containsKey(stem)){
						int oldFreq = queryFrequencies.get(stem);
						queryFrequencies.put(stem, oldFreq+1);
					}else{
						queryFrequencies.put(stem, 1);
					}
					totalQueryWords++;
				}	
			}
		}else{
			while(line.hasMoreTokens()){
				String word = index.processWord(line.nextToken());
				
				//Process each word of the query (as is)
				query.add(index.processWord(word));
				if(queryFrequencies.containsKey(word)){
					int oldFreq = queryFrequencies.get(word);
					queryFrequencies.put(word, oldFreq+1);
				}else{
					queryFrequencies.put(word, 1);
				}
				totalQueryWords++;
			}
		}
		
		//Compute query vector's norm
		computeQueryVectorAndNorm(queryID,query,totalQueryWords);
		
		//Initialize similarities
		similarity = Collections.synchronizedMap(new HashMap<Integer,Double>());
		
		//Compute similarities
		System.out.println("\n\n\n\nComputing similarity ...\n");
		distributeToThreads("similarities", -999, query, totalQueryWords, similarity, null, threadsForSimilarity);
		System.out.println("\n\n\n\n");
		
		//Put similarities in a Max Heap (and remove them from the HashMap)
		PriorityQueue<Candidate> maxHeap = new  PriorityQueue<Candidate>(10, new MyComparator());
		Iterator<Entry<Integer, Double>> similarities = similarity.entrySet().iterator();
		while(similarities.hasNext()){
			
			Entry<Integer, Double> sim = similarities.next();
			System.out.println("similarity("+sim.getKey()+",query): "+sim.getValue());
			
			maxHeap.add(new Candidate(sim.getKey(),sim.getValue()));
			similarities.remove();
		}
		
		//Print top-k similarities
		System.out.println("Top"+topK+":");
		int k=0;
		try(BufferedWriter writer=new BufferedWriter(new FileWriter(queryAnswersFilename,true)))
		{
			writer.write("queryID:"+queryID+" top"+topK+":\n");
			while(!maxHeap.isEmpty() && k<topK){
				Candidate c = maxHeap.poll();
				System.out.println("doc"+c.getDocID()+": "+c.getSimilarity());	
				writer.write("doc"+c.getDocID()+": "+c.getSimilarity()+"\n");
				k++;
			}
			writer.write("\n");
		} catch (IOException e) {e.printStackTrace();}
		
		//How much time did this operation take to complete (in seconds)?
		double totalTime = (System.nanoTime()-start)/Math.pow(10, 9);
		try(BufferedWriter writer=new BufferedWriter(new FileWriter(timeFilename,true)))
		{
			System.out.println("queryID:"+queryID+" results in "+new DecimalFormat("#.##").format(totalTime)+" seconds");
			writer.write("queryID:"+queryID+" results in "+new DecimalFormat("#.##").format(totalTime)+" seconds\n");
		}catch (IOException e) {e.printStackTrace();}
		
	}
	
	/**
	 * A Comparator that implements reverse comparison between double numbers (example 2.0 < 1.5).
	 * This is used to change the PriorityQueue (Min Heap) into a (Max Heap).
	 * */
	public class MyComparator implements Comparator<Candidate> {

		@Override
		public int compare(Candidate ca, Candidate cb) {
			double a = ca.getSimilarity();
			double b = cb.getSimilarity();
			
			if(b-a > 0)		{return  1;}
			else if(b-a < 0){return -1;}
			else 			{return  0;}
		}
	    
	}
	
	/**
	 * A view of a document with it's similarity score with the query
	 * */
	public class Candidate{
		
		private int docID;
		private double similarity;
		
		public Candidate(int docID, double similarity){
			this.docID = docID;
			this.similarity = similarity;
		}
		public int getDocID(){return docID;}
		public double getSimilarity(){return similarity;}
	}
	
	public static void main(String[] args){
				
		System.out.println("Remember: N  = #documents (without query)\n          nt = #documents that contain word (without query)");
		System.out.println("Remember: weight = [1+ln(freq)]*ln(1+N/nt)\n\n");	
		boolean queryExpansion=false;
		if(args.length>0)
		{
			queryExpansion=Boolean.parseBoolean(args[0]);
		}
		QueryProcessor qp = new QueryProcessor(queryExpansion);
		//Compute All Document Norms
		System.out.println("\nComputing vector weights and norm for each document. (a vector contains weights for ALL words of the document) ..."); 	
		qp.computeAllDocumentNorms(qp.threadsForVector, false);//Do compute norms
		//qp.readAllDocumentVectorNorms();				//Do NOT compute norms (read them from existing file "norms")	
		
		//Read all queries
		String queryFilename="query.txt";
		Scanner scanner;
		try {
			scanner = new Scanner(new File(queryFilename));
			int totalQueries = Integer.valueOf(scanner.nextLine());
			
			while(scanner.hasNextLine()){
				
				String line = scanner.nextLine();
				StringTokenizer tokens = new StringTokenizer(line," ");
				int queryID = Integer.valueOf(tokens.nextToken());
				int numberOfResults = Integer.valueOf(tokens.nextToken());
				String query = tokens.nextToken("");
				System.out.println("\n\n\n\n\n********queryID:"+queryID+"(of "+totalQueries+")-->MakeQuery:"+query+" (top"+numberOfResults+")");
				qp.makeQuery(queryID, query, numberOfResults);

			}
		} catch (FileNotFoundException e) {}
	}

}

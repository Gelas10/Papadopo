package main;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class QueryVectorChunkCalculator implements Runnable{

	private Map<String,Double> vector;
	private SharedDouble norm;
	private List<String> query;
	private int queryID;
	private InvertedIndex index;
	private int documentsCount;
	private HashMap<String,Integer> queryFrequencies;
	
	/** 
	 * Initializes all the data structures this thread will read from and write to.
	 * @param vector : a shared Map where this function is going to write/read (other threads may also be using this shared Map).
	 * @param norm : a shared Double that is incremented (other threads may also be using this shared Double).
	 * @param document : a "chunk" the query (some words of the query) 
	 * @param queryID : the id of the query
	 * @param index : an inverted index built for all documents (except the query)
	 * @param documentsCount : the total number of documents of our collection.
	 * @param queryFrequencies : the frequency of each word of the query, inside the query.
	 */
	public QueryVectorChunkCalculator(Map<String,Double> vector, SharedDouble norm, List<String> query, int queryID, InvertedIndex index, int documentsCount, HashMap<String,Integer> queryFrequencies){  
		
		this.vector = vector;
		this.norm = norm;
		this.query = query;
		this.queryID = queryID;
		this.index = index;
		this.documentsCount = documentsCount;
		this.queryFrequencies = queryFrequencies;
	}
	
	/**
	 * Fills a part of the query's "vector".
	 * Given a query's "chunk" of some words, this function puts the weights of the words of the "chunk" in the "vector".
	 * The weight of a word inside this "vector" is: tf*idf (of this word).
	 * */
	@Override
	public void run() {
		
		//Read the document and for each word insert a weight to "vector" 
		Iterator<String> words = query.iterator();
		while(words.hasNext()){
			
			String word = words.next();
			
			//This word appears in > 0 documents
			HashMap<Integer,MutableInt> documentsThatContainWord = index.getHashMap().get(word);
			if(documentsThatContainWord != null){
			
				//query weight vector does not have the weight for this word already computed and stored
				if(!vector.containsKey(word)){
					
					int freqInThisDocument = queryFrequencies.get(word);
					
					//How many documents (AND query) contain this word?
					int nt;
					nt = documentsThatContainWord.size();
					//nt++;//For the query
		
					//Compute tf,idf
					double idf = Math.log( 1 + documentsCount/(double)nt);
					double tf = 1 + Math.log(freqInThisDocument);
				
					//Write to the shared structures (critical section)
					synchronized(this){
						if(!vector.containsKey(word)){
							vector.put(word, tf*idf );
							norm.incrementBy(Math.pow(tf*idf, 2));
						}
					}
					
					System.out.println("weightInDoc"+queryID+"("+word+") = "+" [1+log("+freqInThisDocument+")]*[log(1+"+documentsCount+"/"+nt+")] = "+tf+"*"+idf+" = "+(tf*idf));
					System.out.println("norm += (weight)^2 = ("+(tf*idf)+")^2 = "+Math.pow(tf*idf, 2));
				}
			}	
		}
	}

	
}

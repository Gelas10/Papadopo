package main;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class VectorChunkCalculator implements Runnable {

	private Map<String,Double> vector;
	private MutableDouble norm;
	private List<String> document;
	private int docID;
	private InvertedIndex index;
	private int documentsCount;
	
	/** 
	 * Initializes all the data structures this thread will read from and write to.
	 * @param vector : a shared Map where this function is going to write/read (other threads may also be using this shared Map).
	 * @param norm : a shared Double that is incremented (other threads may also be using this shared Double).
	 * @param document : a "chunk" of this document (some words of the document) 
	 * @param docID : the id of this document, that is (>=0 for all documents) or (=-1 for the query)
	 * @param index : an inverted index built for all documents
	 * @param documentsCount : the total number of documents of our collection.
	 */
	public VectorChunkCalculator(Map<String,Double> vector, MutableDouble norm, List<String> document, int docID, InvertedIndex index, int documentsCount){  
		
		this.vector = vector;
		this.norm = norm;
		this.document = document;
		this.docID = docID;
		this.index = index;
		this.documentsCount = documentsCount;
	}
	
	/**
	 * Fills a part of a document's "vector".
	 * Given a document's "chunk" of some words, this function puts the weights of the words of the "chunk" in the "vector".
	 * The weight of a word inside this "vector" is: tf*idf (of this word).
	 */
	@Override
	public void run() {
		
		//* NOTE1: The "vector" is actually a HashMap that maps Word to Weight.
		 //* NOTE2: tf and idf of a word don't change, so if we see that the weight of a word is already set, there is no need to recompute and reset it.

		//Read the document and for each word insert a weight to "vector" 
		Iterator<String> words = document.iterator();
		while(words.hasNext()){
			
			String word = words.next();
		
					//Insert a weight only the first time you see this word (Is the weight already set? If yes, you are done.).
					//Concurrency Note: 
					//Scenario: Thread1 and Thread2 ask "does vector contain the word 'apple'?" and both get no as an answer.
					//In this case, they will both (one at a time) write the same value 0.32 (for example) inside the map.
					//So Thread1 puts 0.32 as the mapped value of 'apple' and then Thread2 replaces 0.32 with 0.32.
					//This will only happen when the word 'apple' has no mappings. Once a mapping is created, this effect disappears.
			
			if(!vector.containsKey(word)){
			
				HashMap<Integer,MutableInt> docsMap = index.getHashMap().get(word);
				
				int freqInThisDocument = docsMap.get(docID).get();
				
				//How many documents (OR query) contain this word?
				int nt;
				nt = docsMap.size();
				//if(queryFrequencies.containsKey(word)){nt++;}
	
				//Compute tf, idf
				double idf = Math.log( 1 + documentsCount/(double)nt);
				double tf = 1 + Math.log(freqInThisDocument);
			
				//Write to the shared structures
				System.out.println("weightInDoc"+docID+"("+word+") = "+" [1+log("+freqInThisDocument+")]*[log(1+"+documentsCount+"/"+nt+")] = "+tf+"*"+idf+" = "+(tf*idf));
				vector.put(word, tf*idf );
				norm.incrementBy(Math.pow(tf*idf, 2));
				System.out.println("norm += (weight)^2 = ("+(tf*idf)+")^2 = "+Math.pow(tf*idf, 2));
			}
		}		
	}

}

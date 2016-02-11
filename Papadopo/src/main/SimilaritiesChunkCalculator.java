package main;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

public class SimilaritiesChunkCalculator implements Runnable {

	private Map<Integer,Double> similarities;
	private List<String> query;
	private InvertedIndex index;
	private HashMap<Integer,Double> norms;
	private Map<String,Double> queryVector;
	private double queryNorm;
	
	/** 
	 * Initializes all the data structures this thread will read from and write to.
	 * @param sharedMap : a shared Map where this function is going to write/read (other threads may also be using this shared Map).
	 * @param query : a "chunk" of the query (some words of the query) 
	 * @param index : an inverted index built for all documents(except the query)
	 * @param norms : the norm of each weight vector
	 * @param queryVector : the weight vector of the query
	 * @param queryNorm : the norm of the query vector
	 */
	public SimilaritiesChunkCalculator(Map<Integer,Double> sharedMap, List<String> query, InvertedIndex index, HashMap<Integer,Double> norms, Map<String,Double> queryVector, double queryNorm){  
		
		this.similarities = sharedMap;
		this.query = query;
		this.index = index;
		this.norms = norms;
		this.queryVector = queryVector;
		this.queryNorm = queryNorm;
	}

	/**
	 * Given some words of the query query, for each word "w", for each document "d" that contains that word, add (weight(w,d)*weight(w,query))/(docNorm*queryNorm) to the similarity of "d" with the query.
	 */
	@Override
	public void run() {
		
		//For each query word (of this "chunk")
		Iterator<String> queryTerms = query.iterator();
		while(queryTerms.hasNext()){			
		
			String word = queryTerms.next();
			System.out.println(word+" (of query)");
						
			//This word appears in > 0 documents
			HashMap<Integer,MutableInt> documentsThatContainWord = index.getDocumentsFrequency(word);
			if(documentsThatContainWord != null){
				
				//Find idf
				double idf = Math.log(1 + index.getNumberOfDocuments()/(double)documentsThatContainWord.size());
			
				//For each document that contains this word
				for (Entry<Integer, MutableInt> doc : documentsThatContainWord.entrySet()){
					
					int docID = doc.getKey();
					
					//Find tf
					double frequencyInThisDocument=0;
					frequencyInThisDocument = index.getDocumentsFrequency(word).get(docID).get();
					double tf = 1+ Math.log(frequencyInThisDocument);
					
					//Read the weight of this word in the query and in the document
					double weightOfWordInQuery = queryVector.get(word);
					double weightOfWordInDocument = tf*idf;//vectors.get(docID).getVector().get(word);
					
					double documentNorm = norms.get(docID);
					double addThisToSimilarity = (weightOfWordInQuery*weightOfWordInDocument)/(queryNorm*documentNorm);
					
					//Write to the shared map (critical section)
					synchronized(this){
						
						//Add something to the similarity of this document (with the query)
						if(similarities.containsKey(docID)){
							similarities.put(docID, similarities.get(docID) + addThisToSimilarity);
						}else{
							similarities.put(docID,addThisToSimilarity);
						}
					}
					
					System.out.println("\tsim(doc"+docID+") += "+"(weightQuery["+word+"]*weightDoc["+word+"])/(queryNorm*documentNorm)  = ("+weightOfWordInQuery+"*"+weightOfWordInDocument+")/("+queryNorm+"*"+documentNorm+") = "+addThisToSimilarity);
				}
			}	
		}	
	}
}

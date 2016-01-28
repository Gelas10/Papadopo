package main;

import java.util.HashMap;
import java.util.Map.Entry;
import java.util.StringTokenizer;

public class QueryProcessor {
	
	//TODO Make these variables private
	
	
	public InvertedIndex index;
	public double norms[];
	
	/**Maps docID to "vector"*/
	public HashMap<Integer,Vector> vectors;
	
	/**Maps docID to similarity (with a query)*/
	public HashMap<Integer,Double> similarity;
	
	public int documents;
	public int queryID = -1;
	public double queryNorm;
	public HashMap<String,Double> queryVector;
	
	/**An inverted index built on the words of the query only*/
	public InvertedIndex queryInvIndex;
	
	/**A simple representation of a "vector", that is the actual weights it contains and it's norm.*/
	public class Vector{
		private HashMap<String,Double> vector;
		private double norm;
		
		public Vector(HashMap<String,Double> vector1, double norm1){
			vector = vector1;
			norm = norm1;
		}
		
		public HashMap<String,Double> getVector(){return vector;}
		public double getNorm(){return norm;}
	}
	
	public QueryProcessor(String docs[])
	{
		
		documents = docs.length;
		norms = new double[documents];
		similarity = new HashMap<Integer,Double>();
		index = new InvertedIndex();
		vectors = new HashMap<Integer,Vector>();
		
		//Make the Inverted Index
		//Read each document and insert it's words into the index
		for(int i=0;i<docs.length;i++){
			StringTokenizer tok = new StringTokenizer(docs[i]);
			while(tok.hasMoreTokens()){
				
				String word = tok.nextToken();
				index.put(word, i);
			}
		}	
	}
	
	/**
	 * Creates the the "vector" that contains the weight of each word of the document.
	 * The weight of a word inside this "vector" is: tf*idf (of this word).
	 * Note: The "vector" is actually a HashMap that maps Word to Weight.
	 * Note: tf and idf of a word don't change, so if we see a word once inside a document, the weight for this word has been computed
	 * and we do not need to recompute it every time we see this word again inside the document.
	 * @param document : the full text of the document (all words)
	 * @param docID : the id of this document, that is (>=0 for all documents) or (=-1 for the query)
	 * @param index : an inverted index built for all documents
	 * */
	public Vector computeVector(String document, int docID, InvertedIndex index){
		
		double norm = 0;
		HashMap<String,Double> vector = new HashMap<String,Double>();

		//Read the document and for each word insert a weight to "vector" 
		StringTokenizer words = new StringTokenizer(document);
		while(words.hasMoreTokens()){
			
			String word = words.nextToken();
		
			//Insert a weight only the first time you see this word.
			if(!vector.containsKey(word)){
			
				HashMap<Integer,MutableInt> docsMap = index.getHashMap().get(word);
				
				int freqInThisDocument = docsMap.get(docID).get();
				int nt;//how many documents contain this word.
				
				nt = docsMap.size();
	
				double idf = Math.log( 1 + documents/(double)nt);
				double tf = 1 + Math.log(freqInThisDocument);
			
				System.out.println("weightInDoc"+docID+"("+word+") = "+tf+"*"+idf+" = "+(tf*idf));
				vector.put(word, tf*idf );
				norm += Math.pow(tf*idf, 2);
			}
			
		}
		
		norm = Math.sqrt(norm);
		System.out.println("norm("+docID+"): "+norm+"\n-------------------------------------------------------------------------");
		
		return new Vector(vector,norm);
		
	}
	
	/**
	 * Creates an inverted index containing the words of the query only and computes the "vector" and norm of the query.
	 * @param queryString : the full text of the query (all it's words)
	 * */
	public void setQuery(String queryString){	
		
		//Create an inverted index for the query ONLY
		queryInvIndex = new InvertedIndex();
			
		StringTokenizer words = new StringTokenizer(queryString);
		while(words.hasMoreTokens()){
			String word = words.nextToken();
			queryInvIndex.put(word, queryID);
		}
		
		//Compute the query vector and norm
		Vector v1 = computeVector(queryString, queryID , queryInvIndex);
		queryNorm = v1.getNorm();
		queryVector = v1.getVector();
		
	}
	
	public static void main(String[] args){
		
		String docs[] = 
		{			
			"ant ant bee cat dog dog dog elephant fox goat apple room",
			"apple apple apple apple orange banana banana banana melon peach grape pineapple dog room",
			"room chair sofa window desk table bed bed dog ant"
		};
		
		QueryProcessor qp = new QueryProcessor(docs);
		
		qp.setQuery("ant bee goat chair melon");
		
		System.out.println("================================================================================================================================\nComputing vector weights and norm for each document. (a vector contains weights for ALL words of the document) ..."); 
		
		//For each document
		for(int i=0;i<docs.length;i++){
		
			Vector vector = qp.computeVector(docs[i], i , qp.index);
			qp.norms[i] = vector.getNorm();
			qp.vectors.put(i, vector);

		}
		
		System.out.println("\n\n\n================================================================================================================================\nComputing similarity ...\n");
		
		//For each query word, for each doc that contains that word, add something to it's similarity.
		
		//For each query word
		for (Entry<String, Double> queryTerm : qp.queryVector.entrySet()){
					
					String word = queryTerm.getKey();
					
					//For each document that contains this word
					for (Entry<Integer, MutableInt> doc : qp.index.getHashMap().get(word).entrySet()){
						
						int docID = doc.getKey();
						
						//Read the weight of this word in the query and in the document
						double weightOfWordInQuery = qp.queryVector.get(word);
						double weightOfWordInDocument = qp.vectors.get(docID).getVector().get(word);
						
						//Add something to the similarity of this document (with the query)
						if(qp.similarity.containsKey(docID)){
							qp.similarity.put(docID, qp.similarity.get(docID) + (weightOfWordInQuery*weightOfWordInDocument));
						}else{
							qp.similarity.put(docID,weightOfWordInQuery*weightOfWordInDocument);
						}
					}
		}
		
		//Print normalized similarities
		for (Entry<Integer, Double> similarities : qp.similarity.entrySet()){
		
			int docID = similarities.getKey();
			double notNormalizedSimilarity = similarities.getValue();
			
			double docNorm   = qp.vectors.get(docID).getNorm();
			double queryNorm = qp.queryNorm;
			
			double normalizedSimilarity = notNormalizedSimilarity/(docNorm*queryNorm);
			
			System.out.println("similarity("+docID+",query): "+normalizedSimilarity);
		}
		
	}
	
}

package main;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
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
	public Map<String,Double> queryVector;
	
	/**A simple frequency counter for every word in the query*/
	public HashMap<String,Integer> queryFrequencies;
	
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
	 * Fills a part of a document's "vector".
	 * Given a document's "chunk" of some words, this function puts the weights of the words of the "chunk" in the "vector".
	 * The weight of a word inside this "vector" is: tf*idf (of this word).
	 * @param vector : a shared Map where this function is going to write/read (other threads may also be using this shared Map).
	 * @param norm : a shared Double that is incremented (other threads may also be using this shared Double).
	 * @param document : a "chunk" of this document (some words of the document) 
	 * @param docID : the id of this document, that is (>=0 for all documents) or (=-1 for the query)
	 * @param index : an inverted index built for all documents
	 * */
	public void computeVector(Map<String,Double> vector, MutableDouble norm, List<String> document, int docID, InvertedIndex index){

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
				double idf = Math.log( 1 + documents/(double)nt);
				double tf = 1 + Math.log(freqInThisDocument);
			
				//Write to the shared structures
				System.out.println("weightInDoc"+docID+"("+word+") = "+" [1+log("+freqInThisDocument+")]*[log(1+"+documents+"/"+nt+")] = "+tf+"*"+idf+" = "+(tf*idf));
				vector.put(word, tf*idf );
				norm.incrementBy(Math.pow(tf*idf, 2));
				System.out.println("norm += (weight)^2 = ("+(tf*idf)+")^2 = "+Math.pow(tf*idf, 2));
			}
		}		
	}
	
	
	/**
	 * Fills a part of the query's "vector".
	 * Given a query's "chunk" of some words, this function puts the weights of the words of the "chunk" in the "vector".
	 * The weight of a word inside this "vector" is: tf*idf (of this word).
	 * @param vector : a shared Map where this function is going to write/read (other threads may also be using this shared Map).
	 * @param norm : a shared Double that is incremented (other threads may also be using this shared Double).
	 * @param query : a "chunk" of the query (some words of the document) 
	 * @param index : an inverted index built for all documents (except the query). The query word frequencies are stored in a QueryProcessor Map.
	 * */
	public void computeQueryVector(Map<String,Double> vector, MutableDouble norm, List<String> query, InvertedIndex index){

		//Read the document and for each word insert a weight to "vector" 
		Iterator<String> words = query.iterator();
		while(words.hasNext()){
			
			String word = words.next();
			
			if(!vector.containsKey(word)){
			
				HashMap<Integer,MutableInt> docsMap = index.getHashMap().get(word);
				
				int freqInThisDocument = queryFrequencies.get(word);
				
				//How many documents (AND query) contain this word?
				int nt;
				nt = docsMap.size();
				//nt++;//For the query
	
				//Compute tf,idf
				double idf = Math.log( 1 + documents/(double)nt);
				double tf = 1 + Math.log(freqInThisDocument);
			
				//Write to the shared structures
				System.out.println("weightInDoc"+queryID+"("+word+") = "+" [1+log("+freqInThisDocument+")]*[log(1+"+documents+"/"+nt+")] = "+tf+"*"+idf+" = "+(tf*idf));
				vector.put(word, tf*idf );
				norm.incrementBy(Math.pow(tf*idf, 2));
				System.out.println("norm += (weight)^2 = ("+(tf*idf)+")^2 = "+Math.pow(tf*idf, 2));
			}
		}		
	}
	
	/**
	 * Given a query "q", for each query word "w", for each document "d" that contains that word, add {weight(w,d)*weight(w,q)} to the similarity of "d" with the query.
	 * */
	public void computeSimilarities(){
		
		//For each query word
		for (Entry<String, Double> queryTerm : queryVector.entrySet()){
					
			String word = queryTerm.getKey();
			System.out.println(word+" (of query)");
			
			//For each document that contains this word
			for (Entry<Integer, MutableInt> doc : index.getHashMap().get(word).entrySet()){
				
				int docID = doc.getKey();
				
				//Read the weight of this word in the query and in the document
				double weightOfWordInQuery = queryVector.get(word);
				double weightOfWordInDocument = vectors.get(docID).getVector().get(word);
				
				//Add something to the similarity of this document (with the query)
				if(similarity.containsKey(docID)){
					similarity.put(docID, similarity.get(docID) + (weightOfWordInQuery*weightOfWordInDocument));
				}else{
					similarity.put(docID,weightOfWordInQuery*weightOfWordInDocument);
				}
				
				System.out.println("\tsim(doc"+docID+") += "+"weightQuery["+word+"]*weightDoc["+word+"] = "+weightOfWordInQuery+"*"+weightOfWordInDocument+" = "+(weightOfWordInQuery*weightOfWordInDocument));
			}
		}
		
		System.out.println("\n\n");
		
		//Replace similarities with normalized similarities
		for (Entry<Integer, Double> similarities : similarity.entrySet()){
			
			int docID = similarities.getKey();
			
			double notNormalizedSimilarity = similarities.getValue();
			double docNorm   = vectors.get(docID).getNorm();
			
			similarities.setValue(notNormalizedSimilarity/(docNorm*queryNorm));
			//similarities.setValue(notNormalizedSimilarity/docNorm);
			System.out.println("Normalize sim(doc"+docID+","+"query): "+notNormalizedSimilarity+" -> "+similarities.getValue());
		}
	}
	
	
	/**
	 * Counts the word frequency for each word in the query and computes the "vector" and norm of the query.
	 * @param queryString : the full text of the query (all it's words)
	 * */
	public void setQuery(List<String> query){	
		
		//Count the frequency of each word inside the query
		queryFrequencies = new HashMap<String,Integer>();

		//Count the words of the query
//		int totalWords = 0;
		Iterator<String> words = query.iterator();
		while(words.hasNext()){
			String word = words.next();
			if(queryFrequencies.containsKey(word)){
				int oldFreq = queryFrequencies.get(word);
				queryFrequencies.put(word, oldFreq+1);
			}else{
				queryFrequencies.put(word, 1);
			}
//			totalWords++;
		}
		
		//Declare the "shared" HashMap and norm in which the threads are going to write.
		Map<String,Double> vector = Collections.synchronizedMap(new HashMap<String,Double>());
		MutableDouble norm = new MutableDouble();
		
		//TODO Put some threads to do this calculation "computeQueryVector" on a query "chunk" of words. And then join them (the exact same thing we do in main for document vectors).
		
		//Compute the query vector and norm
		computeQueryVector(vector, norm, query, index);
		
		queryNorm = Math.sqrt(norm.get());
		queryVector = vector;
		System.out.println("vector[query]: "+queryVector);
		System.out.println("norm[query]: squareRoot(Σ(weight^2)) = "+queryNorm);
	}
	
	public static void main(String[] args){
				
		System.out.println("Remember: N  = #documents (without query)\n          nt = #documents that contain word (without query)");
		System.out.println("Remember: weight = [1+ln(freq)]*ln(1+N/nt)\n\n");
		
		int threads = 1;
		
		String docs[] = 
		{			
			"ο κομήτης του Χάλλεϋ μας επισκέπτεται περίπου κάθε εβδομήντα έξι χρόνια",
			"ο κομήτης του Χάλλεϋ ανακαλύφθηκε από τον αστρονόμο Έντμοντ Χάλλεϋ",
			"ένας κομήτης διαγράφει ελλειπτική τροχιά",
			"ο πλανήτης Άρης έχει δύο φυσικούς δορυφόρους το Δείμο και το Φόβο",
			"ο πλανήτης Δίας έχει εξήντα τρείς γνωστούς φυσικούς δορυφόρους",
			"ο Ήλιος είναι ένας αστέρας",
			"ο Άρης είναι ένας πλανήτης του ηλιακού μας συστήματος"
		};
		
		QueryProcessor qp = new QueryProcessor(docs);
		
		//Set the query
		ArrayList<String> query = new ArrayList<String>();
		query.add("κομήτης");
		query.add("Χάλλεϋ");
		qp.setQuery(query);
		
		System.out.println("================================================================================================================================\nComputing vector weights and norm for each document. (a vector contains weights for ALL words of the document) ..."); 
		
		//For each document
		for(int i=0;i<docs.length;i++){
			
			System.out.println("\n\n\n\n\n\n"+" doc"+i+"\n"+docs[i]);
			
			//Compute the total number of words this document has.
			int totalWords =0;
			ArrayList<String> words = new ArrayList<String>();
			StringTokenizer tok = new StringTokenizer(docs[i]);
			while(tok.hasMoreTokens()){
				words.add(tok.nextToken());
				totalWords++;
			}
			
			//Declare the "shared" HashMap and norm in which all threads will write (this HashMap is the "vector" that contains word weights inside this document).
			Map<String,Double> sharedVector = Collections.synchronizedMap(new HashMap<String,Double>());
			MutableDouble sharedNorm = new MutableDouble();
			
			//Define how many words each thread will take.
			int wordsPerThread = totalWords/threads;
			int wordsForFirstThread = wordsPerThread;
			wordsForFirstThread += (totalWords%threads);
			int start=0;
			int end=wordsForFirstThread;
			
			//Give a portion to the First thread
			System.out.println("\n---------------------------------------------------------------\n"+"portion: ["+start+","+end+")");
			System.out.println("Thread1 takes: "+words.subList(start, end));
			qp.computeVector(sharedVector, sharedNorm, words.subList(start, end), i , qp.index);
			
			//Give a portion to the Rest of the threads
			for(int t=0;t<threads-1;t++){
				start = end;
				end += wordsPerThread;
				System.out.println("\n---------------------------------------------------------------\n"+"portion: ["+start+","+end+")");
				System.out.println("Thread"+(t+2)+" takes: "+words.subList(start, end));
				qp.computeVector(sharedVector, sharedNorm, words.subList(start, end), i , qp.index);
			}
			
			//Join the threads.
			//TODO : Join the threads
			
			//Store final result
			qp.norms[i] = Math.sqrt(sharedNorm.get());
			qp.vectors.put(i, new Vector(sharedVector,sharedNorm.get()));
			System.out.println("vector["+i+"]: "+qp.vectors.get(i).getVector());
			System.out.println("norm["+i+"]: squareRoot(Σ(weight^2)) = "+qp.norms[i]);
			
		}
		
		System.out.println("\n\n\n================================================================================================================================\nComputing similarity ...\n");
		
		qp.computeSimilarities();
		
		System.out.println("\n\n\n\n");
		
		//Print normalized similarities
		for (Entry<Integer, Double> similarities : qp.similarity.entrySet()){
			
			System.out.println("similarity("+similarities.getKey()+",query): "+similarities.getValue());
		}

	}

}

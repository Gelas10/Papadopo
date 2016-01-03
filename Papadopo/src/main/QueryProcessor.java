package main;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.StringTokenizer;

import main.InvertedIndex.MutableInt;

public class QueryProcessor {
	
	//TODO Make these variables private
	
	public InvertedIndex index;
	public double norms[];
	public ArrayList<HashMap<String,Double>> vectors;
	public double similarity[];
	private double Ld[];
	public int documents;
	public int queryID = -1;
	public double queryNorm;
	public HashMap<String,Double> queryVector;
	public InvertedIndex queryInvIndex;
	
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
	
	public QueryProcessor(String docs[]){
		
		documents = docs.length;
		norms = new double[documents];
		similarity = new double[docs.length];
		index = new InvertedIndex();
		vectors = new ArrayList<HashMap<String,Double>>();
		
		//Make the Inverted Index
		//Read each document and insert it's words into the index
		for(int i=0;i<docs.length;i++){
			StringTokenizer tok = new StringTokenizer(docs[i]);
			while(tok.hasMoreTokens()){
				
				String word = tok.nextToken();
				index.put(word, i);
			}
		}
		
		//Initialize vectors to empty
		for(int i=0;i<documents;i++){
			vectors.add(new HashMap<String,Double>());
		}		
	}
	
	public Vector computeVector(String document, int docID, InvertedIndex index){
		
		double norm = 0;
		HashMap<String,Double> vector = new HashMap<String,Double>();
		
		//Get all unique words of this document.
		HashSet<String> uniqueWordsInDocument = new HashSet<String>();
		 
		StringTokenizer tok = new StringTokenizer(document);
		while(tok.hasMoreTokens()){
			String word = tok.nextToken();
			
			uniqueWordsInDocument.add(word);
		}
		
		//For each unique word
		Iterator<String> words = uniqueWordsInDocument.iterator();
		while(words.hasNext()){
			
			String word = words.next();
			HashMap<Integer,MutableInt> docsMap = index.getHashMap().get(word);
			
			int freqInThisDocument = docsMap.get(docID).get();
			int nt;//how many documents contain this word.
			
			nt = docsMap.size();

			double idf = Math.log( 1 + documents/(double)nt);
			double tf = 1 + Math.log(freqInThisDocument);
			
			norm += Math.pow(tf*idf, 2);
			
			//"vector" does not contain this word because it is UNIQUE inide the document.
			System.out.println("weightInDoc"+docID+"("+word+") = "+tf+"*"+idf+" = "+(tf*idf));
			vector.put(word, tf*idf );
			
		}
		
		norm = Math.sqrt(norm);
		System.out.println("norm("+docID+"): "+norm+"\n-------------------------------------------------------------------------");
		
		return new Vector(vector,norm);
		
	}
	
	public double computeCosineSimilarity(HashMap<String,Double> qvector , double qnorm, HashMap<String,Double> dvector , double dnorm){
		
		double similarity = 0;
		
		//For each query term
		for (Entry<String, Double> wordEntry : qvector.entrySet()){
			
			String word = wordEntry.getKey();
			
			//Document contains query term
			if(dvector.containsKey(word)){
				
				double w_doc = dvector.get(word);
				double w_que = qvector.get(word);
				
				System.out.println("For the word: "+word+" => sim += weightInDoc("+word+")*weightInQuer("+word+") that is "+w_doc+"*"+w_que+" = "+(w_doc*w_que));  
				
				similarity += w_doc*w_que;
			}
		}
		
		similarity /= dnorm*qnorm;
		System.out.println("divideBy:"+dnorm+"*"+qnorm+"="+(dnorm*qnorm)+"\n");
		
		return similarity;
		
	}
	
	public void setQuery(String queryString){
		
		//Convert to array
		ArrayList<String> q = new ArrayList<String>();
		StringTokenizer tt = new StringTokenizer(queryString);
		while(tt.hasMoreTokens()){
			q.add(tt.nextToken());
		}
		String query[] = new String[q.size()];
		q.toArray(query);
		
		
		//Create an inverted index for the query ONLY
		queryInvIndex = new InvertedIndex();
		
		for(int i=0;i<query.length;i++){
			queryInvIndex.put(query[i], queryID);
		}
		
		//Compute the query vector and norm
		Vector v1 = computeVector(queryString, -1 , queryInvIndex);
		queryNorm = v1.getNorm();
		queryVector = v1.getVector();
		
	}
	
	public void setDocuments(int documents1){documents = documents1;} 
	
	public void makeIndexAndNorms(){
		
		String docs[] = {
				"ant ant bee cat dog dog dog elephant fox goat apple room",
				"apple apple apple apple orange banana banana banana melon peach grape pineapple dog room",
				"room chair sofa window desk table bed bed dog ant"
		};
		
		documents = docs.length;
		Ld = new double[documents];
		index = new InvertedIndex();
		
		HashSet<String> testing = new HashSet<String>();				//--//
		
		//Read each document and 1)insert it's words into the index 2)compute the norm of each document.
		for(int i=0;i<docs.length;i++){
			StringTokenizer tok = new StringTokenizer(docs[i]);
			while(tok.hasMoreTokens()){
				String word = tok.nextToken();
				index.put(word, i);
				
				//Compute Ld for each document (that is the norm)		//--//
				testing.add(word);										//--//
			}															//--//
			Ld[i] = 0;													//--//
			Iterator<String> testingIt = testing.iterator();			//--//
			while(testingIt.hasNext()){									//--//
				String word = testingIt.next();							//--//
				int freq = index.getHashMap().get(word).get(i).get();//--//
				Ld[i] += Math.pow(freq, 2);								//--//
			}															//--//
			Ld[i] = Math.sqrt(Ld[i]);									//--//
			System.out.println("norm(doc"+i+"): "+Ld[i]);				//--//
			testing.clear();											//--//
		}
	}
	
	public void computeSimilarityUsingTopkAlgorithm(){
		
		HashMap<Integer,Double> scores = new HashMap<Integer,Double>();
		HashMap<String,HashMap<Integer, MutableInt>> index = this.index.getHashMap();
		
		String query[] = {"ant","bee","goat","chair","melon"};
		
		//For each query term
		for(int i=0;i<query.length;i++){
			
			HashMap<Integer,MutableInt> docsMap = index.get(query[i]);
			double idf = Math.log( 1 + documents/(double)docsMap.size());
			
			//For each document with this term
			for (Entry<Integer, MutableInt> doc : docsMap.entrySet()){
				
				double tf = 1 + Math.log(doc.getValue().get());
				
				//Does this doc already have some score?
				if( scores.containsKey(doc.getKey()) ){
					
					scores.put(doc.getKey() , tf*idf + scores.get(doc.getKey()));
				}else{
					
					scores.put(doc.getKey() , tf*idf);
				}
			}
		}
		
		//Normalize each document's score
		for (Entry<Integer, Double> score : scores.entrySet()){
			int docID = score.getKey();
			scores.put(docID, score.getValue()/Ld[docID]);
		}
		
		//Print the similarity results:
		for (Entry<Integer, Double> score : scores.entrySet()){
			System.out.println("Similarity(query,doc"+score.getKey()+": "+score.getValue());
		}
	}
		
	public static void main(String[] args){
	
		//QueryProcessor qp = new QueryProcessor();
		//qp.makeIndexAndNorms();
		//qp.computeSimilarityUsingTopkAlgorithm();
				
		
		String docs[] = {			
			"ant ant bee cat dog dog dog elephant fox goat apple room",
			"apple apple apple apple orange banana banana banana melon peach grape pineapple dog room",
			"room chair sofa window desk table bed bed dog ant"
		};
		
		QueryProcessor qp = new QueryProcessor(docs);
		
		qp.setQuery("ant bee goat chair melon");
		
		System.out.println("================================================================================================================================\nComputing vector weights and norm for each document. (a vector contains weights for ALL words of the document) ..."); 
		
		//For each document
		for(int i=0;i<docs.length;i++){
		
			Vector v = qp.computeVector(docs[i], i , qp.index);
			qp.norms[i] = v.getNorm();
			qp.vectors.set(i, v.getVector());

		}
		
		System.out.println("\n\n\n================================================================================================================================\nComputing similarity ...\n");
		
		//For each document
		int i=0;
		Iterator<HashMap<String,Double>> docVectors = qp.vectors.iterator();
		while(docVectors.hasNext()){
			
			HashMap<String,Double> vector = docVectors.next();
			
			System.out.println("vectorOf(doc"+i+"): "+vector);
			
			qp.similarity[i] = qp.computeCosineSimilarity(vector,qp.norms[i] , qp.queryVector , qp.queryNorm);
			
			i++;
		}
		
		//Print results
		for(int j=0;j<qp.documents;j++){
			System.out.println("similarity(query,doc"+j+"): "+qp.similarity[j]);
		}
		
	}
	
}

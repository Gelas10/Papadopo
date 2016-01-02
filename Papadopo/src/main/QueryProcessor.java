package main;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.StringTokenizer;

import main.InvertedIndex.MutableInt;

public class QueryProcessor {
	
	//private InvertedIndex index;
	
	//public QueryProcessor(InvertedIndex an_index){
	//	index = an_index;
	//}	
	
	public static void main(String[] args){
	
		String docs[] = {
			"ant ant bee cat dog dog dog elephant fox goat apple room",
			"apple apple apple apple orange banana banana banana melon peach grape pineapple dog room",
			"room chair sofa window desk table bed bed dog ant"
		};
		
		int documents = docs.length;
		InvertedIndex invIndex = new InvertedIndex();
		double Ld[] = new double[documents];
		
		HashSet<String> testing = new HashSet<String>();				//--//
		
		//Read each document and 1)insert it's words into the index 2)compute the norm of each document.
		for(int i=0;i<docs.length;i++){
			StringTokenizer tok = new StringTokenizer(docs[i]);
			while(tok.hasMoreTokens()){
				String word = tok.nextToken();
				invIndex.put(word, i);
				
				//Compute Ld for each document (that is the norm)		//--//
				testing.add(word);										//--//
			}															//--//
			Ld[i] = 0;													//--//
			Iterator<String> testingIt = testing.iterator();			//--//
			while(testingIt.hasNext()){									//--//
				String word = testingIt.next();							//--//
				int freq = invIndex.getHashMap().get(word).get(i).get();//--//
				Ld[i] += Math.pow(freq, 2);								//--//
			}															//--//
			Ld[i] = Math.sqrt(Ld[i]);									//--//
			System.out.println("norm(doc"+i+"): "+Ld[i]);				//--//
			testing.clear();											//--//
		}
		
		HashMap<Integer,Double> scores = new HashMap<Integer,Double>();
		HashMap<String,HashMap<Integer, MutableInt>> index = invIndex.getHashMap();
		
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
}

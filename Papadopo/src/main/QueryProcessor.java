package main;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.StringTokenizer;

import main.InvertedIndex.MutableInt;

public class QueryProcessor {
	
	private InvertedIndex index;
	
	private double Ld[];
	
	private int documents;
	
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
		
		
		//Here goes the new algorithm ...

		//STEP 1)Initialization
		
		//{query,doc0,doc1,doc2,...}
		String docs[] = {
			"ant bee goat chair melon",
			
			"ant ant bee cat dog dog dog elephant fox goat apple room",
			"apple apple apple apple orange banana banana banana melon peach grape pineapple dog room",
			"room chair sofa window desk table bed bed dog ant"
		};
		
		int documents = docs.length-1;
		InvertedIndex invIndex = new InvertedIndex();
		double norms[] = new double[documents+1];
		
		//Read each document (and the query) and insert it's words into the index
		for(int i=0;i<docs.length;i++){
			StringTokenizer tok = new StringTokenizer(docs[i]);
			while(tok.hasMoreTokens()){
				
				String word = tok.nextToken();
				invIndex.put(word, i);
			}
		}
		
		//TODO FIX: Do NOT initialize "weights"
		
		//Each doc has a vector (in the form of HashMap). For example doc1's vector is not doc1=[0 0 0.23 0.78 0.64 0 0] but a HashMap with the term as the key and the weight as the value. 
		ArrayList<HashMap<String,Double>> weights = new ArrayList<HashMap<String,Double>>();
		for(int i=0;i<documents+1;i++){
			weights.add(new HashMap<String,Double>());
		}
		//TODO Consider this vector [0 0 0.23 0.78 0.64 0 0]. It means that the first word "apple" (for example) has weight 0, the second "book" (for example) has 0, ...
		//TODO How do i know this sequence of the words (apple,book,...)?
		
		HashMap<String,HashMap<Integer, MutableInt>> index = invIndex.getHashMap();
		
		//Format the query
		ArrayList<String> q = new ArrayList<String>();
		StringTokenizer tt = new StringTokenizer(docs[0]);
		while(tt.hasMoreTokens()){
			q.add(tt.nextToken());
		}
		String query[] = new String[q.size()];
		q.toArray(query);
		
		
		
		
		
		
		
		
		//STEP 2)Compute weights (Only for the words that appear in the query)
		
		System.out.println("================================================================================================================================"); 
		System.out.println("Computing vector weights for each document. (a vector contains weights only for words that appear in the QUERY and DOCUMENT) ...");
		System.out.println("The following \"weight\" data ARE ALL STORED!");
		
		//For each query term
		for(int i=0;i<query.length;i++){
			
			HashMap<Integer,MutableInt> docsMap = index.get(query[i]);
			
			//idf = ln(1+N/nt)  ,  N:#documents, nt:#documentsWhereTermAppears
			
			double idf = Math.log( 1 + documents/((double)docsMap.size()-1));
			System.out.println("\nqueryWord:"+query[i]+" => idf = ln(1+"+documents+"/"+(docsMap.size()-1)+") = "+idf);
			
			//For each document with this term
			for (Entry<Integer, MutableInt> doc : docsMap.entrySet()){
				
				//tf = 1+ln(freq)    ,  freq:frequency of term inside this document
				double tf = 1 + Math.log(doc.getValue().get());
				System.out.println("tf = 1+ln("+doc.getValue().get()+") = "+tf);
				
				HashMap<String,Double> vector;
				try{
					
					vector = weights.get(doc.getKey());
					
					//This word exists in the "vector"
					if(vector.containsKey(query[i])){

						System.out.println("weightInDoc"+doc.getKey()+"("+query[i]+") += "+tf+"*"+idf+" = "+(tf*idf));
						vector.put(query[i], vector.get(query[i])+(tf*idf) );
					}else{
						
						System.out.println("weightInDoc"+doc.getKey()+"("+query[i]+") = "+tf+"*"+idf+" = "+(tf*idf));
						vector.put(query[i], tf*idf );
					}
					
					//System.out.println("OLD VECTOR!"+doc.getKey());
					
				}catch(IndexOutOfBoundsException e){				//<DEAD CODE> (because of the initialization of "weights" above.)

					//Add this word to "vector" as first seen
					vector = new HashMap<String,Double>();
					vector.put(query[i], tf*idf );
					weights.add(doc.getKey(),vector);
					
					//System.out.println("NEW VECTOR!"+doc.getKey());
				}													//</DEAD CODE>
			}
		}
				
		//STEP 3) Compute vector norms (norm considers all words of a document)
		
		System.out.println("\n\n\n================================================================================================================================");
		System.out.println("Computing vector norm for each document. (a norm is computed using the weight of ALL words of the document, not only the words that appear in the document and the query) ..."); 
		System.out.println("The following \"tf\" and \"idf\" data ARE NOT STORED!(just printed, they are only used to compute norm.)\n");
		
		//For each document (including the query)
		for(int i=0;i<docs.length;i++){
			
			norms[i] = 0;
			
			HashSet<String> uniqueWordsInDocument = new HashSet<String>();
			
			//Get unique words of document 
			StringTokenizer tok = new StringTokenizer(docs[i]);
			while(tok.hasMoreTokens()){
				String word = tok.nextToken();
				
				uniqueWordsInDocument.add(word);
			}
			
			//For each unique word
			Iterator<String> words = uniqueWordsInDocument.iterator();
			while(words.hasNext()){
				String word = words.next();
				
				//Compute weight
				int f = index.get(word).get(i).get();
				double tf = 1+Math.log(f);
				int nt;
						//nt is how many DOCUMENTS contain this word. So if we see that 3 documents contain this word but one of them is the query, then 2 documents really contain the word. 
						if(index.get(word).keySet().contains(0)){
							nt = index.get(word).keySet().size()-1;
						}else{
							nt = index.get(word).keySet().size();
						}
				
				double idf = Math.log(1+(double)documents/(double)nt );
				
				System.out.println("doc"+i+" "+word+" [tf:1+ln("+f+") = "+tf+"] , [idf:ln(1+"+documents+"/"+nt+") = "+idf+"]");
				
				norms[i] += Math.pow(tf*idf, 2);
			}
			
			
			norms[i] = Math.sqrt(norms[i]);
			System.out.println("norm("+i+"): "+norms[i]+"\n-------------------------------------------------------------------------");
		}
		
		
		//STEP 4)Compute similarity
		
		System.out.println("\n\n\n================================================================================================================================");
		System.out.println("Computing similarity ...\n");
		
		double similarity[] = new double[docs.length-1];
		
		HashMap<String,Double> queryVector = weights.get(0);
		
		//For each document
		Iterator<HashMap<String,Double>> docWeights = weights.iterator();
		
		docWeights.next();//The first document is the query so ignore it.
		
		int i=1;
		while(docWeights.hasNext()){
			
			HashMap<String,Double> vector = docWeights.next();
			
			System.out.println("vectorOf(doc"+i+"): "+vector);
			
			similarity[i-1]=0;
			
			//For each query term
			for (Entry<String, Double> wordEntry : queryVector.entrySet()){
				
				String word = wordEntry.getKey();
				
				//Document contains query term
				if(vector.containsKey(word)){
					
					double w_doc = vector.get(word);
					double w_que = queryVector.get(word);
					
					System.out.println("For the word: "+word+" => sim(doc"+i+") += weightInDoc("+word+")*weightInQuer("+word+") that is "+w_doc+"*"+w_que+" = "+(w_doc*w_que));
					
					similarity[i-1] += w_doc*w_que;
				}
			}
			
			similarity[i-1] /= norms[i]*norms[0];
			System.out.println("divideBy:"+norms[i]+"*"+norms[0]+"="+(norms[i]*norms[0])+"\n");
			i++;
		}
		
		for(int j=1;j<documents+1;j++){
			System.out.println("similarity(query,doc"+j+"): "+similarity[j-1]);
		}		
		
	}
	
}

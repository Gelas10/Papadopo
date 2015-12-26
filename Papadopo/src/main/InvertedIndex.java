package main;

import java.util.*;

public class InvertedIndex 
{	
	class MutableInt 
	{
		  int value = 1; // note that we start at 1 since we're counting
		  public synchronized void increment () { ++value;      }
		  public int  get ()       { return value; }
	}
	HashMap<String,Set<Integer>> index;//HashMap <Term,Docs in which the term is found>
	HashMap<String,HashMap<Integer,MutableInt>> index2;//HashMap <Term,HashMap<Document,Frequency in Document>>
	public InvertedIndex()
	{
		index=new HashMap<>();
		index2=new HashMap<>();		
	}
	//Merge 2 HashMaps, Return Merged HashMap
	public HashMap<String,Set<Integer>> merge(HashMap<String,Set<Integer>> map1,HashMap<String,Set<Integer>> map2)
	{
		for (Map.Entry<String, Set<Integer>> e : map2.entrySet())//For each entry<Term,Documents> in map2
		{
			Set<Integer> map1Values=map1.get(e.getKey());//Get the Documents of map1 for this Term 
			if(map1Values!=null)//If there are Documents in map1 for this Term
			{
				for (Integer docID : e.getValue()) //For each document in map2's entry
						map1Values.add(docID);//Add it to the documents of map1
				map1.put(e.getKey(),map1Values);//Replace the Documents of Term in map1 with the merged Documents
			}
			else map1.put(e.getKey(), e.getValue());//If there were no documents in map1 for this Term: Simply put Documents of map2's entry
		}
		return map1;//Return the updated map1 ( merged with map 2 )
	}
	
	public void put(String word,int docId) 
	{
		
		Set<Integer> docs=new HashSet<>();
		Set<Integer> indexDocs=index.get(word);
		if(indexDocs!=null)
		{
			docs=indexDocs;
		}
		docs.add(docId);
		index.put(word, docs);
		
		
	}
	public void put2(String word,int docId) 
	{
		HashMap<Integer, MutableInt> freq = new HashMap<>();
		HashMap<Integer, MutableInt> indexFreq = index2.get(word);
		if(indexFreq!=null)
		{
			freq=indexFreq;
		}		
		MutableInt count = freq.get(docId);
		if (count == null) 
		{
		    freq.put(docId, new MutableInt());
		}
		else 
		{
		    count.increment();
		}
		index2.put(word, freq);
		
	}
//	public HashMap<String,Set<Integer>> getHashMap()
//	{
//		return index;
//	}
	public HashMap<String,HashMap<Integer, MutableInt>> getHashMap()
	{
		return index2;
	}
}

/* Testing shiat
	ArrayList<Integer> ar1=new ArrayList<>();
	ar1.add(1);
	ArrayList<Integer> ar2=new ArrayList<>();
	ar2.add(2);
	ar2.add(3);
	
	index.put("skata", ar1);
	index.put("melata", ar2);
	
	ArrayList<Integer> ar3=new ArrayList<>();
	ar3.add(3);
	ar3.add(4);
	HashMap<String,ArrayList<Integer>> toMerge=new HashMap<>();
	toMerge.put("melata", ar3);
	
	HashMap<String,ArrayList<Integer>> afterMerge=merge(index,toMerge);
	for (Map.Entry<String, ArrayList<Integer>> e : afterMerge.entrySet())
	{
		System.out.println("Key: "+e.getKey());
		for (Integer integer : e.getValue()) 
		{
			System.out.println("Value: "+integer);
		}
	}
 */
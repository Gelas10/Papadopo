package main;

import java.util.*;
import java.util.Map.Entry;

public class InvertedIndex 
{	
	public class MutableInt 
	{
		  private int value;
		  public MutableInt(int avalue){value = avalue;}
		  public MutableInt(){value = 1;}// note that we start at 1 since we're counting
		  public synchronized void increment(){ ++value; }
		  public int get (){ return value; }
	}

	private HashMap<String,HashMap<Integer,MutableInt>> index;//HashMap <Term,HashMap<Document,Frequency in Document>>
	private int documents;
	
	public InvertedIndex()
	{	
		index=new HashMap<>();
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
		HashMap<Integer, MutableInt> freq = new HashMap<>();
		HashMap<Integer, MutableInt> indexFreq = index.get(word);
		if(indexFreq!=null)
		{
			freq=indexFreq;//Getting the frequency of word in EACH document
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
		index.put(word, freq);
		
	}

	public HashMap<String,HashMap<Integer, MutableInt>> getHashMap()
	{
		return index;
	}
	
	public void printIndex(){
		
		for (Map.Entry<String, HashMap<Integer, MutableInt>> e : getHashMap().entrySet()){
			
			System.out.println("Word: "+e.getKey());
			HashMap<Integer, MutableInt> df=e.getValue();
			for (Entry<Integer, MutableInt> docFreq : df.entrySet()){
				
				System.out.println("In Document: "+docFreq.getKey()+" Frequency= "+docFreq.getValue().get());
			}
		}
	}
}

//We should remove this!
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
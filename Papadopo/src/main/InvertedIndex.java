package main;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.*;
import java.util.Map.Entry;

public class InvertedIndex 
{	
	

	HashMap<String,HashMap<Integer,MutableInt>> index;//HashMap <Term,HashMap<Document,Frequency in Document>>
	public InvertedIndex()
	{
		index=new HashMap<>();
	}	
	public InvertedIndex(HashMap<String,HashMap<Integer,MutableInt>> hashmap)
	{
		index=hashmap;
	}
	public InvertedIndex(String filename)
	{
		try(ObjectInputStream in=new ObjectInputStream(new FileInputStream(filename)))
		{
			index=(HashMap<String, HashMap<Integer, MutableInt>>) in.readObject();
		} catch (FileNotFoundException e) 
		{
			e.printStackTrace();
		} catch (IOException e) 
		{
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	public synchronized void put(String word,int docId) 
	{
//		System.out.println("Put");
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
//	public HashMap<String,Set<Integer>> getHashMap()
//	{
//		return index;
//	}
	public HashMap<String,HashMap<Integer, MutableInt>> getHashMap()
	{
		return index;
	}
	
public void printIndex(){
		
		for (Map.Entry<String, HashMap<Integer, MutableInt>> e : getHashMap().entrySet())
		{
			
			System.out.println("Word: "+e.getKey());
			HashMap<Integer, MutableInt> df=e.getValue();
			for (Entry<Integer, MutableInt> docFreq : df.entrySet())
			{
				
				System.out.println("In Document: "+docFreq.getKey()+" Frequency= "+docFreq.getValue().get());
			}
		}
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
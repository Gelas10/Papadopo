package main;

import java.util.*;
import java.util.Map.Entry;



public class IndexWorker extends Thread 
{
	ArrayList<String> words;
	ArrayList<Record> records;
	int doc;
	InvertedIndex index;
	public IndexWorker(ArrayList<String> words,int doc)
	{
		this.words=words;
		this.doc=doc;
		records=new ArrayList<>();
	}
	
	public void run()
	{
		HashMap<String,MutableInt> freq=new HashMap<>();
		for (String word : words) 
		{
			MutableInt count = freq.get(word);
			if (count == null) 
			{
			    freq.put(word, new MutableInt());
			}
			else 
			{
			    count.increment();
			}
		}
		for(Entry<String,MutableInt> entry : freq.entrySet())
		{
			records.add(new Record(entry.getKey(),doc,entry.getValue()));
		}
		
	}
	public ArrayList<Record> getRecords()
	{
		return records;
	}
}

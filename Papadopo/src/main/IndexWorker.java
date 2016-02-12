package main;

import java.util.*;
import java.util.Map.Entry;


/**
 * A Thread which takes as input some words of one document as a List and the id of the document
 * Calculates the words' frequency and creates records of <term,document,frequency in document>
 * @author Gelas
 *
 */
public class IndexWorker extends Thread 
{
	List<String> words;
	ArrayList<Record> records;
	int doc;
	InvertedIndex index;
	public IndexWorker(List<String> words,int doc)
	{
		this.words=words;
		this.doc=doc;
		records=new ArrayList<>();
	}
	
	public void run()
	{
		HashMap<String,MutableInt> freq=new HashMap<>();//Frequency of words in current document (doc field)
		for (String word : words) //For each word
		{
			MutableInt count = freq.get(word);//Frequency of word in current document (doc field)
			if (count == null)//If it is the first occurrence 
			{
			    freq.put(word, new MutableInt());//create new entry
			}
			else//If this has already been seen
			{
			    count.increment();//Increment by 1 its frequency
			}
		}
		for(Entry<String,MutableInt> entry : freq.entrySet())//Fill the ArrayList of records
		{
			records.add(new Record(entry.getKey(),doc,entry.getValue()));
		}
		
	}
	public ArrayList<Record> getRecords()
	{
		return records;
	}
}

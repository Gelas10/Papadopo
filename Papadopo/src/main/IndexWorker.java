package main;

import java.util.*;

public class IndexWorker extends Thread 
{
	ArrayList<String> words;
	int doc;
	InvertedIndex index;
	public IndexWorker(ArrayList<String> words,int doc,InvertedIndex index)
	{
		this.words=words;
		this.doc=doc;
		this.index=index;
	}
	
	public void run()
	{
		for (String word : words) 
		{
//			System.out.println(word);
			index.put(word,doc);
		}
		
	}
}

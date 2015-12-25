package main;

import java.util.ArrayList;

public class IndexWorker extends Thread 
{
	ArrayList<String> words;
	public IndexWorker(ArrayList<String> words)
	{
		this.words=words;
	}
	
	public void run()
	{
		for (String word : words) 
		{
			System.out.println(word);
		}
		
	}
}

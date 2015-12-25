package main;

import java.io.*;
import java.util.*;

public class Papadopo 
{
	private static String processWord(String x) 
	{
	    return x.replaceAll("[(){},.;!?<>%]+", "").toLowerCase();//x.replaceAll("\\p{Punct}+", "");
	}
	public static void main(String[] args) 
	{
		int cores = Runtime.getRuntime().availableProcessors();
		System.out.println(cores);
//		Epic stack overflow answer:
//		If cores is less than one, either your processor is about to die, 
//		or your JVM has a serious bug in it, or the universe is about to blow up.
		
		IndexWorker[] workers=new IndexWorker[cores];//Create as many threads as there are cores
		boolean finished=false;
		int docID=1;		
		do
		{
			String filename=docID+".txt";// Reading files named [id].txt ( example : 1.txt )
			try (BufferedReader reader=new BufferedReader(new FileReader(filename)))
			{
				ArrayList<String> words=new ArrayList<>();//All the words of the file
				System.out.println(docID+")");
				for(String word: reader.readLine().split(" "))
				{
					words.add(processWord(word));	
				}
				int totalSize=words.size();
				int portion=totalSize/cores;
				int start=0;
				int end=portion;
				
				for (int i = 0; i < workers.length; i++)
				{
					
					//Give equal number of words to each thread
					ArrayList<String> cut=new ArrayList<>( words.subList(start, ( (totalSize<end) ? totalSize : end )) );
					workers[i]=new IndexWorker(cut);//Initialize thread
					workers[i].start();
					start=end;
					end+=portion;
				}
				
				for (IndexWorker worker : workers) if(worker.isAlive()) worker.join();//Wait for threads to stop
				docID++;
			} catch (IOException e) 
			{
				finished=true;
			} catch (InterruptedException e) 
			{
				e.printStackTrace();
			} 
			
		}while(!finished);
		
		
	}

}

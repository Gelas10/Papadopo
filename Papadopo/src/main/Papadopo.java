package main;

import java.io.*;
import java.text.DecimalFormat;
import java.util.*;

public class Papadopo 
{
	private static String processWord(String x) 
	{
	    return x.replaceAll("[(){},.;!?<>%]+", "").toLowerCase();//x.replaceAll("\\p{Punct}+", "");
	}
	public static void main(String[] args) 
	{
		long before=System.nanoTime();
		int cores = Runtime.getRuntime().availableProcessors();
		System.out.println("Number of cores: "+cores);
		
		//Epic stack overflow answer:
		//If cores is less than one, either your processor is about to die, 
		//or your JVM has a serious bug in it, or the universe is about to blow up.
		
		InvertedIndex index=new InvertedIndex();
		IndexWorker[] workers=new IndexWorker[cores];//Create as many threads as there are cores
		boolean finished=false;
		int docID=1;		
		do
		{
			String filename=docID+".txt";// Reading files named [id].txt ( example : 1.txt )
			try (Scanner scanner=new Scanner(new File(filename)))
			{
//				String line;
				ArrayList<String> words=new ArrayList<>();//All the words of the file
				while(scanner.hasNext())
				{
					String word=processWord(scanner.next());
					words.add(word);
					System.out.println(word);
				}
				int totalSize=words.size();
				int portion=totalSize/cores;
				int start=0;
				int end=portion;
				
				for (int i = 0; i < workers.length; i++)
				{
					
					//Give equal number of words to each thread
					ArrayList<String> cut=new ArrayList<>( words.subList(start, ( (totalSize<end) ? totalSize : end )) );
					workers[i]=new IndexWorker(cut,docID,index);//Initialize thread ( passing words, document id, and the index )
					workers[i].start();
					start=end;
					end+=portion;
				}
				
				for (IndexWorker worker : workers) if(worker.isAlive()) worker.join();//Wait for threads to stop
				docID++;
			} catch (FileNotFoundException e){
				System.out.println("No more files.");
				finished=true;
			} catch (IOException e){
				System.out.println("IOException while trying to read a line or close a file.");
				finished=true;
			} catch (InterruptedException e){
				e.printStackTrace();
			} 
			
		}while(!finished);
		
		index.printIndex();

		System.out.println("Time= "+new DecimalFormat("#.##").format((System.nanoTime()-before)/Math.pow(10, 9))+" sec");//Time= 33784762 ,34195468, 33424055
	}

}

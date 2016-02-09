package main;

import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.*;
import java.util.Map.Entry;

public class InvertedIndex 
{	
	private boolean manyIndexes=false;
	private int currentIndexId;
	private ArrayList<String> filesWithIndexes;
	private HashMap<String,HashMap<Integer,MutableInt>> index;//HashMap <Term,HashMap<Document,Frequency in Document>>
	private int documents;
	public InvertedIndex()
	{
		index=new HashMap<>();
	}
	public InvertedIndex(HashMap<String,HashMap<Integer,MutableInt>> hashmap)
	{
		index=hashmap;
	}
	public InvertedIndex(String filename,int totalDocs) throws FileNotFoundException, IOException, ClassNotFoundException
	{
		try(ObjectInputStream in=new ObjectInputStream(new FileInputStream(filename)))
		{
			index=(HashMap<String, HashMap<Integer, MutableInt>>) in.readObject();
			documents=totalDocs;
		}
		
	}
	/*
	 * If there are many indexes stored to disk
	 */
	public InvertedIndex(String pattern,int totalIndexes,int totalDocs) throws ClassNotFoundException, IOException
	{
		manyIndexes=true;
		index=new HashMap<>();
		filesWithIndexes=new ArrayList<>();
		for (int i = 0; i < totalIndexes; i++) 
		{
			filesWithIndexes.add(pattern+i+".hmp");
		}
		try(ObjectInputStream in=new ObjectInputStream(new FileInputStream(filesWithIndexes.get(0))))
		{
			index=(HashMap<String, HashMap<Integer, MutableInt>>) in.readObject();
			documents=totalDocs;
			currentIndexId=0;
		}
	}	
	
	public void put(String word,int docId) 
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

	public HashMap<String,HashMap<Integer, MutableInt>> getHashMap()
	{
		return index;
	}
	public HashMap<Integer, MutableInt> getDocumentsFrequency(String term) throws FileNotFoundException, IOException, ClassNotFoundException
	{
		
		if(manyIndexes)//Search in all indexes
		{
			HashMap<Integer, MutableInt> docFreq;
			String filename;
			for(int i=0;i<filesWithIndexes.size();i++)
			{
				docFreq=index.get(term);
				if(docFreq!=null)
					return docFreq;
				++currentIndexId;
				if(currentIndexId>=filesWithIndexes.size())
					currentIndexId=0;
				filename=filesWithIndexes.get(currentIndexId);
				try(ObjectInputStream in=new ObjectInputStream(new FileInputStream(filename)))
				{
					index=(HashMap<String, HashMap<Integer, MutableInt>>) in.readObject();
				}
			}
			
		}		
		return index.get(term);
	}
	public int getNumberOfDocuments()
	{
		return documents;
	}
	public void setDocumentsCount(int count){
		documents = count;
	}
	public void printIndex()
	{
		
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
	public void printIndexToFile(String filename)
	{
		try(BufferedWriter out = new BufferedWriter(new FileWriter(filename)))
		{
			for (Map.Entry<String, HashMap<Integer, MutableInt>> e : getHashMap().entrySet())
			{
				out.write("Word: "+e.getKey()+"\n");
				HashMap<Integer, MutableInt> df=e.getValue();
				for (Entry<Integer, MutableInt> docFreq : df.entrySet())
				{
					out.write("In Document: "+docFreq.getKey()+" Frequency= "+docFreq.getValue().get()+"\n");
				}
			}
		} catch (IOException e1) 
		{
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
	}
	
}

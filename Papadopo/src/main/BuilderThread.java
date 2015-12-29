package main;

import java.util.ArrayList;
import java.util.HashMap;

public class BuilderThread extends Thread
{
	private HashMap<String,HashMap<Integer,MutableInt>> index;//HashMap <Term,HashMap<Document,Frequency in Document>>
	private ArrayList<Record> records;
	public BuilderThread(ArrayList<Record> records)
	{
		index=new HashMap<>();
		this.records=records;
	}
	
	public void run()
	{
		buildIndex();
	}
	
	public void buildIndex()
	{
		HashMap<Integer,MutableInt> docFreq;
		for (Record record : records) 
		{
			String recTerm=record.getTerm();
			int recDoc=record.getDocument();
			MutableInt recFreq=record.getFrequency();
			docFreq=index.get(recTerm);//Get Index Information for current term
			if(docFreq==null)//If this term is NOT in the index
			{
				docFreq=new HashMap<>();//create value
				docFreq.put(recDoc, recFreq);//put document,frequency in value
				index.put(recTerm,docFreq);//put term,document,frequency in index
			}
			else//If this term is already inside the index
			{
				MutableInt currentfreq=docFreq.get(recDoc);//We get THIS DOCUMENT'S frequency
				if(currentfreq==null)//If there is no frequency for THIS DOCUMENT
				{
					docFreq.put(recDoc, recFreq);//put document,frequency
				}
				else//There is already a frequency for THIS DOCUMENT and we need to increment it by recFreq
				{
					currentfreq.incrementBy(recFreq.get());
				}
			}
			
		}
	}
	
	public HashMap<String,HashMap<Integer,MutableInt>> getIndex()
	{
		return index;
	}
}

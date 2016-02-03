package main;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;

public class SorterThread extends Thread
{
	private ArrayList<String> filenames;
	private ArrayList<Record> records;
	private String input;
	private String pattern;
	public SorterThread(String in,String out)
	{
		input=in;
		pattern=out;
	}
	
	public void run()
	{
		sortAndWrite();
	}
	
	public void sortAndWrite()
	{
		filenames=new ArrayList<>();
		
		int fileNumber=0;
		
		ArrayList<Record> records=new ArrayList<>();
		try(BufferedReader reader=new BufferedReader(new FileReader(input)))
		{
			String line;
			while((line=reader.readLine())!=null)
			{
				try
				{
					records.add(new Record(line));
				}catch(OutOfMemoryError error)
				{
					Collections.sort(records);
					filenames.add(pattern+fileNumber+".txt");
					writeToFile(pattern+(fileNumber++)+".txt",records,false);
					records.clear();
				}
			}
			if(!records.isEmpty())
			{
				Collections.sort(records);
				filenames.add(pattern+fileNumber+".txt");
				writeToFile(pattern+(fileNumber++)+".txt",records,false);
			}
			
		} catch (IOException e) 
		{
			e.printStackTrace();
		}
	}
	
	public ArrayList<String> getFilenames()
	{
		return filenames;
	}
	private static void writeToFile(String filename,ArrayList<Record> records,boolean append)
	{
		try(BufferedWriter writer=new BufferedWriter(new FileWriter(filename,append)))
		{
			for (Record record : records) 
			{
				writer.write(record.getTerm()+","+record.getDocument()+","+record.getFrequency().get()+"\n");
			}
		} catch (IOException e) 
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	
	
}

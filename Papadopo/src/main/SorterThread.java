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
//	private ArrayList<Record> records;
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
		
//		ArrayList<Record> records=new ArrayList<>();
		ArrayList<String> strings=new ArrayList<>();
		try(BufferedReader reader=new BufferedReader(new FileReader(input)))
		{
			String line;
			while((line=reader.readLine())!=null)
			{
				try
				{
//					records.add(new Record(line));
					strings.add(line);
				}catch(OutOfMemoryError error)//Not working, find other way to sort chunks
				{
					
//					Collections.sort(records);
					Collections.sort(strings);
					filenames.add(pattern+fileNumber+".txt");
//					writeToFile(pattern+(fileNumber++)+".txt",records,false);
					writeToFile2(pattern+(fileNumber++)+".txt",strings);
//					records.clear();
					strings.clear();
				}
			}
			if(!strings.isEmpty())
//			if(!records.isEmpty())
			{
//				Collections.sort(records);
				Collections.sort(strings);
				filenames.add(pattern+fileNumber+".txt");
//				writeToFile(pattern+(fileNumber++)+".txt",records,false);
				writeToFile2(pattern+(fileNumber++)+".txt",strings);
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
	private static void writeToFile2(String filename,ArrayList<String> strings)
	{
		try(BufferedWriter writer=new BufferedWriter(new FileWriter(filename,false)))
		{
			for (String string : strings) 
			{
				writer.write(string+"\n");
			}
		} catch (IOException e) 
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	
}

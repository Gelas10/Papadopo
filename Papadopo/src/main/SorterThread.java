package main;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;

public class SorterThread extends Thread
{
	private ArrayList<String> filenames;
//	private ArrayList<Record> records;
	private ArrayList<String> strings;
	private String input;
	private String pattern;
	private int fileNumber;
	private long limit;
	public SorterThread(String in,String out,long limit)
	{
		input=in;
		pattern=out;
		this.limit=limit;
	}
	
	public void run()
	{
		sortAndWrite();
	}
	public void sortAndWrite()
	{
		filenames=new ArrayList<>();
		fileNumber=0;
		strings=new ArrayList<>();
		try(BufferedReader reader=new BufferedReader(new FileReader(input)))
		{
			int count=0;
			String line;
			while((line=reader.readLine())!=null)
			{
					strings.add(line);
					++count;
					if(count>=limit)
					{
						QuickSort(strings,0,strings.size()-1);
						filenames.add(pattern+fileNumber+".txt");
						writeToFile(pattern+(fileNumber++)+".txt",strings);
						strings=new ArrayList<>();
						count=0;
					}
			}
			if(!strings.isEmpty())
			{
				System.out.println("Quicksorting one last time");
				QuickSort(strings,0,strings.size()-1);
				filenames.add(pattern+fileNumber+".txt");
				System.out.println("Successfully Quicksorted");
				writeToFile(pattern+(fileNumber)+".txt",strings);
				System.out.println("Written to file: "+pattern+(fileNumber)+".txt");
				strings=null;
			}
			else
			{
				System.out.println("Empty");
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
	private static void writeToFile(String filename,ArrayList<String> strings)
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
	public void QuickSort(ArrayList<String> a , int left , int right)
    {
        if(left >= right){return;}//Invalid input
        
        int i = left;//Set the cursors
        int j = right;
        String pivot = a.get((int)(left + (right - left) / 2));
        
        while(i<=j)//While you get {all lefts}< pivot <{all rights}
        {
            while(a.get(i).compareTo(pivot) < 0){i++;}// a[i]<pivot //Move --> until pivot
            while(a.get(j).compareTo(pivot) > 0){j--;}// a[j]>pivot //Move <-- until pivot
            if(i<=j)
            {
               String temp = a.get(i);//Swap
               a.set(i, a.get(j));
               a.set(j, temp);
               i++;
               j--;
            }
        }        
        QuickSort(a , left , j );
        QuickSort(a , i , right);
    }
	
}

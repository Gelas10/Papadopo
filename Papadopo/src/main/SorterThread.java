package main;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
/**
 * A thread which takes as input a file of unsorted records and sorts it by term
 * Also takes as input the memory limitation in lines
 * The output is X sorted files (where X depends on the limit of memory lines given as input)
 * @author Gelas
 *
 */
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
		filenames=new ArrayList<>();//The filenames of the output files
		fileNumber=0;
		strings=new ArrayList<>();//The lines read to be sorted
		try(BufferedReader reader=new BufferedReader(new FileReader(input)))
		{
			int count=0;
			String line;
			while((line=reader.readLine())!=null)//Read line which corresponds to a record
			{
					strings.add(line);
					++count;
					if(count>=limit)//If the limit of memory lines given was reached
					{
						QuickSort(strings,0,strings.size()-1);//QuickSort the lines (First word of a line is always the term by which we want to sort)
						filenames.add(pattern+fileNumber+".txt");
						writeToFile(pattern+(fileNumber++)+".txt",strings);//Write file to disk
						strings=new ArrayList<>();//Empty memory lines given
						count=0;
					}
			}
			if(!strings.isEmpty())//QuickSort the final block of records
			{
				QuickSort(strings,0,strings.size()-1);
				filenames.add(pattern+fileNumber+".txt");
				writeToFile(pattern+(fileNumber)+".txt",strings);
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

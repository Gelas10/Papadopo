package main;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

/**
 * A thread which merges 2 sorted files of records into 1 sorted file of records
 * The files are sorted by term
 * @author Gelas
 *
 */
public class MergeThread extends Thread
{
	String file1;
	String file2;
	String output;
	
	public MergeThread(String f1,String f2, String out)
	{
		file1=f1;
		file2=f2;
		output=out;
	}
	public void run()
	{
		
		
		
			if(file1!=null && file2!=null)//If the input was 2 files
			{
				
				try(BufferedWriter writer=new BufferedWriter(new FileWriter(output)))//Open writer for output file
				{
					try(BufferedReader reader1=new BufferedReader(new FileReader(file1)))//Open reader for input file1
					{
						try(BufferedReader reader2=new BufferedReader(new FileReader(file2)))//Open reader for input file2
						{
							//Read lines of both files
							String line1=reader1.readLine();
							String line2=reader2.readLine();
							//create their corresponding Record objects
							Record record1=new Record(line1);
							Record record2=new Record(line2);
							
							while((line1!=null) && (line2!=null))//While there are unread lines in both files
							{
								
								//Compare the records
								if(record1.compareTo(record2)<0)//if record1 is smaller
								{
									writer.write(line1+"\n");//write it's corresponding line to output
									line1=reader1.readLine();//and read the next
									if(line1!=null)//if end of file was not reached
									{
										record1=new Record(line1);//create its corresponding Record object
									}
									
								}
								else//Else do the same with record2
								{
									writer.write(line2+"\n");
									line2=reader2.readLine();
									if(line2!=null)
									{
										record2=new Record(line2);
									}
									
								}
							}
							//When one of the two files was read completely
							//Append it to output
							if(line1!=null)
							{
								writer.write(line1+"\n");
								
								while((line1=reader1.readLine())!=null)
								{
									writer.write(line1+"\n");
								}
							}
							else if(line2!=null)
							{
								writer.write(line2+"\n");
								
								while((line2=reader2.readLine())!=null)
								{
									writer.write(line2+"\n");
								}
							}
							new File(file1).deleteOnExit();
							new File(file2).deleteOnExit();
						
						
						} 
					}
				}catch (IOException e) 
				{
					e.printStackTrace();
				}
			}
			else if (file1!=null)//If only 1 file has been given as input for merging
			{
				new File(file1).renameTo(new File(output));//Just rename it to output filename
			}
	
	}
}

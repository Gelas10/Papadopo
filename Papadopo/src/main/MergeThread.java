package main;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

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
		
		
		
			if(file1!=null && file2!=null)
			{
				
				try(BufferedWriter writer=new BufferedWriter(new FileWriter(output)))
				{
					try(BufferedReader reader1=new BufferedReader(new FileReader(file1)))
					{
						try(BufferedReader reader2=new BufferedReader(new FileReader(file2)))
						{
							String line1=reader1.readLine();
							String line2=reader2.readLine();
							
							Record record1=new Record(line1);
							Record record2=new Record(line2);
							
							while((line1!=null) && (line2!=null))
							{
								
								
								if(record1.compareTo(record2)<0)
								{
									writer.write(line1+"\n");
									line1=reader1.readLine();
									if(line1!=null)
									{
										record1=new Record(line1);
									}
									
								}
								else
								{
									writer.write(line2+"\n");
									line2=reader2.readLine();
									if(line2!=null)
									{
										record2=new Record(line2);
									}
									
								}
							}
							
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
							
							
							new File(file1).delete();
							new File(file2).delete();
						
						
						} 
					}
				}catch (IOException e) {e.printStackTrace();}
			}
			else
			{
				new File(file1).renameTo(new File(output));
				System.out.println("renamed");
			}
//				BufferedReader reader1=new BufferedReader(new FileReader(file1));
//				BufferedReader reader2=new BufferedReader(new FileReader(file2));
//				BufferedWriter writer=new BufferedWriter(new FileWriter(output));
//				
				
		
//		catch(NullPointerException e)
//		{
//			System.out.println("NPE with "+file1+","+file2);
//			new File(file1).renameTo(new File(file1+"npe.txt"));
//			new File(file2).renameTo(new File(file2+"npe.txt"));
//		}
	
	}
}

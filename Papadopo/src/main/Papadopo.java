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
	/**
	 * Sorts records in given filename using given totalLines of memory
	 * @param filename	name of the file in which there are unordered records: term,document,frequency
	 * @param totalLines number of total lines that can be used for sorting
	 * @return An ArrayList<String> with the filenames of the sorted segments
	 */
	private static ArrayList<String> sortFromFile(String filename,int totalLines)
	{
		ArrayList<String> filenames=new ArrayList<>();
		String pattern="sorted";
		int fileNumber=0;
		
		ArrayList<Record> records=new ArrayList<>();
		try(BufferedReader reader=new BufferedReader(new FileReader(filename)))
		{
			String line;
			int count=0;
			while((line=reader.readLine())!=null)
			{
				records.add(new Record(line));				
				++count;
				if(count==totalLines)
				{
					Collections.sort(records);
					filenames.add(pattern+fileNumber+".txt");
					writeToFile(pattern+(fileNumber++)+".txt",records,false);
					records.clear();
					count=0;
				}
			}
			if(count>0)
			{
				Collections.sort(records);
				filenames.add(pattern+fileNumber+".txt");
				writeToFile(pattern+(fileNumber++)+".txt",records,false);
			}
				
			
		} catch (IOException e) 
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return filenames;
	}
	
	private static void mergeSortFiles(ArrayList<String> filenames) throws IOException, InterruptedException
	{
		ArrayList<String> nextFilenames=new ArrayList<>();
		
		int totalFiles;
		MergeThread[] mergers;
		mergers=new MergeThread[(filenames.size()/2)+1];
		
		String outfile="merged";
		int count=0;
		for (String name:filenames)
		{
			System.out.println(name);
			
		}
		do
		{
			totalFiles=filenames.size();
			System.out.println(totalFiles+" Total Files "+filenames.size());
//			if(totalFiles % 2!=0)
//				mergers=new MergeThread[totalFiles/2+1];
//			else
//				mergers=new MergeThread[totalFiles/2];

			nextFilenames.clear();
			
			
			for (int i = 0; i < mergers.length; i++) 
			{
				if(filenames.size()>1)
				{
					String file1=filenames.get(0);
					String file2=filenames.get(1);
					String mergedFile=outfile+count+".txt";
					mergers[i]=new MergeThread(file1,file2,mergedFile);
					mergers[i].start();
					filenames.remove(0);
					filenames.remove(0);
					nextFilenames.add(mergedFile);
					++count;
				}
				else if (filenames.size()>0)
				{
					if(i>0)
					{
						
//						for(int j=0;j<i;j++)
//						{
//							if(mergers[j]!=null)
//								if(mergers[j].isAlive())
//									mergers[j].join();
//						}
						mergers[i]=new MergeThread(filenames.get(0),null,outfile+count+".txt");
						mergers[i].start();
						filenames.remove(0);
						nextFilenames.add(outfile+count+".txt");
						++count;
					}
				}
			}
			for (int i = 0; i < mergers.length; i++) 
			{
				if(mergers[i]!=null)
				if(mergers[i].isAlive())
					mergers[i].join();
			}
			if(nextFilenames.size()>1)
				filenames=new ArrayList<>(nextFilenames);
		}while(filenames.size()>1);
		new File(nextFilenames.get(0)).renameTo(new File("Final.txt"));
		
	}
	
	
	public static void main(String[] args) throws InterruptedException 
	{
		String unorderedRecords="records.txt";
		new File(unorderedRecords).delete();
		
		long before=System.nanoTime();
		int cores = Runtime.getRuntime().availableProcessors();
		System.out.println("Number of cores: "+cores);
		
		//Epic stack overflow answer:
		//If cores is less than one, either your processor is about to die, 
		//or your JVM has a serious bug in it, or the universe is about to blow up.
		
		
		IndexWorker[] workers=new IndexWorker[cores];//Create as many threads as there are cores
		boolean finished=false;
		int docID=1;
		ArrayList<String> words=new ArrayList<>();//All the words of the file		
		Random r=new Random();
		ArrayList<Record> records=new ArrayList<>();//Records <term,document,frequency>
//		for (int i = 0; i < 5000000; i++) 
//		{
//			words.add("word"+i);
//		}
		before=System.nanoTime();
		do
		{
			String filename=docID+".txt";// Reading files named [id].txt ( example : 1.txt )
			try (Scanner scanner=new Scanner(new File(filename)))
			{
				words=new ArrayList<>();//All the words of the file
				while(scanner.hasNext())
				{
					String word=processWord(scanner.next());
//					if(word!="")
					words.add(word);
//					System.out.println(word);
				}
				
				int totalSize=words.size();
				int portion=totalSize/cores;
				int start=0;
				int end=portion;
				
				for (int i = 0; i < workers.length; i++)
				{
					if(totalSize-end<portion)
						end=totalSize;
					//Give equal number of words to each thread
					workers[i]=new IndexWorker(new ArrayList<>( words.subList(start, end)),docID);//Initialize thread ( passing words, document id )
					workers[i].start();
					start=end;
					end+=portion;
				}
				
				for (int i = 0; i < workers.length; i++)
				{
					if(workers[i].isAlive()) workers[i].join();//Wait for threads to stop
					
					records.addAll(workers[i].getRecords());
				}
//				for (int i = 0; i < workers.length; i++)
				writeToFile("records.txt",records,true);
				records.clear();
				System.out.println("TimeForDoc= "+new DecimalFormat("#.##").format((System.nanoTime()-before)/Math.pow(10, 9))+" sec");//
				docID++;
			} catch (FileNotFoundException e)
			{
				System.out.println("No more files.");
				finished=true;
			} catch (InterruptedException e)
			{
				e.printStackTrace();
			} 
			
		}while(!finished);
		
		System.out.println("TimeBeforeSort= "+new DecimalFormat("#.##").format((System.nanoTime()-before)/Math.pow(10, 9))+" sec");//
		//Sorting while using limited number of lines in memory
		int totalLines=100;
		ArrayList<String> files=new ArrayList<>();
		files=sortFromFile(unorderedRecords,totalLines);
		try 
		{
			mergeSortFiles(files);
		} catch (IOException e) 
		{
			// TODO Auto-generated catch block
			System.out.println("caught");
			e.printStackTrace();
		}
		//		Collections.sort(records);
		//Records are sorted , time to build the index using threads
		try(ObjectOutputStream out=new ObjectOutputStream(new FileOutputStream("s")))
		{
			// TODO build index using totalLines of memory and store segments to disk 
		} 
		catch (FileNotFoundException e) {e.printStackTrace();}
		catch (IOException e) {e.printStackTrace();}
		
		BuilderThread[] builders=new BuilderThread[cores];
		int totalSize=records.size();
		int portion=totalSize/cores;
		int start=0;
		int end=portion;
		for (int i = 0; i < builders.length; i++)
		{
			if(totalSize-end<=portion)
				end=totalSize;
			else
				while(records.get(end).getTerm()==records.get(end-1).getTerm())//end and end-1 will be separated if they are the different
				{
					System.out.println("Same");
				}
			//Give equal number of words to each thread
			ArrayList<Record> cut=new ArrayList<>( records.subList(start, end) );
//			while(cut.get(cut.size()-1).getTerm()==cut.get(cut.size()-2).getTerm())
//			{
//				--end;
//				cut.remove(cut.size()-1);
//			}
			
			builders[i]=new BuilderThread(cut);//Initialize thread ( passing words, document id )
			builders[i].start();
			start=end;
			end+=portion;
		}
		ArrayList<HashMap<String,HashMap<Integer,MutableInt>>> indexes=new ArrayList<>();
		for (int i = 0; i < builders.length; i++)
		{
			if(builders[i].isAlive()) builders[i].join();//Wait for threads to stop
			indexes.add(builders[i].getIndex());
		}
		InvertedIndex index=new InvertedIndex();
//		index.mergeIndexes(indexes);
//		System.out.println("TimeAfterSort ="+new DecimalFormat("#.##").format((System.nanoTime()-before)/Math.pow(10, 9))+" sec");//
//		for (Record record : records) 
//		{
//			System.out.println(record.getTerm()+","+record.getDocument()+","+record.getFrequency().get());
//		}
		
//		index.printIndex();

		System.out.println("Time= "+new DecimalFormat("#.##").format((System.nanoTime()-before)/Math.pow(10, 9))+" sec");//
	}

}

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
	 * @param cores number of threads to be used
	 * @return An ArrayList<String> with the filenames of the sorted segments
	 * @throws InterruptedException 
	 */
	private static ArrayList<String> sortFromFile(String[] inputFiles,int cores) throws InterruptedException
	{
		ArrayList<String> filenames=new ArrayList<>();
		String pattern="sorted-";
//		int fileNumber=0;
		SorterThread[] sorters=new SorterThread[cores];
		for (int i = 0; i < cores; i++) 
		{
			sorters[i]=new SorterThread(inputFiles[i],pattern+i+"-");
			sorters[i].start();
		}
		for (int i = 0; i < cores; i++) 
		{
			sorters[i].join();
			filenames.addAll(sorters[i].getFilenames());
		}
		
		return filenames;
	}
	
	private static void mergeSortFiles(ArrayList<String> filenames,String outputFileName) throws IOException, InterruptedException
	{
		if(filenames.size()==1)//If we tried to merge a single file
		{
			new File(filenames.get(0)).renameTo(new File(outputFileName));
		}
		else
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
			//If we tried to merge only 1 file from the beginning just rename it to outputFileName
			new File(nextFilenames.get(0)).renameTo(new File(outputFileName));
		}
	}
	
	
	public static void main(String[] args) throws InterruptedException, IOException 
	{		
		
		int totalLines=100000;//of memory
		int totalNumberOfDocuments;
		if(args.length>0)
		{
			try
			{
				totalLines=Integer.parseInt(args[0]);
			}
			catch(NumberFormatException e)
			{
				totalLines=100000;
			}
		}

		
		String unorderedRecords="records.txt";
		new File(unorderedRecords).delete();
		
		long before=System.nanoTime();
		int cores = Runtime.getRuntime().availableProcessors();
		System.out.println("Number of cores: "+cores);
		
		//Epic stack overflow answer:
		//If cores is less than one, either your processor is about to die, 
		//or your JVM has a serious bug in it, or the universe is about to blow up.
		String[] unsortedFilenames=new String[cores];
		String pattern_unsorted="records-";
		for (int i = 0; i < unsortedFilenames.length; i++) 
		{
			unsortedFilenames[i]=pattern_unsorted+i+".txt";
			new File(unsortedFilenames[i]).delete();
		}
		IndexWorker[] workers=new IndexWorker[cores];//Create as many threads as there are cores
		boolean finished=false;
		int docID=1;
		ArrayList<String> words=new ArrayList<>();//All the words of the file		
		Random r=new Random();
//		ArrayList<Record> records=new ArrayList<>();//Records <term,document,frequency>//forDeletion
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
				words=new ArrayList<>();//clearing memory
				Runtime.getRuntime().gc();//clearing memory
				for (int i = 0; i < workers.length; i++)
				{
					if(workers[i].isAlive()) workers[i].join();//Wait for threads to stop
					//Writing records as <term,doc,frequency in doc> in each thread's text file
					writeToFile(unsortedFilenames[i],workers[i].getRecords(),true);
					
//					records.addAll(workers[i].getRecords());//forDeletion
				}
//				Writing records as <term,doc,frequency in doc> in a text file
//				writeToFile(unorderedRecords,records,true);//forDeletion
//				records.clear();//forDeletion
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
		totalNumberOfDocuments=docID;
		System.out.println("TimeBeforeSort= "+new DecimalFormat("#.##").format((System.nanoTime()-before)/Math.pow(10, 9))+" sec");//
		//Sorting while using limited number of lines in memory (totalLines)
		
		ArrayList<String> files=new ArrayList<>();//Names of files which contain records sorted by term
		files=sortFromFile(unsortedFilenames,cores);//sort using <cores> threads
		String sortedRecords="sortedRecords.txt";
		mergeSortFiles(files,sortedRecords);//Merging the sorted files into one file
		
		//		Collections.sort(records);
		//Records are sorted , time to build the index
		int count=0;
		String pattern="index";
		try(BufferedReader reader=new BufferedReader(new FileReader(sortedRecords)))
		{
			HashMap<String,HashMap<Integer,MutableInt>> index=new HashMap<>();
			HashMap<Integer,MutableInt> docFreq;
			String line=reader.readLine();
			Record record;
			String recTerm="";
			do
			{
				try
				{
					record=new Record(line);
					recTerm=record.getTerm();
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
					line=reader.readLine();
					
				}
				catch(OutOfMemoryError error)
				{
					//Write HashMap to binary file
					try(ObjectOutputStream out=new ObjectOutputStream(new FileOutputStream(pattern+count+".hmp")))
					{
					//Resolving case where the term of the last record of current index is the same with the first record of the next index
						HashMap<String,HashMap<Integer,MutableInt>> temp=new HashMap<>();//Create next HashMap
						temp.put(recTerm, index.get(recTerm));//Store last term with his docs and frequencies
						index.remove(recTerm);//remove from current index
						out.writeObject(index);//write current index to disk
						System.out.println("Index "+count+" saved to file");
						index.clear();//clear the index
						index=temp;//assign current index to be the next HashMap
						Runtime.getRuntime().gc();
						++count;
					}
					catch (FileNotFoundException e) {e.printStackTrace();}
					catch (IOException e) {e.printStackTrace();}
				}
			}while(line!=null);
			//When all records were read, write to output the last index
			if(index.size()>0)
			{
				//Write HashMap to binary file
				try(ObjectOutputStream out=new ObjectOutputStream(new FileOutputStream(pattern+count+".hmp")))
				{
					out.writeObject(index);		
					System.out.println("Index "+count+" saved to file");
					++count;
				}
				catch (FileNotFoundException e) {e.printStackTrace();}
				catch (IOException e) {e.printStackTrace();}
			}
			
		}
		InvertedIndex s;
		try 
		{
			s = new InvertedIndex(pattern+0+".hmp",totalNumberOfDocuments);
//			System.out.println(s.getDocumentsFrequency("same").get(3).get());
//			s.printIndex();
//			s.printIndexToFile("printedIndex.txt");
		} catch (ClassNotFoundException e) 
		{
			e.printStackTrace();
		}
//		try 
//		{
//			s = new InvertedIndex(pattern,1,totalNumberOfDocuments);
//			System.out.println(s.getDocumentsFrequency("same").get(3).get());
//		} catch (ClassNotFoundException e) 
//		{
//			e.printStackTrace();
//		}
		System.out.println("Index Built and stored in "+count+" files");
		System.out.println("Time= "+new DecimalFormat("#.##").format((System.nanoTime()-before)/Math.pow(10, 9))+" sec");//
	}

}
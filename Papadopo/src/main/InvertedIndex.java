package main;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.text.DecimalFormat;
import java.util.*;

public class InvertedIndex
{	
	private boolean manyIndexes=false;
	private int currentIndexId;
	private ArrayList<String> filesWithIndexes;
	private HashMap<String,HashMap<Integer,MutableInt>> index;//HashMap <Term,HashMap<Document,Frequency in Document>>
	private int documents;
	private HashMap<Integer,Integer> totalWordsInDocument;
	public InvertedIndex()
	{
		index=new HashMap<>();
		totalWordsInDocument=new HashMap<>();
	}
	/**
	 * Builds the index from documents on disk using as many threads as there are cores
	 * The documents need to be named: 1.txt, 2.txt, ... N.txt
	 * 
	 * Implementation:
	 * 
	 * Phase 1)Creating Records <Term,Document,Frequency>:
	 * -Read next document and store its words in an ArrayList
	 * -Cut the ArrayList into almost equal pieces and pass them to IndexWorker threads
	 * -Write the records returned from each worker to disk ( There will be as many Record files as there are Cores-Threads )
	 * 
	 * Phase2)Sorting and Merging the Record files:
	 * -Pass to each Sorter Thread one Record File
	 * -Merge the sorted files produced by the threads
	 * 
	 * Phase3)Building the Index:
	 * -Approximation of memory limitation using number of total records
	 * -If memory limitation does not let us hold the index in memory
	 * 	.Read sorted records from file and hash them to index until limit is reached
	 * 	.Store the part of the index to disk
	 * 	.Continue until all sorted records have been read
	 * -If index can be built completely in memory
	 *  .Read all sorted records and hash them to index
	 *  
	 */
	public void buildIndex()
	{

		int totalNumberOfDocuments;
		
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
			File f=new File(unsortedFilenames[i]);
			f.delete();
			f.deleteOnExit();
		}
		IndexWorker[] workers=new IndexWorker[cores];//Create as many threads as there are cores
		boolean finished=false;
		int docID=1;
		List<String> words;//All the words of the file	
		
		ArrayList<Record> records=new ArrayList<>();//Records <term,document,frequency>
		long totalRecords=0;
		String word;
		before=System.nanoTime();//timing
		do//Phase1) Creating the records
		{
			String filename = QueryProcessor.pathToDocumentsFolder+docID+".txt";// Reading files named [id].txt ( example : 1.txt )
			try (Scanner scanner=new Scanner(new File(filename)))
			{
				
				words=new ArrayList<>();
				while(scanner.hasNext())
				{
					word=processWord(scanner.next());//Remove Punctuation
					words.add(word);
				}
				
				int totalSize=words.size();
				totalWordsInDocument.put(docID, totalSize);//Hash document id to how many words this document has
				int portion=totalSize/cores;
				int start=0;
				int end=portion;
				
				for (int i = 0; i < workers.length; i++)
				{
					if(i==workers.length-1)
						end=totalSize;
					//Give equal number of words to each thread					
					workers[i]=new IndexWorker(words.subList(start, end),docID);//Initialize thread ( passing words, document id )
					workers[i].start();
					start=end;
					end+=portion;
				}
				words=null;//Clearing memory
				for (int i = 0; i < workers.length; i++)
				{
					if(workers[i].isAlive()) workers[i].join();//Wait for threads to stop
					//Writing records as <term,doc,frequency in doc> in each thread's text file
					records=workers[i].getRecords();
					totalRecords+=records.size();
					writeToFile(unsortedFilenames[i],records,true);
					workers[i]=null;//Dereferencing for memory clear
				}
//				Writing records as <term,doc,frequency in doc> in a text file
				System.out.println("TimeForDoc "+docID+"= "+new DecimalFormat("#.##").format((System.nanoTime()-before)/Math.pow(10, 9))+" sec");//
				records=new ArrayList<>();
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
		//Phase2) Sorting while using limited number of lines in memory
		ArrayList<String> files=new ArrayList<>();//Names of files which contain records sorted by term
		files=sortFromFile(unsortedFilenames,cores,totalRecords);//sort using <cores> threads
		String sortedRecords="sortedRecords.txt";
		mergeSortFiles(files,sortedRecords);//Merging the sorted files into one file
		System.out.println("TimeAfterSort= "+new DecimalFormat("#.##").format((System.nanoTime()-before)/Math.pow(10, 9))+" sec");//
				
		//Phase3) Records are sorted , time to build the index
		long fileSize=new File(sortedRecords).length();
		long freeMemory=Runtime.getRuntime().freeMemory();
		System.out.println(fileSize/(1024*1024)+"mb : "+freeMemory/(1024*1024)+"mb");
		int turns=(int)(fileSize/freeMemory) +2;
		if(fileSize<freeMemory*2/3)
			turns=1;
	
		long limit=1+ totalRecords/turns;
		
		int lineCount=0;
		int indexCount=0;
		String pattern="index";
		HashMap<String,HashMap<Integer,MutableInt>> indexHashMap=null;
		try(BufferedReader reader=new BufferedReader(new FileReader(sortedRecords)))
		{
			indexHashMap=new HashMap<>();
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
					docFreq=indexHashMap.get(recTerm);//Get Index Information for current term
					if(docFreq==null)//If this term is NOT in the index
					{
						docFreq=new HashMap<>();//create value
						docFreq.put(recDoc, recFreq);//put document,frequency in value
						indexHashMap.put(recTerm,docFreq);//put term,document,frequency in index
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
					if(lineCount>=limit)
					{
						System.out.println("Writing index "+indexCount);
						try(ObjectOutputStream out=new ObjectOutputStream(new FileOutputStream(pattern+indexCount+".hmp")))
						{
						//Resolving case where the term of the last record of current index is the same with the first record of the next index
							HashMap<String,HashMap<Integer,MutableInt>> temp=new HashMap<>();//Create next HashMap
							temp.put(recTerm, indexHashMap.remove(recTerm));//Store last term with his docs and frequencies and remove from current index
							
							out.writeObject(indexHashMap);//write current index to disk
							System.out.println("Index "+indexCount+" saved to file");
							indexHashMap=temp;//assign current index to be the next HashMap
							++indexCount;
						}
						catch (FileNotFoundException e) {e.printStackTrace();}
						catch (IOException e) {e.printStackTrace();}
						lineCount=0;
					}
					++lineCount;
					line=reader.readLine();
					
				}
				catch(OutOfMemoryError error){System.out.println("OUT OF MEMORY");}				
			}while(line!=null);
			//When all records were read, write to output the last index
			if(indexHashMap.size()>0)
			{
				if(indexCount>0)
				{
					System.out.println("Writing index "+indexCount+" (Last Index)");
					//Write HashMap to binary file
					try(ObjectOutputStream out=new ObjectOutputStream(new FileOutputStream(pattern+indexCount+".hmp")))
					{
						out.writeObject(indexHashMap);		
						System.out.println("Index "+indexCount+" saved to file");
					}
					catch (FileNotFoundException e) {e.printStackTrace();}
					catch (IOException e) {e.printStackTrace();}
				}
				else
				{
					System.out.println("Finished Building Index");					
				}
				
			}
			
		} catch (IOException e1) {e1.printStackTrace();}		
		System.out.println("Index Built and stored in "+(indexCount+1)+" files");
		System.out.println("Time= "+new DecimalFormat("#.##").format((System.nanoTime()-before)/Math.pow(10, 9))+" sec");//
		System.out.println();
		index=indexHashMap;
		documents=totalNumberOfDocuments;
		if(indexCount>0)
		{
			manyIndexes=true;
			filesWithIndexes=new ArrayList<>();
			for (int i = 0; i <= indexCount; i++) 
			{
				filesWithIndexes.add(pattern+i+".hmp");
			}
			currentIndexId=indexCount;
		}				
		
		System.out.println("Deleting file : "+sortedRecords);
		new File(sortedRecords).deleteOnExit();
		

	}
	/**
	 * Searches for the term's occurrence list inside the index which is stored in memory
	 * If it doesn't find it and there are other parts of the index stored on the disk:
	 * Reads each part of the index and brings it in memory until the term's occurrence list is found or there are no more parts on disk
	 * @param term the term whose occurrence list is being asked
	 * @return the occurrence list
	 */
	@SuppressWarnings("unchecked")
	public synchronized HashMap<Integer, MutableInt> getDocumentsFrequency(String term) 
	{
		
		if(manyIndexes)//Search in all indexes
		{
			HashMap<Integer, MutableInt> docFreq;
			String filename;
			for(int i=0;i<filesWithIndexes.size();i++)//For as many times as there are indexes
			{
				docFreq=index.get(term);//Search the occurrence list
				if(docFreq!=null)//If found
					return docFreq;//Return it
				++currentIndexId;//If not found: increment currendIndexId so that another part of the index is read from disk
				if(currentIndexId>=filesWithIndexes.size())
					currentIndexId=0;
				filename=filesWithIndexes.get(currentIndexId);//Get the filename which corresponds to this part of the index
				try(ObjectInputStream in=new ObjectInputStream(new BufferedInputStream(new FileInputStream(filename),16*1024)))
				{
					index=(HashMap<String, HashMap<Integer, MutableInt>>) in.readObject();//Bring it to memory
				} catch (Exception e) {e.printStackTrace();} 
			}
			
		}		
		return index.get(term);
	}
	public int getNumberOfDocuments()
	{
		return documents;
	}
	
	public int getSizeOfDocument(int docid)
	{
		return totalWordsInDocument.get(docid);
	}
	//Functions used for building index
	public String processWord(String x) 
	{
	    return x.replaceAll("[(){},.;!?<>%]+", "").toLowerCase();//x.replaceAll("\\p{Punct}+", "");
	}
	
	private void writeToFile(String filename,ArrayList<Record> records,boolean append)
	{
		try(BufferedWriter writer=new BufferedWriter(new FileWriter(filename,append)))
		{
			for (Record record : records) 
			{
				writer.write(record.getTerm()+","+record.getDocument()+","+record.getFrequency().get()+"\n");
			}
		} catch (IOException e) {e.printStackTrace();}
	}
	/**
	 * Sorts files of records using threads and limited memory
	 * @param inputFiles ALL the files which need to be sorted
	 * @param cores How many threads to use
	 * @param totalRecords how many records are inside all the files ( Used for memory limit approximation )
	 * @return The filenames of the sorted files which then need to be merged
	 */
	private ArrayList<String> sortFromFile(String[] inputFiles,int cores,long totalRecords) 
	{
		ArrayList<String> filenames=new ArrayList<>();//Output filenames
		String pattern="sorted-";
//		int fileNumber=0;
		SorterThread[] sorters=new SorterThread[cores];//Threads which will sort each file
		//Approximation of chunk size using free memory and fileSize
		long length=(new File(inputFiles[0]).length())*cores;//length of file 1 multiplied by number of threads = approximation of total size of files
		long freeMemory=Runtime.getRuntime().freeMemory();
		int timesToRun=(int)(length/freeMemory) +2;
		if(length<freeMemory/2)//If there is plenty of memory just run once
			timesToRun=1;
		long linesPerFile=totalRecords/cores;//How many lines does a thread need to run only once
		long limitOfLines=1+linesPerFile/timesToRun;//How many memory lines will be given to each thread
		for (int i = 0; i < cores; i++) //Launch threads
		{
			sorters[i]=new SorterThread(inputFiles[i],pattern+i+"-",limitOfLines);
			sorters[i].start();
		}
		for (int i = 0; i < cores; i++) 
		{
			//Wait for threads to stop
			try { sorters[i].join(); } catch (InterruptedException e){ e.printStackTrace();}
				
			
			filenames.addAll(sorters[i].getFilenames());
		}
		
		return filenames;
	}
	/**
	 * Merges many sorted files by term to one
	 * @param filenames ALL the files to merge
	 * @param outputFileName the filename of the merged sorted file
	 */
	private void mergeSortFiles(ArrayList<String> filenames,String outputFileName)
	{
		if(filenames.size()==1)//If we tried to merge a single file
		{
			new File(filenames.get(0)).renameTo(new File(outputFileName));
		}
		else
		{
			
		
			ArrayList<String> nextFilenames=new ArrayList<>();//What files will need to be merged in the next phase
			
			MergeThread[] mergers;
			mergers=new MergeThread[(filenames.size()/2)+1];//Create half as many mergers as there are files +1
			
			String outfile="merged";
			int count=0;
			
			do
			{
				nextFilenames.clear();
				
				
				for (int i = 0; i < mergers.length; i++) //For each merger thread
				{
					if(filenames.size()>1)//If there is more than 1 file to merge
					{
						String file1=filenames.get(0);//Get 2 filenames from the list of files to be merged
						String file2=filenames.get(1);
						String mergedFile=outfile+count+".txt";
						mergers[i]=new MergeThread(file1,file2,mergedFile);//Give them for merging to the merger thread
						mergers[i].start();
						filenames.remove(0);//Remove them from the list as they have now been merged into one
						filenames.remove(0);
						nextFilenames.add(mergedFile);//Add merged file to next phase's files to be merged if necessary
						++count;
					}
					else if (filenames.size()>0)//If there is only 1 file to merge
					{
						if(i>0)//If there wasn't only 1 file from the beginning of the phase
						{							
							mergers[i]=new MergeThread(filenames.get(0),null,outfile+count+".txt");
							mergers[i].start();
							filenames.remove(0);
							nextFilenames.add(outfile+count+".txt");
							++count;
						}
					}
				}
				//Wait for all threads of current phase to finish before enacting the next phase
				for (int i = 0; i < mergers.length; i++) 
				{
					if(mergers[i]!=null)
						if(mergers[i].isAlive())
							try { mergers[i].join();} catch (InterruptedException e) {e.printStackTrace();}
				}
				if(nextFilenames.size()>1)//If there are more than 1 file to merge
					filenames=new ArrayList<>(nextFilenames);//Place filenames to the list of files to be merged
			}while(filenames.size()>1);//Until there is only 1 file produced from a phase ( The final file )
			//If we tried to merge only 1 file from the beginning just rename it to outputFileName
			new File(nextFilenames.get(0)).renameTo(new File(outputFileName));
		}
	}
}

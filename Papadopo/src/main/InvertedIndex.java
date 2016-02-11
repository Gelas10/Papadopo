package main;

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
import java.io.Serializable;
import java.text.DecimalFormat;
import java.util.*;
import java.util.Map.Entry;

public class InvertedIndex implements Serializable
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
	
	public InvertedIndex(HashMap<String,HashMap<Integer,MutableInt>> hashmap)
	{
		index=hashmap;
	}
	public InvertedIndex(String filename,int totalDocs) throws FileNotFoundException, IOException, ClassNotFoundException
	{
		try(ObjectInputStream in=new ObjectInputStream(new FileInputStream(filename)))
		{
			index=(HashMap<String, HashMap<Integer, MutableInt>>) in.readObject();
			documents=totalDocs;
		}
		
	}
	
	/*
	 * If there are many indexes stored to disk
	 */
//	public InvertedIndex(String pattern,int totalIndexes,int totalDocs) throws ClassNotFoundException, IOException
//	{
//		manyIndexes=true;
//		index=new HashMap<>();
//		filesWithIndexes=new ArrayList<>();
//		for (int i = 0; i < totalIndexes; i++) 
//		{
//			filesWithIndexes.add(pattern+i+".hmp");
//		}
//		try(ObjectInputStream in=new ObjectInputStream(new FileInputStream(filesWithIndexes.get(0))))
//		{
//			index=(HashMap<String, HashMap<Integer, MutableInt>>) in.readObject();
//			documents=totalDocs;
//			currentIndexId=0;
//		}
//	}	
	public void manyIndexes(String pattern,int totalIndexes,int totalDocs)
	{
		manyIndexes=true;
		index=new HashMap<>();
		filesWithIndexes=new ArrayList<>();
		for (int i = 0; i < totalIndexes; i++) 
		{
			filesWithIndexes.add(pattern+i+".hmp");
		}
		try(ObjectInputStream in=new ObjectInputStream(new FileInputStream(filesWithIndexes.get(0))))
		{
			index=(HashMap<String, HashMap<Integer, MutableInt>>) in.readObject();
			documents=totalDocs;
			currentIndexId=0;
		} 
		catch (IOException e) {e.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	public void buildIndex()
	{

//		int totalLines=100000;//of memory
		int totalNumberOfDocuments;
//		if(args.length>0)
//		{
//			try
//			{
//				totalLines=Integer.parseInt(args[0]);
//			}
//			catch(NumberFormatException e)
//			{
//				totalLines=100000;
//			}
//		}
		
		
//		String unorderedRecords="records.txt";
//		new File(unorderedRecords).delete();
		
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
		List<String> words;//=new LinkedList<>();//ArrayList<>();//All the words of the file	
		
//		Random r=new Random();
		ArrayList<Record> records=new ArrayList<>();//Records <term,document,frequency>
		long totalRecords=0;
//		for (int i = 0; i < 5000000; i++) 
//		{
//			words.add("word"+i);
//		}
		String word;
//		words=new ArrayList<>();//All the words of the file
		before=System.nanoTime();
		do
		{
			String filename=docID+".txt";// Reading files named [id].txt ( example : 1.txt )
			try (Scanner scanner=new Scanner(new File(filename)))
			{
				
				words=new ArrayList<>();
//				words.clear();
				while(scanner.hasNext())
				{
					word=processWord(scanner.next());
					words.add(word);
//					System.out.println(word);
				}
				
				int totalSize=words.size();
				totalWordsInDocument.put(docID, totalSize);
				int portion=totalSize/cores;
//				portion+=totalSize%cores;
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
				words=null;
//				words=new ArrayList<>();//clearing memory
//				Runtime.getRuntime().gc();//clearing memory
				for (int i = 0; i < workers.length; i++)
				{
					if(workers[i].isAlive()) workers[i].join();//Wait for threads to stop
					//Writing records as <term,doc,frequency in doc> in each thread's text file
					records=workers[i].getRecords();
					totalRecords+=records.size();
					writeToFile(unsortedFilenames[i],records,true);
					workers[i]=null;//Dereferencing for memory clear
//					records.addAll(workers[i].getRecords());//forDeletion
				}
//				Writing records as <term,doc,frequency in doc> in a text file
//				writeToFile(unorderedRecords,records,true);//forDeletion
//				records.clear();//forDeletion
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
		//Sorting while using limited number of lines in memory (totalLines)
//For testing		
//Random r=new Random();
//for (String filename : unsortedFilenames) 
//{
//	try(BufferedWriter writer=new BufferedWriter(new FileWriter(filename,true)))
//	{
//		for(int i=0; i<300000; i++) 
//		{
//			writer.write("test"+i+","+r.nextInt(docID-1)+","+r.nextInt(20)+"\n");
//			writer.write("test"+i+","+r.nextInt(docID-1)+","+r.nextInt(20)+"\n");
//			writer.write("test"+i+","+r.nextInt(docID-1)+","+r.nextInt(20)+"\n");
//			writer.write("test"+i+","+r.nextInt(docID-1)+","+r.nextInt(20)+"\n");
//			writer.write("test"+i+","+r.nextInt(docID-1)+","+r.nextInt(20)+"\n");
//		}
//	} catch (IOException e) 
//	{
//		// TODO Auto-generated catch block
//		e.printStackTrace();
//	}
//}
//System.out.println("TimeBeforeSort= "+new DecimalFormat("#.##").format((System.nanoTime()-before)/Math.pow(10, 9))+" sec");//
		ArrayList<String> files=new ArrayList<>();//Names of files which contain records sorted by term
		files=sortFromFile(unsortedFilenames,cores,totalRecords);//sort using <cores> threads
		String sortedRecords="sortedRecords.txt";
		mergeSortFiles(files,sortedRecords);//Merging the sorted files into one file
		System.out.println("TimeAfterSort= "+new DecimalFormat("#.##").format((System.nanoTime()-before)/Math.pow(10, 9))+" sec");//
		//		Collections.sort(records);
		//Records are sorted , time to build the index
		long fileSize=new File(sortedRecords).length();
		long freeMemory=Runtime.getRuntime().freeMemory();
		System.out.println(fileSize/(1024*1024)+"mb : "+freeMemory/(1024*1024)+"mb");
		int turns=(int)(fileSize/freeMemory) +2;
		if(fileSize<freeMemory/2)
			turns=1;
//	totalRecords=1600000*4;
		long limit=1+ totalRecords/turns;
		
		int lineCount=0;
		int indexCount=0;
		String pattern="index";
		try(BufferedReader reader=new BufferedReader(new FileReader(sortedRecords)))
		{
			HashMap<String,HashMap<Integer,MutableInt>> indexHashMap=new HashMap<>();
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
//					System.out.println(lineCount);
					if(lineCount>=limit)
					{
						System.out.println("Writing and index");
						try(ObjectOutputStream out=new ObjectOutputStream(new FileOutputStream(pattern+indexCount+".hmp")))
						{
						//Resolving case where the term of the last record of current index is the same with the first record of the next index
							HashMap<String,HashMap<Integer,MutableInt>> temp=new HashMap<>();//Create next HashMap
							temp.put(recTerm, indexHashMap.remove(recTerm));//Store last term with his docs and frequencies and remove from current index
							
							out.writeObject(indexHashMap);//write current index to disk
							System.out.println("Index "+indexCount+" saved to file");
//							index.clear();//clear the index
							indexHashMap=temp;//assign current index to be the next HashMap
//							Runtime.getRuntime().gc();
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
				//Write HashMap to binary file
				try(ObjectOutputStream out=new ObjectOutputStream(new FileOutputStream(pattern+indexCount+".hmp")))
				{
					out.writeObject(indexHashMap);		
					System.out.println("Index "+indexCount+" saved to file");
					++indexCount;
				}
				catch (FileNotFoundException e) {e.printStackTrace();}
				catch (IOException e) {e.printStackTrace();}
			}
			
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		manyIndexes(pattern,indexCount,totalNumberOfDocuments);
		
		System.out.println("Deleting file : "+sortedRecords);
		new File(sortedRecords).deleteOnExit();
		System.out.println("Index Built and stored in "+indexCount+" files");
		System.out.println("Time= "+new DecimalFormat("#.##").format((System.nanoTime()-before)/Math.pow(10, 9))+" sec");//
		
		try(ObjectOutputStream out=new ObjectOutputStream(new FileOutputStream("indexObject"+".obj")))
		{
			out.writeObject(this);		
			System.out.println("Object Written to Disk");
		}
		catch (FileNotFoundException e) {e.printStackTrace();}
		catch (IOException e) {e.printStackTrace();}
	}
	
	public void put(String word,int docId) 
	{
//		System.out.println("Put");
		HashMap<Integer, MutableInt> freq = new HashMap<>();
		HashMap<Integer, MutableInt> indexFreq = index.get(word);
		
		if(indexFreq!=null)
		{
			freq=indexFreq;//Getting the frequency of word in EACH document
		}		
		
		MutableInt count = freq.get(docId);
		if (count == null) 
		{
		    freq.put(docId, new MutableInt());
		}
		else 
		{
		    count.increment();
		}
		index.put(word, freq);
		
	}

	public HashMap<String,HashMap<Integer, MutableInt>> getHashMap()
	{
		return index;
	}
	public HashMap<Integer, MutableInt> getDocumentsFrequency(String term) 
	{
		
		if(manyIndexes)//Search in all indexes
		{
			HashMap<Integer, MutableInt> docFreq;
			String filename;
			for(int i=0;i<filesWithIndexes.size();i++)
			{
				docFreq=index.get(term);
				if(docFreq!=null)
					return docFreq;
				++currentIndexId;
				if(currentIndexId>=filesWithIndexes.size())
					currentIndexId=0;
				filename=filesWithIndexes.get(currentIndexId);
				try(ObjectInputStream in=new ObjectInputStream(new FileInputStream(filename)))
				{
					index=(HashMap<String, HashMap<Integer, MutableInt>>) in.readObject();
				} catch (Exception e) 
				{
					// TODO Auto-generated catch block
					e.printStackTrace();
				} 
			}
			
		}		
		return index.get(term);
	}
	public int getNumberOfDocuments()
	{
		return documents;
	}
	public void setDocumentsCount(int count){
		documents = count;
	}
	public void printIndex()
	{
		
		for (Map.Entry<String, HashMap<Integer, MutableInt>> e : getHashMap().entrySet())
		{
			
			System.out.println("Word: "+e.getKey());
			HashMap<Integer, MutableInt> df=e.getValue();
			for (Entry<Integer, MutableInt> docFreq : df.entrySet())
			{
				
				System.out.println("In Document: "+docFreq.getKey()+" Frequency= "+docFreq.getValue().get());
			}
		}
	}
	public void printIndexToFile(String filename)
	{
		try(BufferedWriter out = new BufferedWriter(new FileWriter(filename)))
		{
			for (Map.Entry<String, HashMap<Integer, MutableInt>> e : getHashMap().entrySet())
			{
				out.write("Word: "+e.getKey()+"\n");
				HashMap<Integer, MutableInt> df=e.getValue();
				for (Entry<Integer, MutableInt> docFreq : df.entrySet())
				{
					out.write("In Document: "+docFreq.getKey()+" Frequency= "+docFreq.getValue().get()+"\n");
				}
			}
		} catch (IOException e1) 
		{
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
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
		} catch (IOException e) 
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	private ArrayList<String> sortFromFile(String[] inputFiles,int cores,long totalRecords) 
	{
		ArrayList<String> filenames=new ArrayList<>();
		String pattern="sorted-";
//		int fileNumber=0;
		SorterThread[] sorters=new SorterThread[cores];
		//Approximation of chunk size using free memory and filesize
		long length=(new File(inputFiles[0]).length())*cores;//22.6*4 = 90mb
		long freeMemory=Runtime.getRuntime().freeMemory();//58mb
		int timesToRun=(int)(length/freeMemory) +2;
		if(length<freeMemory/2)
			timesToRun=1;
		long linesPerFile=totalRecords/cores;
		long limitOfLines=1+linesPerFile/timesToRun;
		System.out.println(length/(1024*1024) +"mb : "+freeMemory/(1024*1024)+"mb");
		for (int i = 0; i < cores; i++) 
		{
			sorters[i]=new SorterThread(inputFiles[i],pattern+i+"-",limitOfLines);
			sorters[i].start();
		}
		for (int i = 0; i < cores; i++) 
		{
			try {
				sorters[i].join();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			filenames.addAll(sorters[i].getFilenames());
		}
		
		return filenames;
	}
	
	private void mergeSortFiles(ArrayList<String> filenames,String outputFileName)
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
						try {
							mergers[i].join();
						} catch (InterruptedException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
				}
				if(nextFilenames.size()>1)
					filenames=new ArrayList<>(nextFilenames);
			}while(filenames.size()>1);
			//If we tried to merge only 1 file from the beginning just rename it to outputFileName
			new File(nextFilenames.get(0)).renameTo(new File(outputFileName));
		}
	}
}

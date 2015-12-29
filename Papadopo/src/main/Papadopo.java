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
	
	private static ArrayList<String> sortFromFile(String filename)
	{
		ArrayList<String> filenames=new ArrayList<>();
		String pattern="sorted";
		int fileNumber=0;
		int totalLines=10;
		ArrayList<Record> records=new ArrayList<>();
		try(BufferedReader reader=new BufferedReader(new FileReader(filename)))
		{
			String line;
			String[] tokens=new String[3];
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
		boolean fackedup=false;
		boolean finished=false;
		int fackedi=-1;
		ArrayList<String> nextFilenames=new ArrayList<>();
		int cores=Runtime.getRuntime().availableProcessors();
		
		int totalFiles;
		MergeThread[] mergers;
		mergers=new MergeThread[(filenames.size()/2)+1];
		
		String outfile="merged";
		int count=0;
		for (String name:filenames)
		{
			System.out.println(name);
			
		}
//		while(!filenames.isEmpty())
//		{
//			for (int i = 0; i < mergers.length; i++) 
//			{
//				String file1;
//				String file2;
//				String mergedFile=outfile+count+".txt";;
//				if(filenames.isEmpty())
//					break;
//				else if(filenames.size()>1)
//				{
//					file1=filenames.get(0);
//					filenames.remove(0);
//					file2=filenames.get(0);
//					filenames.remove(0);
//					mergedFile=outfile+count+".txt";
//					mergers[i]=new MergeThread(file1,file2,mergedFile);
//					mergers[i].start();
//				}
//				else if(filenames.size()==1)
//				{
//					mergedFile=filenames.get(0);
//					filenames.remove(0);
//				}
//				
//				nextFilenames.add(mergedFile);
//				++count;
//			}
//			for(int i=0 ; i<mergers.length; i++)
//			{
//				if(mergers[i]!=null)
//					if(mergers[i].isAlive())
//						mergers[i].join();
//			}
//			if(filenames.isEmpty())
//			{
//				if(nextFilenames.size()>1)
//				{
//					filenames=new ArrayList<>(nextFilenames);
//				}
//				else if (nextFilenames.size()==1)
//				{
//					
//				}
//			}
//		}
//		System.out.println("Count "+count);
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
//				System.out.println(totalFiles+" Total Files inside "+filenames.size());
				
//				for (String filename : filenames) 
//				{
//					System.out.println(filename);
//				}
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
					
//					new File(filenames.get(0)).renameTo(new File(outfile+count+".txt"));
//					System.out.println("ELSE");
//					filenames.remove(0);
//					nextFilenames.add(outfile+count+".txt");
//					fackedup=true;
//					fackedi=i;
//					++count;
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
						
//						System.out.println(outfile+count+".txt");
//						System.out.println("BB");
						nextFilenames.add(outfile+count+".txt");
						++count;
					}
					
					
					
				}
				else
				{
					
				}
				
			}
//			for (int i = 0; i < totalFiles; i+=2) 
//			{
//				
//				if(i+1<totalFiles)
//				{
//					mergers[threadCount]=new MergeThread(filenames.get(i),(i+1>=totalFiles) ? null:filenames.get(i+1),outfile+count+".txt");				
//					mergers[threadCount].start();
////					filenames.get(i)=outfile+count+".txt";
//					nextFilenames.add(outfile+count+".txt");
//					++threadCount;
//					++count;
//				}
//				else
//				{
//					System.out.println("hI");
//				}
//				
//			}
			for (int i = 0; i < mergers.length; i++) 
			{
				if(mergers[i]!=null)
				if(mergers[i].isAlive())
					mergers[i].join();
			}
//			filenames.clear();
//			filenames=nextFilenames;
			if(nextFilenames.size()>1)
				filenames=new ArrayList<>(nextFilenames);
		}while(filenames.size()>1);
		//new File(filenames.get(0)).renameTo(new File("Final.txt"));
//		System.out.println(filenames.size());
//		merge2Files(filenames[0],filenames[1]);
		if(fackedup)System.out.println("FACKED UP"+fackedi);
		
	}
	
	
	public static void main(String[] args) throws InterruptedException 
	{
		String output="records.txt";
		new File(output).delete();
		
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
		
		ArrayList<String> files=new ArrayList<>();
		files=sortFromFile(output);
		try {
			mergeSortFiles(files);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			System.out.println("caught");
			e.printStackTrace();
		}
		//		Collections.sort(records);
		//Records are sorted , time to build the index using threads
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

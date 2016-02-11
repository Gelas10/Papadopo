package main;

import java.io.FileInputStream;
import java.io.ObjectInputStream;

public class Papadopo 
{
	
	
	
	public static void main(String[] args) 
	{		
		System.out.println("Only Building index");
		InvertedIndex s=new InvertedIndex();
		try(ObjectInputStream in=new ObjectInputStream(new FileInputStream("indexObject.obj")))
		{
			s=(InvertedIndex)in.readObject();
		} catch (Exception e) {e.printStackTrace();}
//		s.buildIndex();
		System.out.println(s.getSizeOfDocument(1)+" "+s.getNumberOfDocuments()+" "+s.getSizeOfDocument(3));
		
		
//		s.printIndexToFile("printed.txt");
//		s.getSizeOfDocument(1);
	}

}
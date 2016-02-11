package main;

public class Papadopo 
{
	
	
	
	public static void main(String[] args) 
	{		
		System.out.println("Only Building index");
		InvertedIndex s=new InvertedIndex();
		s.buildIndex();
		System.out.println(s.getSizeOfDocument(1));
		s.printIndexToFile("printed.txt");;
//		s.getSizeOfDocument(1);
	}

}
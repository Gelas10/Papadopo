package main;

import java.io.*;
import java.util.*;

public class Papadopo 
{

	public static void main(String[] args) 
	{
		// Reading files named [id].txt ( example : 1.txt )
		boolean finished=false;
		int count=1;
		do
		{
			String filename=count+".txt";
			try (BufferedReader reader=new BufferedReader(new FileReader(filename)))
			{
				System.out.println(count+")");
				for(String word: reader.readLine().split(" "))
				{
					System.out.println(word);
					
				}
				count++;
			} catch (IOException e) 
			{
				finished=true;
			}
			
		}while(!finished);
		
		
	}

}

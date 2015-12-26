package main;

import java.util.*;

public class InvertedIndex 
{
	HashMap<String,ArrayList<Integer>> index;//HashMap <Term,Docs in which the term is found>
	public InvertedIndex()
	{
		index=new HashMap<>();
	}
	//Merge 2 HashMaps, Return Merged HashMap
	public HashMap<String,ArrayList<Integer>> merge(HashMap<String,ArrayList<Integer>> map1,HashMap<String,ArrayList<Integer>> map2)
	{
		for (Map.Entry<String, ArrayList<Integer>> e : map2.entrySet())//For each entry<Term,Documents> in map2
		{
			ArrayList<Integer> map1Values=map1.get(e.getKey());//Get the Documents of map1 for this Term 
			if(map1Values!=null)//If there are Documents in map1 for this Term
			{
				for (Integer docID : e.getValue()) //For each document in map2's entry
					if(!map1Values.contains(docID)) //We do NOT want duplicates
						map1Values.add(docID);//Add it to the documents of map1
				map1.put(e.getKey(),map1Values);//Replace the Documents of Term in map1 with the merged Documents
			}
			else map1.put(e.getKey(), e.getValue());//If there were no documents in map1 for this Term: Simply put Documents of map2's entry
		}
		return map1;//Return the updated map1 ( merged with map 2 )
	}
}

/* Testing shiat
	ArrayList<Integer> ar1=new ArrayList<>();
	ar1.add(1);
	ArrayList<Integer> ar2=new ArrayList<>();
	ar2.add(2);
	ar2.add(3);
	
	index.put("skata", ar1);
	index.put("melata", ar2);
	
	ArrayList<Integer> ar3=new ArrayList<>();
	ar3.add(3);
	ar3.add(4);
	HashMap<String,ArrayList<Integer>> toMerge=new HashMap<>();
	toMerge.put("melata", ar3);
	
	HashMap<String,ArrayList<Integer>> afterMerge=merge(index,toMerge);
	for (Map.Entry<String, ArrayList<Integer>> e : afterMerge.entrySet())
	{
		System.out.println("Key: "+e.getKey());
		for (Integer integer : e.getValue()) 
		{
			System.out.println("Value: "+integer);
		}
	}
 */
package main;

import java.util.ArrayList;
import java.util.Random;

public class UnorderedArray
{
    private ArrayList<String> a;
    int size;
    
    public void QuickSort(ArrayList<String> a , int left , int right)
    {
        if(left >= right){return;}//Invalid input
        
        int i = left;//Set the cursors
        int j = right;
        String pivot = a.get((int)(left + (right - left) / 2));
        
        while(i<=j)//While you get {all lefts}< pivot <{all rights}
        {
            while(a.get(i).compareTo(pivot) < 0){i++;}// a[i]<pivot //Move --> until pivot
            while(a.get(j).compareTo(pivot) > 0){j--;}// a[j]>pivot //Move <-- until pivot
            if(i<=j)
            {
               String temp = a.get(i);//Swap
               a.set(i, a.get(j));
               a.set(j, temp);
               i++;
               j--;
            }
        }        
        QuickSort(a , left , j );
        QuickSort(a , i , right);
    }
    
    public UnorderedArray(int asize)
    {
        a = new ArrayList<String>();
        size = asize;
        Random r = new Random();
        for(int i=0;i<size;i++){a.add(String.valueOf(r.nextInt(100)));}
    }
    public ArrayList<String> getArray(){return a;}
    public void printArray(){for(int i=0;i<size;i++){System.out.print("|"+a.get(i));}System.out.println("|");}
    
}

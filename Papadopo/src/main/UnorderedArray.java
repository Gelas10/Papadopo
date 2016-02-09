package main;

import java.util.Random;

public class UnorderedArray
{
    private String[] a;
    int size;
    
    public void QuickSort(String[] a , int left , int right)
    {
        if(left >= right){return;}//Invalid input
        
        int i = left;//Set the cursors
        int j = right;
        String pivot = a[(int)(left + (right - left) / 2)];
        
        while(i<=j)//While you get {all lefts}< pivot <{all rights}
        {
            while(a[i].compareTo(pivot) < 0){i++;}// a[i]<pivot //Move --> until pivot
            while(a[j].compareTo(pivot) > 0){j--;}// a[i]>pivot //Move <-- until pivot
            if(i<=j)
            {
               String temp = a[i];//Swap
               a[i] = a[j];
               a[j] = temp;
               i++;
               j--;
            }
        }        
        QuickSort(a , left , j );
        QuickSort(a , i , right);
    }
    
    public UnorderedArray(int asize)
    {
        a = new String[asize];
        size = asize;
        Random r = new Random();
        for(int i=0;i<size;i++){a[i] = String.valueOf(r.nextInt(100));}
    }
    public String[] getArray(){return a;}
    public void printArray(){for(int i=0;i<size;i++){System.out.print("|"+a[i]);}System.out.println("|");}
    
}

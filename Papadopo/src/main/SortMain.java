package main;

public class SortMain 
{
    public static void main(String[] Args)
    {
        int size = 10;
        UnorderedArray a = new UnorderedArray(size);
        
        System.out.println("UNORDERED");a.printArray();
        a.QuickSort(a.getArray(), 0, size-1);
        System.out.println("ORDERED");a.printArray();
    }
}

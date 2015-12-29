package main;

public class MutableInt 
{
	  int value;
	  public MutableInt()
	  {
		  value=1;// note that we start at 1 since we're counting
	  }
	  public MutableInt(int x)
	  {
		  value=x;
	  }
	  public synchronized void increment(){ ++value; }
	  public void incrementBy(int x){ value+=x; }
	  public int get (){ return value; }
}
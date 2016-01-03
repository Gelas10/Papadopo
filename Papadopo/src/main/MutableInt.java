package main;

import java.io.Serializable;

public class MutableInt implements Serializable
{
	  /**
	 * 
	 */
	private static final long serialVersionUID = 1869808761733034817L;
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
package main;

public class Record implements Comparable<Object>
{
	private String term;
	private int document;
	private MutableInt frequency;
	
	public Record(String term,int document,MutableInt frequency)
	{
		this.term=term;
		this.document=document;
		this.frequency=frequency;
	}
	public Record(String tdf)
	{
		String[] tokens=new String[3];
		tokens=tdf.split(",");
		term=tokens[0];
		document=Integer.parseInt(tokens[1]);
		frequency=new MutableInt(Integer.parseInt(tokens[2]));
	}
	public String getTerm()
	{
		return term;
	}
	public int getDocument()
	{
		return document;
	}
	public MutableInt getFrequency()
	{
		return frequency;
	}

	@Override
	public int compareTo(Object arg0) 
	{
		return term.compareTo(((Record)arg0).getTerm());
	}
	
	
}

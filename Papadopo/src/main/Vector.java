package main;

import java.util.Map;

/**A simple representation of a "vector", that is the actual weights it contains and it's norm.*/
public class Vector {

	private Map<String,Double> vector;
	private double norm;
	
	public Vector(Map<String,Double> vector1, double norm1){
		vector = vector1;
		norm = norm1;
	}
	
	public Map<String,Double> getVector(){return vector;}
	public double getNorm(){return norm;}
}

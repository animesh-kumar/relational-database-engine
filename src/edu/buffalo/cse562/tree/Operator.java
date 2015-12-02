package edu.buffalo.cse562.tree;

import java.util.List;

import edu.buffalo.cse562.schema.Tuple;

 abstract class Operator {

	//public static String opCondition;
	
	public String opValue;
	public String opString;
	
	public abstract List<Tuple> Operate();
}

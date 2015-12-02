package edu.buffalo.cse562.tree;

import java.util.List;

import edu.buffalo.cse562.schema.Relation;
import edu.buffalo.cse562.schema.Tuple;

public abstract class OperatorInterface {

	public Object opValue;
	public String opString;
	
	public abstract Relation operate(Relation relation);
}
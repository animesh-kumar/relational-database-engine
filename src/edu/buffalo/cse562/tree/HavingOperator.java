package edu.buffalo.cse562.tree;

import java.util.List;

import edu.buffalo.cse562.schema.Relation;
import edu.buffalo.cse562.schema.Tuple;

public class HavingOperator extends OperatorInterface{
	
	
	public HavingOperator(Object in)
	{
		super.opValue=in;
		super.opString="HavingOperator";
	}


	@Override
	public Relation operate(Relation relation) {
		// TODO Auto-generated method stub
		return null;
	}

}

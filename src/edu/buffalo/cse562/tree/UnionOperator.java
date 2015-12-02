package edu.buffalo.cse562.tree;

import java.util.List;

import edu.buffalo.cse562.schema.Relation;
import edu.buffalo.cse562.schema.Tuple;

public class UnionOperator extends OperatorInterface{

	
	public UnionOperator (Object in)
	{
		super.opValue=in;
		super.opString="UnionOperator";
	}
	@Override
	public Relation operate(Relation relation) {
		// TODO Auto-generated method stub
		return null;
	}

}

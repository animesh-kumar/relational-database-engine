package edu.buffalo.cse562.tree;

import java.util.List;

import net.sf.jsqlparser.expression.BinaryExpression;
import edu.buffalo.cse562.common.JoinUtil;
import edu.buffalo.cse562.schema.Relation;

public class CartesianOperator extends OperatorInterface {

	public CartesianOperator(Object in) {
		super.opValue = in;
		super.opString = "CartesianOperator";
	}

	public Relation operate(Relation relationA, Relation relationB,
			List<BinaryExpression> equiJoins) {
		return JoinUtil.hashJoin(relationA, relationB, equiJoins);
	}

	@Override
	public Relation operate(Relation relation) {
		// TODO Auto-generated method stub
		return null;
	}

}

package edu.buffalo.cse562.tree;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import net.sf.jsqlparser.expression.BooleanValue;
import net.sf.jsqlparser.expression.Expression;
import edu.buffalo.cse562.common.CommonEval;
import edu.buffalo.cse562.common.TupleIterator;
import edu.buffalo.cse562.schema.CustomColumn;
import edu.buffalo.cse562.schema.Relation;
import edu.buffalo.cse562.schema.Tuple;

public class SelectionOperator extends OperatorInterface {

	Relation relation;
	private Expression exp;

	public SelectionOperator(Object in) {
		super.opString = "SelectionOperator";
		super.opValue = in;
	}

	public SelectionOperator(Relation relation, Expression exp) {
		this.relation = relation;
		this.exp = exp;
	}

	public Relation operate(Relation relation) {
		List<Tuple> tuples = relation.getTuples();
		TupleIterator ti = new TupleIterator(tuples);
		List<Tuple> retTuples = new ArrayList<Tuple>();
		CommonEval obj = new CommonEval(relation.getTupleSchema());
		while (ti.hasNext()) {
			Tuple tuple = ti.next();
			obj.setTuple(tuple);
			/*
			 * for(String s : relation.getTupleSchema().keySet()){ CustomColumn
			 * col = relation.getTupleSchema().get(s); System.out.println(); }
			 */

			BooleanValue bool = null;
			try {
				// System.out.println("SelectionOperator Expression :" + exp);
				bool = (BooleanValue) obj.eval(exp);
				// System.out.println(bool);
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			if (("TRUE").equals(bool.toString())) {
				retTuples.add(tuple);
			}
		}
		Relation rel = new Relation();
		rel.setTuples(retTuples);
		rel.setTupleSchema(relation.getTupleSchema());
		rel.setTableName(relation.getTableName());
		rel.setName(relation.getName());
		rel.setJoinRelations(relation.getJoinRelations());
		return rel;
	}

}

package edu.buffalo.cse562.tree;

import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.List;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.LeafValue;
import edu.buffalo.cse562.common.CommonEval;
import edu.buffalo.cse562.schema.Relation;
import edu.buffalo.cse562.schema.Tuple;

public class GroupByOperatorHack {

	Relation relation;
	private List<Expression> groupByColumnReferences;

	public GroupByOperatorHack(Relation relation,
			List<Expression> groupByColumnReferences) {
		this.relation = relation;
		this.groupByColumnReferences = groupByColumnReferences;
		new LinkedHashMap<String, List<Tuple>>();
	}

	public String operate(Tuple tuple, CommonEval obj) throws SQLException {
		obj.setTuple(tuple);
		StringBuilder key = new StringBuilder();
		if (groupByColumnReferences != null) {
			for (Expression expression : groupByColumnReferences) {
				LeafValue leafVal = null;
				leafVal = obj.eval(expression);
				key.append(leafVal);
			}
		}
		return key.toString();
	}

}

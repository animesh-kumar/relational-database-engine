package edu.buffalo.cse562.tree;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.LeafValue;
import edu.buffalo.cse562.common.CommonEval;
import edu.buffalo.cse562.common.TupleIterator;
import edu.buffalo.cse562.schema.CustomColumn;
import edu.buffalo.cse562.schema.Relation;
import edu.buffalo.cse562.schema.Tuple;

public class GroupByOperator extends OperatorInterface {

	Relation relation;
	private List<Expression> groupByColumnReferences;

	public GroupByOperator(Relation relation,
			List<Expression> groupByColumnReferences) {
		this.relation = relation;
		this.groupByColumnReferences = groupByColumnReferences;
	}

	public GroupByOperator(Object in) {
		super.opValue = in;
		super.opString = "GroupByOperator";
	}

	public Relation operate(Relation relation) {
		List<Tuple> tuples = relation.getTuples();
		Map<String, CustomColumn> tupleSchema = relation.getTupleSchema();
		TupleIterator ti = new TupleIterator(tuples);
		// Linked hashmap to preserve sorting
		Map<String, List<Tuple>> retTuples = new LinkedHashMap<String, List<Tuple>>();
		LeafValue[] values = new LeafValue[groupByColumnReferences.size()];
		CommonEval obj = new CommonEval(tupleSchema);
		while (ti.hasNext()) {
			int index = 0;
			Tuple tuple = ti.next();
			obj.setTuple(tuple);
			// If all the group by column references have the same value,
			// it will be grouped in the same group
			// Fetch the value for each of column references
			for (Expression expression : groupByColumnReferences) {
				LeafValue leafVal = null;
				try {
					leafVal = obj.eval(expression);
					values[index++] = leafVal;
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
			// Check if given values type exist of not, if it does, add to same
			// key
			String key = "";
			for (LeafValue leafValue : values) {
				key += leafValue.toString();
			}
			List<Tuple> tupleList = null;
			if (retTuples.containsKey(key)) {
				tupleList = retTuples.get(key);
				tupleList.add(tuple);
				retTuples.put(key, tupleList);
			} else {
				tupleList = new ArrayList<Tuple>();
				tupleList.add(tuple);
				retTuples.put(key, tupleList);
			}
		}
		// Now create the grouping
		// Set the relation as grouped
		relation.setGrouped(true);
		relation.setGroupedListOfTuples(retTuples);
		return relation;
	}

}

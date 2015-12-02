package edu.buffalo.cse562.tree;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import net.sf.jsqlparser.expression.BooleanValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.LeafValue.InvalidLeaf;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.PlainSelect;
import edu.buffalo.cse562.common.CommonEval;
import edu.buffalo.cse562.common.CommonUtil;
import edu.buffalo.cse562.schema.CustomColumn;
import edu.buffalo.cse562.schema.Relation;
import edu.buffalo.cse562.schema.Tuple;

public class EvaluateQueryWithOneRelation {

	private PlainSelect ps;

	public EvaluateQueryWithOneRelation(PlainSelect ps) {
		this.ps = ps;
	}

	public Relation executeQuery(Relation relation) throws IOException,
			SQLException, InvalidLeaf {
		FromItem fromItem = ps.getFromItem();
		if (fromItem instanceof Table) {
			// Convert it to And Expression
			BufferedReader reader = new BufferedReader(new FileReader(
					relation.getDataFile()));
			String line;
			BooleanValue bool = null;
			// Build the new tuple schema
			Map<String, CustomColumn> oldTupleSchema = relation
					.getTupleSchema();
			Map<String, CustomColumn> newTupleSchema = new LinkedHashMap<String, CustomColumn>();
			Map<Integer, String> indexMap = new LinkedHashMap<Integer, String>();
			CustomColumn col;
			int newIndex = 0;
			for (String key : oldTupleSchema.keySet()) {
				col = oldTupleSchema.get(key);
				if (col.isRequired()) {
					indexMap.put(col.getOriginalIndex(), col.getDataType());
					col.setIndex(newIndex++);
					newTupleSchema.put(key, col);
				}
			}

			CommonUtil.setColumnIndexMapForTuple(indexMap);
			CommonEval obj = new CommonEval(relation.getTupleSchema());
			GroupByOperatorHack gbo = new GroupByOperatorHack(relation,
					ps.getGroupByColumnReferences());
			ProjectOperatorHack po = new ProjectOperatorHack(
					relation.getTupleSchema(), ps.getSelectItems());
			Map<String, Tuple> groupedListOfTuples = new LinkedHashMap<String, Tuple>();
			Expression where = ps.getWhere();
			while ((line = reader.readLine()) != null) {
				// System.out.println(line);
				Tuple tuple = new Tuple(line);
				// Change the eval object
				obj.setTuple(tuple);
				bool = (BooleanValue) obj.eval(where);
				if (bool.getValue()) {
					// Passed select and now apply other operators
					String groupName = gbo.operate(tuple, obj);
					tuple = po.operate(groupName, tuple, obj);
					groupedListOfTuples.put(groupName, tuple);
				}

			}
			List<Tuple> newList = new LinkedList<Tuple>();
			newList.addAll(groupedListOfTuples.values());
			relation.setTuples(newList);
			relation.setTupleSchema(ProjectOperatorHack.newTupleSchema);
			OrderByOperator obo = new OrderByOperator(
					ProjectOperatorHack.newTupleSchema, ps.getOrderByElements());
			relation = obo.operate(relation);
			reader.close();
		}
		return relation;
	}

}

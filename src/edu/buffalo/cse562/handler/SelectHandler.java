/*package edu.buffalo.cse562.handler;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.LeafValue;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.OrderByElement;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.SelectBody;
import net.sf.jsqlparser.statement.select.SelectItem;
import net.sf.jsqlparser.statement.select.SubJoin;
import net.sf.jsqlparser.statement.select.SubSelect;
import edu.buffalo.cse562.common.CommonUtil;
import edu.buffalo.cse562.operator.CartesianOperator;
import edu.buffalo.cse562.operator.GroupByOperator;
import edu.buffalo.cse562.operator.OrderByOperator;
import edu.buffalo.cse562.operator.ProjectionOperator;
import edu.buffalo.cse562.operator.SelectionOperator;
import edu.buffalo.cse562.schema.CustomColumn;
import edu.buffalo.cse562.schema.Relation;
import edu.buffalo.cse562.schema.Tuple;

public class SelectHandler {

	public static List<Tuple> handlePlainSelect(Map<String, Relation> tables,
			Map<String, CustomColumn> tupleSchema, SelectBody body) {
		PlainSelect ps = (PlainSelect) body;
		FromItem fromItem = ps.getFromItem();
		Relation relation = null;
		if (fromItem instanceof SubSelect) {
			// Evaluate the statement as select
			SelectBody subSelectBody = ((SubSelect) fromItem).getSelectBody();
			if (subSelectBody instanceof PlainSelect) {
				List<Tuple> tuples = handlePlainSelect(tables, tupleSchema,
						subSelectBody);
				relation = new Relation();
				relation.setTableName(subSelectBody.toString());
				relation.setTuples(tuples);
			}
		} else if (fromItem instanceof SubJoin) {

		} else {
			// System.out.println("fromItem handlePlainSelect:: "+fromItem.toString());
			relation = tables.get(fromItem.toString());
			
			 * if(relation==null){ relation =
			 * tables.get(fromItem.toString().toUpperCase()); }
			 
			// System.out.println("Relation from map handlePlainSelect:: "+relation);
			relation.setTuples(CommonUtil.readTuplesFromFile(relation
					.getDataFile()));
		}
		tables.put(relation.getTableName(), relation);
		List<Tuple> tuples = new ArrayList<Tuple>();
		if (ps.getJoins() != null) {
			for (Iterator joinsIt = ps.getJoins().iterator(); joinsIt.hasNext();) {
				Join join = (Join) joinsIt.next();
				relation = tables.get(join.getRightItem().toString());
				relation.setTuples(CommonUtil.readTuplesFromFile(relation
						.getDataFile()));
				tables.put(relation.getTableName(), relation);
			}
			*//**
			 * Perform the Cartesian product only if joins are present
			 *//*

			Set<String> tableSet = tables.keySet();
			// if(tableSet.size()>1){
			CartesianOperator cartOp = new CartesianOperator();
			for (Object table : tableSet) {
				tuples = cartOp.operate(tuples, tables.get(table.toString())
						.getTuples());
			}
		} else {
			// if no joins, add the tuples from from item to tuples
			tuples = relation.getTuples();
		}

		// }

		*//**
		 * Evaluate selection
		 *//*
		Expression whereExp = ps.getWhere();
		if (whereExp != null) {
			SelectionOperator so = new SelectionOperator(tupleSchema, whereExp);
			tuples = so.operate(tuples);
		}
		
		
		 * Evaluate ORDER BY in the end
		 
		List<OrderByElement> orderByElements = ps.getOrderByElements();
		if (orderByElements != null && orderByElements.size() > 0) {
			OrderByOperator obo = new OrderByOperator(tupleSchema,
					orderByElements);
			tuples = obo.operate(tuples);
		}
}
		*//**
		 * Evaluate group by
		 *//*

		List<Expression> groupByColumnReferences = ps
				.getGroupByColumnReferences();
		boolean groupBy = false;
		HashMap<String, List<Tuple>> groupedTupleList = null;
		if (groupByColumnReferences != null
				&& groupByColumnReferences.size() > 0) {
			groupBy = true;
			GroupByOperator gb = new GroupByOperator(tupleSchema,
					groupByColumnReferences);
			groupedTupleList = gb.operate(tuples);
		}

		*//**
		 * Evaluate Projection
		 *//*
		// if group by is true, evaluate projection for each tuple list and then
		// concatenate the results
		// Fetch the list of project items first
		List<SelectItem> projectionItems = ps.getSelectItems();
		if (groupBy) {
			// Empty the tuples
			tuples.removeAll(tuples);
			for (List<Tuple> tupleList : groupedTupleList.values()) {
				tuples.addAll(evaluateProjections(tupleSchema, projectionItems,
						tupleList));
			}
		} else {
			tuples = evaluateProjections(tupleSchema, projectionItems, tuples);
		}

		return tuples;
		// System.out.println("Printed " + count + " rows");
	}

	private static List<Tuple> evaluateProjections(
			Map<String, CustomColumn> tupleSchema,
			List<SelectItem> projectionItems, List<Tuple> tuples) {
		ProjectionOperator po = new ProjectionOperator(tupleSchema,
				projectionItems);
		tuples = po.operate(tuples);
		return tuples;
	}

}
*/
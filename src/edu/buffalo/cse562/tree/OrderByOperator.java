package edu.buffalo.cse562.tree;

import java.sql.Date;
import java.sql.SQLException;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

import net.sf.jsqlparser.expression.DateValue;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.LeafValue;
import net.sf.jsqlparser.expression.LeafValue.InvalidLeaf;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.statement.select.OrderByElement;
import edu.buffalo.cse562.common.CommonEval;
import edu.buffalo.cse562.schema.CustomColumn;
import edu.buffalo.cse562.schema.Relation;
import edu.buffalo.cse562.schema.Tuple;

public class OrderByOperator extends OperatorInterface {

	Map<String, CustomColumn> tupleSchema;
	private List<OrderByElement> orderByElements;

	public OrderByOperator(Map<String, CustomColumn> tupleSchema,
			List<OrderByElement> orderByElements) {
		this.tupleSchema = tupleSchema;
		this.orderByElements = orderByElements;
	}

	public OrderByOperator(Object in) {
		super.opValue = in;
		super.opString = "OrderByOperator";
	}

	public Relation operate(Relation relation) {
		tupleSchema = relation.getTupleSchema();
		List<Tuple> tuples = relation.getTuples();
		Collections.sort(tuples, new ChainedTupleComparator());
		relation.setTuples(tuples);
		return relation;

	}

	public class ChainedTupleComparator implements Comparator<Tuple> {

		@Override
		public int compare(Tuple tuple1, Tuple tuple2) {
			CommonEval obj1 = new CommonEval(tupleSchema);
			obj1.setTuple(tuple1);
			CommonEval obj2 = new CommonEval(tupleSchema);
			obj2.setTuple(tuple2);
			for (OrderByElement orderByElement : orderByElements) {
				boolean asc = orderByElement.isAsc();
				Expression expression = orderByElement.getExpression();

				LeafValue leafVal1 = null, leafVal2 = null;
				try {
					if (asc) {
						// Second should be greater than first
						leafVal1 = obj1.eval(expression);
						leafVal2 = obj2.eval(expression);
					} else {
						leafVal1 = obj2.eval(expression);
						leafVal2 = obj1.eval(expression);
					}

					if (leafVal1 instanceof LongValue
							&& leafVal2 instanceof LongValue) {
						long diff = leafVal1.toLong() - leafVal2.toLong();
						if (diff == 0) {
							continue;
						} else if (diff > 0) {
							return 1;
						} else {
							return -1;
						}
					} else if (leafVal1 instanceof DoubleValue
							&& leafVal2 instanceof DoubleValue) {
						double diff = leafVal1.toDouble() - leafVal2.toDouble();
						if (diff == 0) {
							continue;
						} else if (diff > 0) {
							return 1;
						} else {
							return -1;
						}
					} else if (leafVal1 instanceof DateValue
							&& leafVal2 instanceof DateValue) {
						Date date1 = ((DateValue) leafVal1).getValue();
						Date date2 = ((DateValue) leafVal2).getValue();
						int diff = date1.compareTo(date2);
						if (diff == 0) {
							continue;
						} else if (diff > 0) {
							return 1;
						} else {
							return -1;
						}
					} else {
						String str1 = ((StringValue) leafVal1)
								.getNotExcapedValue();
						String str2 = ((StringValue) leafVal2)
								.getNotExcapedValue();
						int diff = str1.compareTo(str2);
						if (diff == 0) {
							continue;
						} else if (diff > 0) {
							return 1;
						} else {
							return -1;
						}
					}
					// Else it's a string

				} catch (SQLException e) {
					e.printStackTrace();
				} catch (InvalidLeaf e) {
					e.printStackTrace();
				}

			}
			return 0;
		}
	}
}

package edu.buffalo.cse562.tree;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import net.sf.jsqlparser.expression.CaseExpression;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.LeafValue;
import net.sf.jsqlparser.expression.LeafValue.InvalidLeaf;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.operators.arithmetic.Multiplication;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;
import edu.buffalo.cse562.common.CommonEval;
import edu.buffalo.cse562.common.TupleIterator;
import edu.buffalo.cse562.schema.CustomColumn;
import edu.buffalo.cse562.schema.Relation;
import edu.buffalo.cse562.schema.Tuple;

public class ProjectOperator extends OperatorInterface {

	/*
	 * Map<String, CustomColumn> tupleSchema; private List<SelectItem>
	 * projectionItems; public double sum = 0; public double sumForAvg = 0;
	 * public double countForAvg = 0; public double avg = 0; public int count =
	 * 0; public double max = Double.MIN_VALUE; public double min =
	 * Double.MAX_VALUE;
	 */

	Map<String, CustomColumn> tupleSchema;
	private Relation relation;
	private List<SelectItem> projectionItems;
	private HashMap<String, Double> sum = new HashMap<String, Double>();
	private HashMap<String, Double> sumForAvg = new HashMap<String, Double>();
	private HashMap<String, Integer> countForAvg = new HashMap<String, Integer>();
	private HashMap<String, Integer> count = new HashMap<String, Integer>();
	private HashMap<String, Double> max = new HashMap<String, Double>();
	private HashMap<String, Double> min = new HashMap<String, Double>();

	public ProjectOperator(Object in) {
		super.opValue = in;
		super.opString = "ProjectOperator";
	}

	public ProjectOperator(Map<String, CustomColumn> tupleSchema,
			List<SelectItem> projectItems) {
		this.tupleSchema = tupleSchema;
		this.projectionItems = projectItems;

	}

	public Relation operate(Relation relation) {
		// this.relation = relation;
		this.relation = relation;
		tupleSchema = relation.getTupleSchema();
		List<Tuple> tuples = relation.getTuples();
		if (relation.isGrouped()) {
			ArrayList<Tuple> newTupleList = new ArrayList<Tuple>();
			// Now run projection for each list of tuples and keep adding to the
			// tuple list
			for (List<Tuple> tupleList : relation.getGroupedListOfTuples()
					.values()) {
				sum = new HashMap<String, Double>();
				sumForAvg = new HashMap<String, Double>();
				countForAvg = new HashMap<String, Integer>();
				count = new HashMap<String, Integer>();
				max = new HashMap<String, Double>();
				min = new HashMap<String, Double>();
				List<Tuple> projectedList = operate(tupleList);
				newTupleList.addAll(projectedList);
			}
			relation.setTuples(newTupleList);

		} else {
			relation.setTuples(operate(tuples));
		}
		return relation;
	}

	private List<Tuple> operate(List<Tuple> tuples) {
		TupleIterator ti = new TupleIterator(tuples);
		List<Tuple> retTuples = new ArrayList<Tuple>();
		// Since projection will change the schema, update the tuple schema of
		// the new relation
		LinkedHashMap<String, CustomColumn> newTupleSchema = new LinkedHashMap<String, CustomColumn>();
		int counter = 0;
		LeafValue[] values = new LeafValue[projectionItems.size()];
		CommonEval obj = new CommonEval(tupleSchema);
		while (ti.hasNext()) {
			Tuple tuple = ti.next();
			obj.setTuple(tuple);
			// List<LeafValue> values = new ArrayList<LeafValue>();
			int index = 0;
			String newColumnName = "";
			for (SelectItem selectItem : projectionItems) {
				boolean isCalculatedValue = false;
				if (selectItem instanceof SelectExpressionItem) {
					SelectExpressionItem selectExp = (SelectExpressionItem) selectItem;
					String selectItemStr = selectItem.toString();
					Expression expression = selectExp.getExpression();
					String alias = selectExp.getAlias();
					if (alias != null && !alias.isEmpty()) {
						newColumnName = alias;
					}
					// Expression can be a function like SUM, AGG, etc or a
					// Column or a Sub Select
					if (expression instanceof Function) {
						Function function = (Function) expression;
						String functionName = function.getName();
						List<Expression> parameters = null;
						isCalculatedValue = true;
						// In case of all colums
						if (function.isAllColumns()) {
							// Assuming all columns function will invoke only
							// Count(*)
							switch (functionName) {
							case "COUNT":
								Integer countVal = count.get(selectItemStr);
								if (countVal == null) {
									countVal = 1;
									count.put(selectItemStr, countVal);
								} else {
									count.put(selectItemStr, ++countVal);
								}
								values[index] = new LongValue(countVal);
								break;
							}
							// Since we are working on aggregate
							// functions, make sure you don't
							// have more than one tuple
							if (retTuples.size() > 0) {
								retTuples.remove(0);
							}

						} else {
							// Get the parameters required for the expression
							// say SUM (A+B) will have A, B as params
							parameters = function.getParameters()
									.getExpressions();

							for (Expression parameter : parameters) {
								if (parameter instanceof Multiplication) {
									try {
										LeafValue val = obj.eval(parameter);
										values = getValue(values,
												selectItemStr, functionName,
												val, index);

									} catch (SQLException e) {
										// TODO Auto-generated catch block
										e.printStackTrace();
									} catch (InvalidLeaf e) {
										// TODO Auto-generated catch block
										e.printStackTrace();
									}
									// Since we are working on aggregate
									// functions, make sure you don't
									// have more than one tuple
									if (retTuples.size() > 0) {
										retTuples.remove(0);
									}
								} else if (parameter instanceof Column) {
									String columnName = ((Column) parameter)
											.getWholeColumnName();
									CustomColumn col = tupleSchema
											.get(columnName);
									newColumnName = columnName;
									try {
										LeafValue val = obj.eval(col);
										values = getValue(values,
												selectItemStr, functionName,
												val, index);

									} catch (InvalidLeaf e) {
										// TODO Auto-generated catch block
										e.printStackTrace();
									} catch (SQLException e) {
										e.printStackTrace();
									}
									// Since we are working on aggregate
									// functions, make sure you don't
									// have more than one tuple
									if (retTuples.size() > 0) {
										retTuples.remove(0);
									}
								} else if (parameter instanceof CaseExpression) {
									try {
										LeafValue val = obj.eval(parameter);
										values = getValue(values,
												selectItemStr, functionName,
												val, index);

									} catch (SQLException e) {
										// TODO Auto-generated catch block
										e.printStackTrace();
									} catch (InvalidLeaf e) {
										// TODO Auto-generated catch block
										e.printStackTrace();
									}
									// Since we are working on aggregate
									// functions, make sure you don't
									// have more than one tuple
									if (retTuples.size() > 0) {
										retTuples.remove(0);
									}
								}
							}
						}
					} else if (expression instanceof Column) {
						Column column = (Column) expression;
						String colName = column.getWholeColumnName();
						newColumnName = colName;
						CustomColumn col = tupleSchema.get(colName);
						try {
							LeafValue val = obj.eval(col);
							values[index] = val;
						} catch (SQLException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}

					} else {

						if (newColumnName == null || newColumnName.isEmpty()) {
							newColumnName = expression.toString();
						}
						try {
							LeafValue result = obj.eval(expression);
							values[index] = result;
							// Fix alias
							CustomColumn col = new CustomColumn();
							col.setIndex(index);
							col.setColumnName(alias);
							col.setIndex(index);
							if (result instanceof LongValue) {
								col.setDataType("int");
							} else if (result instanceof DoubleValue) {
								col.setDataType("decimal");
							}
							tupleSchema.put(alias, col);
						} catch (SQLException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					}
				}
				if (counter == 0) {
					CustomColumn column = new CustomColumn();
					column.setColumnName(newColumnName);
					CustomColumn oldColumn = tupleSchema.get(newColumnName);
					if (oldColumn != null) {
						column.setDataType(oldColumn.getDataType());
						column.setTable(oldColumn.getTable());
					} else if (isCalculatedValue) {
						column.setDataType("double");
					} else {
						column.setDataType("string");
					}
					column.setIndex(index);
					newTupleSchema.put(newColumnName, column);
				}
				index++;
			}
			tuple.setValues(values);
			retTuples.add(tuple);
			if (counter == 0) {
				relation.setTupleSchema(newTupleSchema);
			}
			counter++;
		}
		return retTuples;
	}

	private LeafValue[] getValue(LeafValue[] values, String selectItemStr,
			String functionName, LeafValue val, int index) throws InvalidLeaf {
		switch (functionName) {
		case "SUM":
			Double sumVal = sum.get(selectItemStr);
			if (sumVal == null) {
				sumVal = val.toDouble();
				sum.put(selectItemStr, sumVal);
			} else {
				sumVal += val.toDouble();
				sum.put(selectItemStr, sumVal);
			}
			values[index] = new DoubleValue(sumVal);
			break;
		case "AVG":
			Double sumForAvgVal = sumForAvg.get(selectItemStr);
			if (sumForAvgVal == null) {
				sumForAvgVal = val.toDouble();
				sumForAvg.put(selectItemStr, sumForAvgVal);
			} else {
				sumForAvgVal += val.toDouble();
				sumForAvg.put(selectItemStr, sumForAvgVal);
			}
			Integer countForAvgVal = countForAvg.get(selectItemStr);
			if (countForAvgVal == null) {
				countForAvgVal = 1;
				countForAvg.put(selectItemStr, countForAvgVal);
			} else {
				countForAvg.put(selectItemStr, ++countForAvgVal);
			}
			values[index] = new DoubleValue(sumForAvgVal / countForAvgVal);
			break;
		case "COUNT":
			Integer countVal = count.get(selectItemStr);
			if (countVal == null) {
				countVal = 1;
				count.put(selectItemStr, countVal);
			} else {
				count.put(selectItemStr, ++countVal);
			}
			values[index] = new LongValue(countVal);
			break;
		case "MIN":
			Double minVal = min.get(selectItemStr);
			if (minVal == null) {
				minVal = val.toDouble();
				min.put(selectItemStr, minVal);
			} else {
				if (val.toDouble() < minVal) {
					minVal = val.toDouble();
				}
				min.put(selectItemStr, minVal);
			}
			values[index] = new DoubleValue(minVal);
			break;
		case "MAX":
			Double maxVal = max.get(selectItemStr);
			if (maxVal == null) {
				maxVal = val.toDouble();
				max.put(selectItemStr, maxVal);
			} else {
				if (val.toDouble() > maxVal) {
					maxVal = val.toDouble();
				}
				max.put(selectItemStr, maxVal);
			}
			values[index] = new DoubleValue(maxVal);
			break;
		}
		return values;
	}

}

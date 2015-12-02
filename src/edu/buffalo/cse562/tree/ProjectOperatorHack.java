package edu.buffalo.cse562.tree;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.LeafValue;
import net.sf.jsqlparser.expression.LeafValue.InvalidLeaf;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;
import edu.buffalo.cse562.common.CommonEval;
import edu.buffalo.cse562.schema.CustomColumn;
import edu.buffalo.cse562.schema.Tuple;

public class ProjectOperatorHack {

	Map<String, CustomColumn> tupleSchema;
	private List<SelectItem> projectionItems;
	private HashMap<String, HashMap<String, Double[]>> map = new HashMap<String, HashMap<String, Double[]>>();
	private static final int INDEX_FOR_SUM = 0;
	private static final int INDEX_FOR_COUNT = 1;
	private static final int INDEX_FOR_MAX = 2;
	private static final int INDEX_FOR_MIN = 3;
	private static final int INDEX_FOR_SUM_AVG = 4;
	private static final int INDEX_FOR_COUNT_AVG = 5;
	public static LinkedHashMap<String, CustomColumn> newTupleSchema = new LinkedHashMap<String, CustomColumn>();
	private String alias;
	private Expression[] expressions;
	private String[] expressionNames;
	private int[] functionType;
	private int[] expressionType;

	public ProjectOperatorHack(Map<String, CustomColumn> tupleSchema,
			List<SelectItem> projectItems) {
		this.tupleSchema = tupleSchema;
		this.projectionItems = projectItems;
		preProcessProjectItems();
	}

	private Expression[] preProcessProjectItems() {
		int index = 0;
		String newColumnName = "";
		expressions = new Expression[projectionItems.size()];
		expressionNames = new String[projectionItems.size()];
		functionType = new int[projectionItems.size()];
		expressionType = new int[projectionItems.size()];
		for (SelectItem selectItem : projectionItems) {
			boolean isCalculatedValue = false;
			if (selectItem instanceof SelectExpressionItem) {
				SelectExpressionItem selectExp = (SelectExpressionItem) selectItem;
				Expression expression = selectExp.getExpression();
				alias = selectExp.getAlias();
				if (alias != null && !alias.isEmpty()) {
					newColumnName = alias;
				}
				// Expression can be a function like SUM, AGG, etc or a
				// Column or a Sub Select
				if (expression instanceof Function) {
					Function function = (Function) expression;
					expressionType[index] = 0;
					String functionName = function.getName();
					if (functionName.equals("SUM")) {
						functionType[index] = 0;
					} else if (functionName.equals("COUNT")) {
						functionType[index] = 1;
					} else if (functionName.equals("AVG")) {
						functionType[index] = 2;
					} else if (functionName.equals("MAX")) {
						functionType[index] = 3;
					} else if (functionName.equals("MIN")) {
						functionType[index] = 4;
					}
					List<Expression> parameters = null;
					isCalculatedValue = true;
					// In case of all colums
					if (function.isAllColumns()) {
						// Count(*)
						expressions[index] = expression;

					} else {
						// Get the parameters required for the expression
						// say SUM (A+B) will have A, B as params
						parameters = function.getParameters().getExpressions();

						for (Expression parameter : parameters) {
							expressions[index] = parameter;
						}
					}
				} else if (expression instanceof Column) {
					Column column = (Column) expression;
					newColumnName = column.getWholeColumnName();
					expressions[index] = expression;
					expressionType[index] = 1;
					functionType[index] = -1;
				}

			}
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
			expressionNames[index] = newColumnName;
			newTupleSchema.put(newColumnName, column);
			index++;
		}
		return expressions;
	}

	public Tuple operate(String groupName, Tuple tuple, CommonEval obj)
			throws SQLException, InvalidLeaf {
		LeafValue[] values = new LeafValue[projectionItems.size()];
		HashMap<String, Double[]> valuesForGroup = map.get(groupName);
		if (valuesForGroup == null) {
			valuesForGroup = new HashMap<String, Double[]>();
		}
		for (int i = 0; i < expressions.length; i++) {
			String selectItemStr = expressionNames[i];
			Expression expression = expressions[i];
			Double[] aggregatedValues = valuesForGroup.get(selectItemStr);
			if (aggregatedValues == null) {
				aggregatedValues = createAggregatedValues(selectItemStr);
			}
			if (expression instanceof Function
					&& ((Function) expression).getName().equals("COUNT")) {
				int count = aggregatedValues[INDEX_FOR_COUNT].intValue();
				count++;
				values[i] = new LongValue(String.valueOf(count));
				aggregatedValues[INDEX_FOR_COUNT] = (double) count;
				valuesForGroup.put(selectItemStr, aggregatedValues);
			} else if (expressionType[i] == 1 && expression instanceof Column) {
				LeafValue val = obj.eval(expression);
				values[i] = val;
			} else {
				LeafValue val = obj.eval(expression);
				Double calculated = 0.0;
				switch (functionType[i]) {
				case 0:
					calculated = aggregatedValues[INDEX_FOR_SUM] += val
							.toDouble();
					break;
				case 1:
					calculated = aggregatedValues[INDEX_FOR_COUNT] += 1;
					break;
				case 2:
					aggregatedValues[INDEX_FOR_SUM_AVG] += val.toDouble();
					aggregatedValues[INDEX_FOR_COUNT_AVG] += 1;
					calculated = aggregatedValues[INDEX_FOR_SUM_AVG]
							/ aggregatedValues[INDEX_FOR_COUNT_AVG];
					break;
				case 3:
					if (val.toDouble() < aggregatedValues[INDEX_FOR_MIN]) {
						calculated = aggregatedValues[INDEX_FOR_MIN] = val
								.toDouble();
					}
					break;
				case 4:
					if (val.toDouble() > aggregatedValues[INDEX_FOR_MAX]) {
						calculated = aggregatedValues[INDEX_FOR_MAX] = val
								.toDouble();
					}
					break;
				}
				values[i] = new DoubleValue(calculated);
			}
			valuesForGroup.put(selectItemStr, aggregatedValues);
			map.put(groupName, valuesForGroup);
		}
		tuple.setValues(values);
		return tuple;
	}

	private Double[] createAggregatedValues(String newColumnName) {
		Double[] aggregatesForGroup = new Double[6];
		Arrays.fill(aggregatesForGroup, 0.0);
		return aggregatesForGroup;
	}
}

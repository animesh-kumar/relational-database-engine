package edu.buffalo.cse562.common;

import java.util.List;
import java.util.Map;

import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.CaseExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.Parenthesis;
import net.sf.jsqlparser.expression.WhenClause;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.OrderByElement;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.SelectBody;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import edu.buffalo.cse562.schema.CustomColumn;
import edu.buffalo.cse562.schema.Relation;

public class ColumnUtility {

	private static Map<String, String> columnRelationMap;

	public void setRequiredColumns(Map<String, Relation> relations,
			SelectBody body) {
		PlainSelect ps = (PlainSelect) body;
		columnRelationMap = CommonUtil.getColumnRelationMap();
		// First add all the select items
		for (Object object : ps.getSelectItems()) {
			if (object instanceof SelectExpressionItem) {
				setRequiredStatusForExpression(
						((SelectExpressionItem) object).getExpression(),
						relations);
			}
		}
		// Add all where Items
		Expression where = ps.getWhere();
		setRequiredStatusForExpression(where, relations);
		// Add all group by items
		if (ps.getGroupByColumnReferences() != null) {
			for (Object object : ps.getGroupByColumnReferences()) {
				setRequiredStatusForExpression(((Expression) object), relations);
			}
		}
		// Add all order by items
		if (ps.getOrderByElements() != null) {
			for (Object object : ps.getOrderByElements()) {
				setRequiredStatusForExpression(
						((OrderByElement) object).getExpression(), relations);
			}
		}
	}

	private void setRequiredStatusForExpression(Expression expression,
			Map<String, Relation> relations) {
		if (expression instanceof Column) {
			// Fetch the corresponding CUstom Column
			CustomColumn column = getCustomColumnFromRelationSchema(
					(Column) expression, relations);
			if (column != null) {
				column.setRequired(true);
			}
		} else if (expression instanceof BinaryExpression) {
			setRequiredStatusForExpression(
					((BinaryExpression) expression).getLeftExpression(),
					relations);
			setRequiredStatusForExpression(
					((BinaryExpression) expression).getRightExpression(),
					relations);
		} else if (expression instanceof CaseExpression) {
			List<WhenClause> whenClauses = ((CaseExpression) expression)
					.getWhenClauses();
			for (WhenClause whenClause : whenClauses) {
				Expression whenExpression = whenClause.getWhenExpression();
				setRequiredStatusForExpression(whenExpression, relations);
			}
		} else if (expression instanceof Function) {
			ExpressionList parameters = ((Function) expression).getParameters();
			if (parameters != null) {
				List<Expression> expressions = parameters.getExpressions();
				for (Expression exp : expressions) {
					setRequiredStatusForExpression(exp, relations);
				}
			}
		} else if (expression instanceof Parenthesis) {
			Parenthesis paranthesis = (Parenthesis) expression;
			setRequiredStatusForExpression(paranthesis.getExpression(),
					relations);
		}
	}

	public CustomColumn getCustomColumnFromRelationSchema(Column column,
			Map<String, Relation> relations) {
		String columnName = column.getWholeColumnName();
		String tableName = columnRelationMap.get(columnName);
		if (tableName != null) {
			Relation relation = relations.get(tableName);
			Map<String, CustomColumn> tupleSchema = relation.getTupleSchema();
			for (String colName : tupleSchema.keySet()) {
				if (colName.equalsIgnoreCase(columnName)) {
					return tupleSchema.get(colName);
				}
			}
		}
		return null;
	}
}

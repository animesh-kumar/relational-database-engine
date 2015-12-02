package edu.buffalo.cse562.common;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Parenthesis;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.SelectBody;
import edu.buffalo.cse562.schema.Relation;

public class WhereUtility {

	HashMap<String, List<Expression>> whereList = new HashMap<String, List<Expression>>();
	Set<Expression> equiJoins = new HashSet<Expression>();
	private Map<String, String> columnRelationMap;

	public HashMap<String, List<Expression>> getWhereList() {
		return whereList;
	}

	public Set<Expression> getEquiJoins() {
		return equiJoins;
	}

	public WhereUtility(SelectBody body, Map<String, Relation> tables) {
		if (body instanceof PlainSelect) {
			PlainSelect ps = (PlainSelect) body;
			columnRelationMap = CommonUtil.getColumnRelationMap();
			Expression whereExp = ps.getWhere();
			populateWhereListAndJoins(whereExp, tables);
		}
	}

	private void populateWhereListAndJoins(Expression whereExp,
			Map<String, Relation> tables) {
		if (whereExp != null && whereExp instanceof BinaryExpression) {
			BinaryExpression be = (BinaryExpression) whereExp;
			if (be instanceof AndExpression) {
				populateWhereListAndJoins(be.getLeftExpression(), tables);
				populateWhereListAndJoins(be.getRightExpression(), tables);
			} else if ((be.getLeftExpression() instanceof Column)
					&& !(be.getRightExpression() instanceof Column)) {
				Column column = (Column) be.getLeftExpression();
				addWhereExpression(be, column, tables);
			} else if (!(be.getLeftExpression() instanceof Column)
					&& (be.getRightExpression() instanceof Column)) {
				Column column = (Column) be.getRightExpression();
				addWhereExpression(be, column, tables);
			} else if ((be.getLeftExpression() instanceof Column)
					&& (be.getRightExpression() instanceof Column)
					&& be instanceof EqualsTo) {
				equiJoins.add(be);
			} else if ((be.getLeftExpression() instanceof Column)
					&& (be.getRightExpression() instanceof Column)
					&& !(be instanceof EqualsTo)) {
				Column column = (Column) be.getRightExpression();
				addWhereExpression(be, column, tables);
			}
		} else if (whereExp != null && whereExp instanceof Parenthesis) {
			Parenthesis pa = (Parenthesis) whereExp;
			Expression expression = pa.getExpression();
			if (expression instanceof OrExpression) {
				OrExpression or = (OrExpression) expression;
				Expression leftExpression = or.getLeftExpression();
				if (leftExpression instanceof BinaryExpression) {
					BinaryExpression left = (BinaryExpression) leftExpression;
					Column column = (Column) left.getLeftExpression();
					addWhereExpression(pa, column, tables);
				}
			} else {
				populateWhereListAndJoins(expression, tables);
			}
		}
	}

	private void addWhereExpression(Expression expression, Column column,
			Map<String, Relation> tables) {
		String tableName = columnRelationMap.get(column.getColumnName());
		if (tableName != null) {
			List<Expression> list = whereList.get(tableName);
			if (list == null) {
				list = new ArrayList<Expression>();
				list.add(expression);
			} else {
				list.add(expression);
			}
			whereList.put(tableName, list);
		}
	}

}

package edu.buffalo.cse562.handler;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
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
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.OrderByElement;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.SelectBody;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SubSelect;
import edu.buffalo.cse562.common.CommonUtil;
import edu.buffalo.cse562.schema.Relation;
import edu.buffalo.cse562.tree.CartesianOperator;
import edu.buffalo.cse562.tree.GroupByOperator;
import edu.buffalo.cse562.tree.JoinOperator;
import edu.buffalo.cse562.tree.OperatorInterface;
import edu.buffalo.cse562.tree.OrderByOperator;
import edu.buffalo.cse562.tree.ProjectOperator;
import edu.buffalo.cse562.tree.QueryTree;
import edu.buffalo.cse562.tree.RelationOperator;

public class GenerateTree {

	private LinkedList<Object> fromList;
	private QueryTree tree;
	private Map<String, String> columnRelationMap;

	public GenerateTree() {
		fromList = new LinkedList<Object>();
		tree = new QueryTree();
		columnRelationMap = CommonUtil.getColumnRelationMap();
	}

	public QueryTree generateTree(SelectBody body, Map<String, Relation> tables) {

		if (body instanceof PlainSelect) {
			PlainSelect ps = (PlainSelect) body;
			updateStatement(ps);
			if (ps.getJoins() == null) {
				return null;
			}
			List<Join> joinItem = new ArrayList<Join>();
			QueryTree qt = null;
			fromList.add(ps.getFromItem());
			Join join = null;

			if (ps.getJoins() != null) {
				for (Iterator joinsIt = ps.getJoins().iterator(); joinsIt
						.hasNext();) {
					join = (Join) joinsIt.next();
					if (join.getOnExpression() == null)
						fromList.add(join.getRightItem().toString());

					else {
						joinItem.add(join);
					}
				}

			}

			List orderByElements = ps.getOrderByElements();
			if (orderByElements != null) {
				OperatorInterface oB = new OrderByOperator(orderByElements);
				tree.insert(oB);
			}

			List selectItems = ps.getSelectItems();
			if (selectItems != null) {
				OperatorInterface p = new ProjectOperator(selectItems);
				tree.insert(p);
			}

			List groupByColumnReferences = ps.getGroupByColumnReferences();
			if (groupByColumnReferences != null) {
				OperatorInterface gb = new GroupByOperator(
						groupByColumnReferences);
				tree.insert(gb);
			}

			// Expression where = ps.getWhere();
			// if (where != null) {
			// OperatorInterface s = new SelectionOperator(where);
			// tree.insert(s);
			//
			// }

			if (joinItem.size() == 0) {
				// TODO GENERIC JOIN

				while (fromList.size() > 0) {
					if (fromList.size() == 1) {
						Object peek = fromList.peek();
						if (peek instanceof SubSelect) {
							SubSelect ss = (SubSelect) peek;
							// Pop from the from list, since new tree will be
							// generated
							fromList.poll();
							qt = generateTree(ss.getSelectBody(), tables);
						}

						else {
							OperatorInterface c = new RelationOperator(peek);
							CommonUtil.setRelationOnMemory(peek.toString()
									.toUpperCase());

							tree.insert(c);
						}

						fromList.poll();
						break;

					}

					else {
						Collections.reverse(fromList);

						for (int i = 0; i < fromList.size() - 1; i++) {
							OperatorInterface c = new CartesianOperator(
									"CartesianOperator" + i + "");
							tree.insert(c);
						}

						Iterator<Object> itr = fromList.iterator();
						while (itr.hasNext()) {

							Object relation = itr.next();
							OperatorInterface c = new RelationOperator(relation);
							if (!itr.hasNext()) {
								CommonUtil.setRelationOnMemory(relation
										.toString().toUpperCase());
							}
							tree.insert(c);
						}
						break;
					}

				}

			} else {
				OperatorInterface j = new JoinOperator(joinItem.get(0));
				tree.insert(j);

				OperatorInterface f = new RelationOperator(fromList.get(0));
				tree.insert(f);

				f = new RelationOperator(joinItem.get(0).getRightItem()
						.toString());
				tree.insert(f);

			}

		}

		return tree;
	}

	public void updateStatement(Object object) {
		if (object instanceof PlainSelect) {
			PlainSelect ps = (PlainSelect) object;
			FromItem fromItem = ps.getFromItem();
			updateStatement(fromItem);
			List joins = ps.getJoins();
			updateStatement(joins);
			List selectItems = ps.getSelectItems();
			updateStatement(selectItems);
			Expression where = ps.getWhere();
			updateStatement(where);
			List groupByColumnReferences = ps.getGroupByColumnReferences();
			updateStatement(groupByColumnReferences);
			List orderByElements = ps.getOrderByElements();
			updateStatement(orderByElements);
		} else if (object instanceof Column) {
			Column column = (Column) object;
			Table table = column.getTable();
			String columnName = column.getColumnName().toUpperCase();
			if (table != null && table.getName() != null) {
				table.setName(table.getName().toUpperCase());
			} else {
				String tableName = columnRelationMap.get(columnName);
				if (tableName != null) {
					table = new Table();
					table.setName(tableName.toUpperCase());
					column.setTable(table);
				}
			}
			column.setColumnName(columnName);
		} else if (object instanceof Table) {
			Table table = (Table) object;
			table.setName(table.getName().toUpperCase());
		} else if (object instanceof Relation) {
			Relation table = (Relation) object;
			table.setName(table.getName().toUpperCase());
		} else if (object instanceof Join) {
			Join join = (Join) object;
			updateStatement(join.getRightItem());
		} else if (object instanceof List) {
			List list = (List) object;
			for (Object obj : list) {
				updateStatement(obj);
			}
		} else if (object instanceof OrderByElement) {
			OrderByElement se = (OrderByElement) object;
			Expression expression = se.getExpression();
			updateStatement(expression);
		} else if (object instanceof SelectExpressionItem) {
			SelectExpressionItem se = (SelectExpressionItem) object;
			Expression expression = se.getExpression();
			updateStatement(expression);
			String alias = se.getAlias();
			if (alias != null) {
				se.setAlias(alias.toUpperCase());
			}
		} else if (object instanceof BinaryExpression) {
			updateStatement(((BinaryExpression) object).getLeftExpression());
			updateStatement(((BinaryExpression) object).getRightExpression());
		} else if (object instanceof CaseExpression) {
			List<WhenClause> whenClauses = ((CaseExpression) object)
					.getWhenClauses();
			for (WhenClause whenClause : whenClauses) {
				Expression whenExpression = whenClause.getWhenExpression();
				updateStatement(whenExpression);
			}
		} else if (object instanceof Function) {
			Function function = (Function) object;
			String functionName = function.getName().toUpperCase();
			ExpressionList parameters = function.getParameters();
			if (parameters != null) {
				List<Expression> expressions = parameters.getExpressions();
				for (Expression exp : expressions) {
					updateStatement(exp);
				}
			}
			function.setName(functionName);
		} else if (object instanceof Parenthesis) {
			Parenthesis paranthesis = (Parenthesis) object;
			updateStatement(paranthesis.getExpression());
		}
	}
}

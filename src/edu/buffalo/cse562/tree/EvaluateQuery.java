package edu.buffalo.cse562.tree;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.OrderByElement;
import net.sf.jsqlparser.statement.select.SelectBody;
import net.sf.jsqlparser.statement.select.SelectItem;
import edu.buffalo.cse562.common.CommonUtil;
import edu.buffalo.cse562.common.WhereUtility;
import edu.buffalo.cse562.schema.Relation;
import edu.buffalo.cse562.schema.Tuple;

public class EvaluateQuery {

	QueryTree qt;
	static List<Tuple> tuples = new ArrayList<Tuple>();
	static Set<String> joinRelations = new HashSet<String>();
	static Set<Expression> equiJoins = new HashSet<Expression>();
	private BinaryExpression equiJoin = null;
	private List<BinaryExpression> joinExpressions = new ArrayList<BinaryExpression>();
	static HashMap<String, List<Expression>> whereList = new HashMap<String, List<Expression>>();

	// static HashMap<String, List<Tuple>> groupedTupleList = null;
	public EvaluateQuery(QueryTree qt) {

		this.qt = qt;
	}

	public void evaluateQuery(Map<String, Relation> tables, SelectBody body,
			WhereUtility wu) throws SQLException {
		if (qt.op.opString != null) {
			switch (qt.op.opString) {

			case "RelationalOperator":

			{
				Relation relation = tables.get(qt.op.opValue.toString());
				// Set the where conditions needed on this relation
				whereList = wu.getWhereList();
				List<Expression> whereConditions = whereList.get(relation
						.getTableName());
				Expression expression = null;
				// Convert it to And Expression
				if (whereConditions != null) {
					if (whereConditions.size() > 1) {
						for (int i = 0; i < whereConditions.size(); i++) {
							if (i == 0) {
								expression = new AndExpression(
										whereConditions.get(i),
										whereConditions.get(i + 1));
								i++;
							} else {
								expression = new AndExpression(expression,
										whereConditions.get(i));
							}
						}
					} else {
						expression = whereConditions.get(0);
					}
				}
				relation.setSelectExpression(expression);
				if (qt.op.opValue.toString().equals(
						CommonUtil.getRelationOnMemory())) {
					relation.setTuples(CommonUtil.readTuplesFromFile(relation));
				}
				relation.setTupleSchema(relation.getTupleSchema());
				qt.relationField = relation;

			}

				break;

			case "SelectionOperator": {
				Expression whereExp = (Expression) qt.op.opValue;

				SelectionOperator so = new SelectionOperator(
						qt.leftChild.relationField, whereExp);

				qt.relationField = so.operate(so.relation);
			}
				break;

			case "GroupByOperator": {
				List<Expression> groupByList = (List<Expression>) qt.op.opValue;
				if (groupByList != null) {
					GroupByOperator gbo = new GroupByOperator(
							qt.leftChild.relationField, groupByList);
					qt.relationField = gbo.operate(gbo.relation);
				}
			}
				break;

			case "ProjectOperator": {
				List<SelectItem> projectionItems = (List<SelectItem>) qt.op.opValue;
				ProjectOperator po = new ProjectOperator(
						qt.leftChild.relationField.getTupleSchema(),
						projectionItems);
				qt.relationField = po.operate(qt.leftChild.relationField);
			}
				break;

			case "OrderByOperator": {
				List<OrderByElement> OrderByItems = (List<OrderByElement>) qt.op.opValue;
				OrderByOperator ob = new OrderByOperator(
						qt.leftChild.relationField.getTupleSchema(),
						OrderByItems);
				qt.relationField = ob.operate(qt.leftChild.relationField);

			}
				break;

			case "CartesianOperator":

			{
				CartesianOperator co = new CartesianOperator(
						"CartesianOperator");
				// Fetch the equijoin for the left and right column
				equiJoins = wu.getEquiJoins();
				for (Expression expression : equiJoins) {
					BinaryExpression be = (BinaryExpression) expression;
					Column left = (Column) be.getLeftExpression();
					Column right = (Column) be.getRightExpression();
					if (left.getTable() != null && right.getTable() != null) {
						if ((left
								.getTable()
								.getName()
								.equals(qt.leftChild.relationField
										.getTableName()) && right
								.getTable()
								.getName()
								.equals(qt.rightChild.relationField
										.getTableName()))
								|| (right
										.getTable()
										.getName()
										.equals(qt.leftChild.relationField
												.getTableName()) && left
										.getTable()
										.getName()
										.equals(qt.rightChild.relationField
												.getTableName()))) {
							equiJoin = (BinaryExpression) expression;
							// Check if left and right are in proper order for
							// better handling
							if (left.getTable()
									.getName()
									.equals(qt.rightChild.relationField
											.getTableName())
									&& right.getTable()
											.getName()
											.equals(qt.leftChild.relationField
													.getTableName())) {
								Column temp = left;
								equiJoin.setLeftExpression(right);
								equiJoin.setRightExpression(temp);
							}
							joinExpressions.add(equiJoin);
						} else {
							// For already joined tables
							if ((qt.leftChild.relationField.getJoinRelations()
									.contains(left.getTable().getName()) && qt.rightChild.relationField
									.getTableName().equals(
											right.getTable().getName()))
									|| (qt.leftChild.relationField
											.getJoinRelations().contains(
													right.getTable().getName()) && qt.rightChild.relationField
											.getTableName().equals(
													left.getTable().getName()))) {
								equiJoin = (BinaryExpression) expression;
								// Check if left and right are in proper order
								// for
								// better handling
								if (qt.leftChild.relationField
										.getJoinRelations().contains(
												right.getTable().getName())
										&& qt.rightChild.relationField
												.getTableName().equals(
														left.getTable()
																.getName())) {
									Column temp = left;
									equiJoin.setLeftExpression(right);
									equiJoin.setRightExpression(temp);
								}
								joinExpressions.add(equiJoin);
							} /*else if (qt.leftChild.relationField
									.getJoinRelations().contains(
											left.getTable().getName())
									&& qt.leftChild.relationField
											.getJoinRelations().contains(
													right.getTable().getName())) {
								equiJoin = (BinaryExpression) expression;
								joinExpressions.add(equiJoin);
							}*/
						}
					}
				}
//				if (joinExpressions.size() > 1) {
//					AndExpression andExp = new AndExpression();
//					for (int i = 0; i < joinExpressions.size(); i++) {
//						if (i == 0) {
//							andExp.setLeftExpression(joinExpressions.get(0));
//							andExp.setRightExpression(joinExpressions.get(1));
//							i++;
//							continue;
//						}
//						andExp.setLeftExpression(andExp);
//						andExp.setRightExpression(joinExpressions.get(i));
//					}
//					equiJoin = andExp;
//				} else if (!joinExpressions.isEmpty()) {
//					equiJoin = joinExpressions.get(0);
//				}
				qt.relationField = co.operate(qt.leftChild.relationField,
						qt.rightChild.relationField, joinExpressions);
				qt.relationField.setTableName(qt.leftChild.relationField
						.getTableName()
						+ "_"
						+ qt.rightChild.relationField.getTableName());
				joinRelations.add(qt.leftChild.relationField.getTableName());
				joinRelations.add(qt.rightChild.relationField.getTableName());
				qt.relationField.setJoinRelations(joinRelations);

			}

			}
		}
	}
}

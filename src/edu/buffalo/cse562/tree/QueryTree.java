package edu.buffalo.cse562.tree;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;

import net.sf.jsqlparser.statement.select.SelectBody;
import edu.buffalo.cse562.common.WhereUtility;
import edu.buffalo.cse562.schema.Relation;

public class QueryTree extends OperatorInterface {

	// Data Members

	public QueryTree root;

	public QueryTree getLeftChild() {
		return leftChild;
	}

	public void setLeftChild(QueryTree leftChild) {
		this.leftChild = leftChild;
	}

	public QueryTree getRightChild() {
		return rightChild;
	}

	public void setRightChild(QueryTree rightChild) {
		this.rightChild = rightChild;
	}

	public Relation getRelationField() {
		return relationField;
	}

	public void setRelationField(Relation relationField) {
		this.relationField = relationField;
	}

	public QueryTree leftChild;
	QueryTree rightChild;
	public OperatorInterface op;
	QueryTree parent;

	Queue<QueryTree> qtQueue = new LinkedList<QueryTree>();
	HashMap<String, Boolean> mapOfOperators = new HashMap<String, Boolean>();
	boolean insertToRight;
	boolean ignoreLastIf;
	boolean visited;
	public Relation relationField;
	public boolean skip = false;

	QueryTree(OperatorInterface x) {
		this.op = x;
	}

	public QueryTree() {
		// default constructor
		mapOfOperators.put("CartesianOperator0", true);
		mapOfOperators.put("CartesianOperator1", true);
		mapOfOperators.put("CartesianOperator2", true);
		mapOfOperators.put("CartesianOperator3", true);
		mapOfOperators.put("CartesianOperator4", true);
		mapOfOperators.put("CartesianOperator5", true);
		mapOfOperators.put("CartesianOperator6", true);
		mapOfOperators.put("CartesianOperator7", true);
		mapOfOperators.put("UnionOperator", true);
		mapOfOperators.put("JoinOperator", true);

	}

	public void insert(OperatorInterface x) {
		
		if (root == null) {
			root = new QueryTree(x);
			qtQueue.add(root);
		}

		else {
			QueryTree temp = qtQueue.peek();
			QueryTree insert = new QueryTree(x);

			if (mapOfOperators.containsKey(temp.op.opValue)) {
				if (mapOfOperators.containsKey(insert.op.opValue)) {
					if (temp.leftChild != null) {
						while (temp.rightChild == null
								&& temp.leftChild != null) {
							temp = temp.leftChild;
						}
					}
					temp.setLeftChild(insert);
					qtQueue.add(temp.leftChild);
					return;
				} else {
					if (temp.leftChild != null) {

						temp.setRightChild(insert);

						qtQueue.poll();

						return;
					} else if (temp.leftChild == null) {
						if (temp.rightChild == null) {

							temp.setRightChild(insert);

							return;
						} else {

							temp.setLeftChild(insert);

							qtQueue.poll();

						}
					}
				}
			}

			if (temp.leftChild == null && insertToRight == false) {

				temp.setLeftChild(insert);
				temp.leftChild.parent = temp;
				qtQueue.poll();

				qtQueue.add(temp.leftChild);

			}

		}
	}

	public void treeTraversal(QueryTree node) {
		if (node == null) {
			return;
		}

		treeTraversal(node.rightChild);
		treeTraversal(node.leftChild);

	}

	public QueryTree treeTraverse(QueryTree node,
			Map<String, Relation> tupleSchema, SelectBody body, WhereUtility wu) throws SQLException {

		if (node == null) {

			return null;
		}

		QueryTree left = treeTraverse(node.leftChild, tupleSchema, body, wu);
		QueryTree right = treeTraverse(node.rightChild, tupleSchema, body, wu);
		EvaluateQuery eq;
		if (right != null) {
			if (!node.rightChild.visited) {
				System.out.println("Right : " + right.op.opValue.toString());
				node.rightChild.visited = true;
				eq = new EvaluateQuery(right);
				eq.evaluateQuery(tupleSchema, body, wu);
			}
		}

		if (left != null && !node.leftChild.visited) {
			node.rightChild.visited = true;
			System.out.println("Right : " + left.op.opValue.toString());
			node.leftChild.visited = true;
			eq = new EvaluateQuery(left);
			eq.evaluateQuery(tupleSchema, body, wu);
		}

		eq = new EvaluateQuery(node);
		eq.evaluateQuery(tupleSchema, body, wu);
		//System.out.println("Root : " + node.op.opValue.toString());
		node.visited = true;
		return node;

	}

	@Override
	public Relation operate(Relation relation) {
		// TODO Auto-generated method stub
		return null;
	}

}

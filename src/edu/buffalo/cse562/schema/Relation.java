package edu.buffalo.cse562.schema;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import net.sf.jsqlparser.expression.Expression;

/**
 * This class serves as a placeholder for schema definitions for each relation.
 * 
 * @author Animesh Kumar <animeshk@buffalo.edu>
 *
 */
public class Relation {

	public Relation() {
		tupleSchema = new LinkedHashMap<String, CustomColumn>();
	}

	/**
	 * Stores the name of the relation
	 */
	private String tableName;

	/**
	 * Links to the file path which contains the data for the given table.
	 */
	private String dataFile;

	/**
	 * List of tuples contained in the relation
	 */
	private List<Tuple> tuples;

	/**
	 * A grouped list of List of tuples contained in the relation
	 */
	private Map<String, List<Tuple>> groupedListOfTuples;

	/**
	 * List of columns
	 */
	private Map<String, CustomColumn> tupleSchema;

	private Expression selectExpression;

	private Set<String> joinRelations;

	/**
	 * Name of the base / intermediate relation
	 */
	private String name;
	
	private boolean fromItem;

	/**
	 * Retrieves the table name.
	 * 
	 * @return
	 */
	public String getTableName() {
		return tableName;
	}

	public void setTableName(String tableName) {
		this.tableName = tableName;
	}

	public String getDataFile() {
		return dataFile;
	}

	public void setDataFile(String dataFile) {
		this.dataFile = dataFile;
	}

	public List<Tuple> getTuples() {
		return tuples;
	}

	public void setTuples(List<Tuple> tuples) {
		this.tuples = tuples;
	}

	public Map<String, CustomColumn> getTupleSchema() {
		return tupleSchema;
	}

	public void setTupleSchema(Map<String, CustomColumn> tupleSchema) {
		this.tupleSchema = tupleSchema;
	}

	private boolean isGrouped;

	public void setGrouped(boolean b) {
		// TODO Auto-generated method stub
		isGrouped = b;
	}

	private List<Integer> groupEndIndices = new ArrayList<Integer>();

	public List<Integer> getGroupEndIndices() {
		return groupEndIndices;
	}

	public Map<String, List<Tuple>> getGroupedListOfTuples() {
		return groupedListOfTuples;
	}

	public void setGroupedListOfTuples(
			Map<String, List<Tuple>> groupedListOfTuples) {
		this.groupedListOfTuples = groupedListOfTuples;
	}

	public void setGroupEndIndices(List<Integer> groupEndIndices) {
		this.groupEndIndices = groupEndIndices;
	}

	public boolean isGrouped() {
		return isGrouped;
	}

	public Expression getSelectExpression() {
		return selectExpression;
	}

	public void setSelectExpression(Expression selectExpression) {
		this.selectExpression = selectExpression;
	}

	public Set<String> getJoinRelations() {
		if (joinRelations == null) {
			joinRelations = new HashSet<String>();
		}
		return joinRelations;
	}

	public void setJoinRelations(Set<String> joinRelations) {
		this.joinRelations = joinRelations;
	}

	/**
	 * @return the name
	 */
	public String getName() {
		return name;
	}

	/**
	 * @param name
	 *            the name to set
	 */
	public void setName(String name) {
		this.name = name;
	}

	public boolean isFromItem() {
		return fromItem;
	}

	public void setFromItem(boolean fromItem) {
		this.fromItem = fromItem;
	}

}

package edu.buffalo.cse562.common;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import edu.buffalo.cse562.schema.CustomColumn;
import edu.buffalo.cse562.schema.Relation;

public class ParseSchema {

	int index = 0;
	Map<String, Relation> tables = new TreeMap<String, Relation>(String.CASE_INSENSITIVE_ORDER);
	private Map<String, CustomColumn> tupleSchema = new TreeMap<String, CustomColumn>(String.CASE_INSENSITIVE_ORDER);
	private String dataDirectory;

	public ParseSchema(String dataDirectory) {
		this.dataDirectory = dataDirectory;
	}

	public void parseSchema(CreateTable createStatement) {

		// Create an Relation object to store the column
		// definitions
		Relation relation = new Relation();
		// TODO
		// Set the table name
		relation.setTableName(createStatement.getTable().getName());
		relation.setName(createStatement.getTable().getName());
		
		//System.out.println("Table Name:: "+relation.getTableName());

		relation.setDataFile(dataDirectory + "/"
				+ relation.getTableName().toLowerCase() + ".dat");
		
		//System.out.println("Data File:: "+relation.getDataFile());

		// Run through each of the attribute and fill in column
		// details of the relation
		List<ColumnDefinition> columnDefinitions = createStatement
				.getColumnDefinitions();
		for (ColumnDefinition definition : columnDefinitions) {
			// Set the attribute name and type into our custom
			// type created to store the column information
			CustomColumn column = new CustomColumn();
			column.setColumnName(definition.getColumnName());
			column.setDataType(definition.getColDataType().getDataType());
			column.setIndex(index);
			Table table = new Table();
			table.setName(createStatement.getTable().getName());
			column.setTable(table);
			tupleSchema.put(column.getTable().getName() + CommonConstants.DOT
					+ column.getColumnName(), column);
			index++;

		}
		tables.put(relation.getTableName(), relation);

	}

	public Map<String, Relation> getTables() {
		return tables;
	}

	public Map<String, CustomColumn> getTupleSchema() {
		return tupleSchema;
	}

	

}

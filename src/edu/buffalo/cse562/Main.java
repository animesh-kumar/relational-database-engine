package edu.buffalo.cse562;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import net.sf.jsqlparser.expression.LeafValue.InvalidLeaf;
import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.parser.ParseException;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.Limit;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectBody;
import edu.buffalo.cse562.common.ColumnUtility;
import edu.buffalo.cse562.common.CommonUtil;
import edu.buffalo.cse562.common.IndexType;
import edu.buffalo.cse562.common.WhereUtility;
import edu.buffalo.cse562.handler.GenerateTree;
import edu.buffalo.cse562.schema.CustomColumn;
import edu.buffalo.cse562.schema.Relation;
import edu.buffalo.cse562.schema.Tuple;
import edu.buffalo.cse562.tree.EvaluateQueryWithOneRelation;
import edu.buffalo.cse562.tree.QueryTree;

/**
 * @author Priyankar Nandi <pnandi@buffalo.edu>
 * @author Animesh Kumar <animeshk@buffalo.edu>
 * @author Deep Parekh <deepjaye@buffalo.edu>
 * 
 */
public class Main {

	public static void main(String[] args) throws SQLException, InvalidLeaf {
		// long start = System.currentTimeMillis();

		List<File> sqlFiles = new ArrayList<File>();
		Map<String, Relation> relationsMasterCopy = new TreeMap<String, Relation>(
				String.CASE_INSENSITIVE_ORDER);
		String dataDir = null;
		String dbDir = null;
		Boolean isLoad = false;

		for (int i = 0; i < args.length; i++) {
			if (("--data").equals(args[i])) {
				i++;
				dataDir = args[i];
			} else if (("--db").equals(args[i])) {
				i++;
				dbDir = args[i];
				// CommonUtil.setSwapAvailable(true);
			} else if (("--load").equals(args[i])) {
				isLoad = true;
			} else {
				sqlFiles.add(new File(args[i]));
			}
		}

		for (File sqlFile : sqlFiles) {

			try {

				FileReader stream = new FileReader(sqlFile);
				CCJSqlParser parser = new CCJSqlParser(stream);
				Statement stmt;
				while ((stmt = parser.Statement()) != null) {
					if (stmt instanceof CreateTable) {
						CreateTable createStatement = (CreateTable) stmt;
						// Create an Relation object to store the column
						// definitions
						Relation relation = new Relation();
						// TODO
						// Set the table name
						Table table = createStatement.getTable();
						String relationName = table.getName().toUpperCase();
						relation.setTableName(relationName);
						relation.setDataFile(dataDir + File.separator
								+ relation.getTableName().toLowerCase()
								+ ".dat");
						// Run through each of the attribute and fill in column
						// details of the relation
						List<ColumnDefinition> columnDefinitions = createStatement
								.getColumnDefinitions();
						Map<String, CustomColumn> tupleSchema = new LinkedHashMap<String, CustomColumn>();
						int index = 0;
						for (ColumnDefinition definition : columnDefinitions) {
							// Set the attribute name and type into our custom
							// type created to store the column information
							CustomColumn column = new CustomColumn();
							String columnName = definition.getColumnName()
									.toUpperCase();
							// String completeColumnName = relationName
							// + CommonConstants.DOT + columnName;
							column.setColumnName(columnName);
							column.setDataType(definition.getColDataType()
									.getDataType().toLowerCase());
							column.setOriginalIndex(index);
							column.setIndex(index);
							column.setTable(table);
							column.setRequired(false);
							// All columns by default won't be indexed. Indexing
							// will be determined later.
							column.setIndexType(IndexType.NO_INDEX);
							String wholeColumnName = column
									.getWholeColumnName().toUpperCase();
							tupleSchema.put(wholeColumnName, column);
							CommonUtil.updateColumnRelationMap(wholeColumnName,
									relationName);
							CommonUtil.updateColumnRelationMap(columnName,
									relationName);
							index++;
						}
						relation.setTupleSchema(tupleSchema);
						relationsMasterCopy.put(relationName, relation);
					} else if (stmt instanceof Select) {
						SelectBody selectBody = ((Select) stmt).getSelectBody();
						CommonUtil.relationsMasterCopy = relationsMasterCopy;
						GenerateTree gt = new GenerateTree();
						QueryTree qt = gt.generateTree(selectBody,
								relationsMasterCopy);
						// Get a list of all required columns from the query
						ColumnUtility cu = new ColumnUtility();
						cu.setRequiredColumns(relationsMasterCopy, selectBody);
						Relation rel = null;
						PlainSelect ps = (PlainSelect) selectBody;
						if (ps.getJoins() == null) {
							// This is a huge hack run all the operators on each
							// tuple one by one
							// Our Query plan doesn't work like that and needs
							// everything in memory
							// Hence this hack
							EvaluateQueryWithOneRelation eqr = new EvaluateQueryWithOneRelation(
									ps);
							FromItem fromItem = ps.getFromItem();
							Table table = (Table) fromItem;
							rel = eqr.executeQuery(relationsMasterCopy
									.get(table.getName()));
						} else {
							// Get the list of all Where items and equiJoins
							WhereUtility wu = new WhereUtility(selectBody,
									relationsMasterCopy);
							qt = qt.treeTraverse(qt.root, relationsMasterCopy,
									selectBody, wu);
							rel = qt.getLeftChild().getRelationField();
						}
						List<Tuple> tuples = rel.getTuples();
						Limit limit = ps.getLimit();
						long limitCount = tuples.size();
						if (limit != null) {
							limitCount = limit.getRowCount();
						}
						for (int i = 0; i < limitCount; i++) {
							System.out.println(tuples.get(i).getStringVal());
						}
					}
				}

			} catch (IOException e) {
				e.printStackTrace();
			} catch (ParseException e) {
				e.printStackTrace();
			}

		}
		// long end = System.currentTimeMillis();
		// System.out.println("Query output in " + (end - start) / 1000 +
		// " secs");
	}
}

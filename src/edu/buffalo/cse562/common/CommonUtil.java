package edu.buffalo.cse562.common;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import net.sf.jsqlparser.expression.BooleanValue;
import net.sf.jsqlparser.expression.DateValue;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.LeafValue;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.StringValue;
import edu.buffalo.cse562.schema.CustomColumn;
import edu.buffalo.cse562.schema.Relation;
import edu.buffalo.cse562.schema.Tuple;

public class CommonUtil {

	private static boolean swapAvailable;

	public static Map<String, Relation> relationsMasterCopy;

	private static Map<String, String> columnRelationMap;

	private static Map<Integer, String> columnIndexMapForTuple;

	private static String relationOnMemory = null;

	private static TreeMap<String, DateValue> dateMap = new TreeMap<String, DateValue>();
	private static TreeMap<String, StringValue> stringMap = new TreeMap<String, StringValue>();

	public static Map<Integer, String> getColumnIndexMapForTuple() {
		return columnIndexMapForTuple;
	}

	public static void setColumnIndexMapForTuple(
			Map<Integer, String> columnIndexMapForTuple) {
		CommonUtil.columnIndexMapForTuple = columnIndexMapForTuple;
	}

	public static Map<String, String> getColumnRelationMap() {
		return columnRelationMap;
	}

	public static void updateColumnRelationMap(String columnName,
			String tableName) {
		if (columnRelationMap == null) {
			columnRelationMap = new LinkedHashMap<String, String>();
		}
		columnRelationMap.put(columnName, tableName);
	}

	public static boolean isSwapAvailable() {
		return swapAvailable;
	}

	public static void setSwapAvailable(boolean swapAvailable) {
		CommonUtil.swapAvailable = swapAvailable;
	}

	public static List<Tuple> readTuplesFromFile(Relation relation)
			throws SQLException {
		// System.out.println("Inside");
		BufferedReader reader = null;
		List<Tuple> tuples = new LinkedList<Tuple>();
		try {
			reader = new BufferedReader(new FileReader(relation.getDataFile()));
			String line;
			BooleanValue bool = null;
			Expression selectExpression = relation.getSelectExpression();
			// Build the new tuple schema
			Map<String, CustomColumn> oldTupleSchema = relation
					.getTupleSchema();
			Map<String, CustomColumn> newTupleSchema = new LinkedHashMap<String, CustomColumn>();
			Map<Integer, String> indexMap = new LinkedHashMap<Integer, String>();
			CustomColumn col;
			int newIndex = 0;
			for (String key : oldTupleSchema.keySet()) {
				col = oldTupleSchema.get(key);
				if (col.isRequired()) {
					indexMap.put(col.getOriginalIndex(), col.getDataType());
					col.setIndex(newIndex++);
					newTupleSchema.put(key, col);
				}
			}

			CommonUtil.setColumnIndexMapForTuple(indexMap);
			int count = 0;
			CommonEval obj = new CommonEval(relation.getTupleSchema());
			while ((line = reader.readLine()) != null) {
				// System.out.println(line);
				Tuple tuple = new Tuple(line);
				if (selectExpression != null) {
						// Change the eval object
						obj.setTuple(tuple);
						bool = (BooleanValue) obj.eval(selectExpression);
					if (bool.getValue()) {
						tuples.add(tuple);
						count++;
					}
				} else {
					tuples.add(tuple);
					count++;
				}

				// if(count % 1000 == 0 && count != 0)
				// System.out.println(count);

				if (count % 1000000 == 0 && count != 0)
					System.gc();

			}
			relation.setTupleSchema(newTupleSchema);
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				reader.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		// System.out.println("Outside");
		return tuples;
	}

	public static Map<String, CustomColumn> mergeTupleSchemas(
			Map<String, CustomColumn> relATupleSchema,
			Map<String, CustomColumn> relBTupleSchema) {
		Map<String, CustomColumn> retMap = new LinkedHashMap<String, CustomColumn>();
		retMap.putAll(relATupleSchema);
		Set<String> keysA = relATupleSchema.keySet();
		int index = keysA.size();
		for (String s : relBTupleSchema.keySet()) {
			CustomColumn col = relBTupleSchema.get(s);
			col.setIndex(index++);
			relBTupleSchema.put(s, col);
		}
		retMap.putAll(relBTupleSchema);
		// System.out.println("Return map size:: "+retMap.size());
		return retMap;
	}

	public static LeafValue getLeafVal(String data, String colDataType) {

		switch (colDataType) {
		case "string":
		case "varchar":
		case "char":
			// StringValue construction eats up first and last character hence
			// needed
			if (stringMap.containsKey(data)) {
				return stringMap.get(data);
			} else {
				StringValue val = new StringValue("  ");
				val.setValue(data);
				stringMap.put(data, val);
				return val;
			}
		case "int":
			LongValue intVal = new LongValue(data);
			return intVal;
		case "decimal":
			DoubleValue decimalVal = new DoubleValue(data);
			return decimalVal;
		case "double":
			DoubleValue doubleVal = new DoubleValue(data);
			return doubleVal;
		case "date":
			if (dateMap.containsKey(data)) {
				return dateMap.get(data);
			} else {
				DateValue dateVal = new DateValue(" " + data + " ");
				dateMap.put(data, dateVal);
				return dateVal;
			}
			// System.out.println(longVal);
		default:
			if (stringMap.containsKey(data)) {
				return stringMap.get(data);
			} else {
				StringValue val = new StringValue("  ");
				val.setValue(data);
				stringMap.put(data, val);
				return val;
			}
		}

	}

	public static String getRelationOnMemory() {
		return relationOnMemory;
	}

	public static void setRelationOnMemory(String relationOnMemory) {
		CommonUtil.relationOnMemory = relationOnMemory;
	}

	public static void createNewTupleSchema(Relation relation) {
		// Build the new tuple schema
		Map<String, CustomColumn> oldTupleSchema = relation.getTupleSchema();
		Map<String, CustomColumn> newTupleSchema = new LinkedHashMap<String, CustomColumn>();
		Map<Integer, String> indexMap = new LinkedHashMap<Integer, String>();
		CustomColumn col;
		int newIndex = 0;
		for (String key : oldTupleSchema.keySet()) {
			col = oldTupleSchema.get(key);
			if (col.isRequired()) {
				indexMap.put(col.getOriginalIndex(), col.getDataType());
				col.setIndex(newIndex++);
				newTupleSchema.put(key, col);
			}
		}
		relation.setTupleSchema(newTupleSchema);
		CommonUtil.setColumnIndexMapForTuple(indexMap);
	}

	public static Map<Integer, String> createNewTupleSchema1(Relation relation) {
		// Build the new tuple schema
		Map<String, CustomColumn> oldTupleSchema = relation.getTupleSchema();
		Map<String, CustomColumn> newTupleSchema = new LinkedHashMap<String, CustomColumn>();
		Map<Integer, String> indexMap = new LinkedHashMap<Integer, String>();
		CustomColumn col;
		int newIndex = 0;
		for (String key : oldTupleSchema.keySet()) {
			col = oldTupleSchema.get(key);
			if (col.isRequired()) {
				indexMap.put(col.getOriginalIndex(), col.getDataType());
				col.setIndex(newIndex++);
				newTupleSchema.put(key, col);
			}
		}
		relation.setTupleSchema(newTupleSchema);
		return indexMap;
	}

}

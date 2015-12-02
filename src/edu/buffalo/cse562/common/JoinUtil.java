package edu.buffalo.cse562.common;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.BooleanValue;
import net.sf.jsqlparser.expression.DateValue;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.LeafValue;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.schema.Column;
import edu.buffalo.cse562.schema.CustomColumn;
import edu.buffalo.cse562.schema.Relation;
import edu.buffalo.cse562.schema.Tuple;

public class JoinUtil {

	public static Relation hashJoin(Relation rel1, Relation rel2,
			List<BinaryExpression> expressions) {
		Relation relation = new Relation();
		Map<String, CustomColumn> tuple1Schema = rel1.getTupleSchema();
		Map<String, List<Tuple>> map = new HashMap<String, List<Tuple>>();
		Tuple tuple1, tuple2;
		String key;
		List<Tuple> lst1, retTuples = new LinkedList<Tuple>();
		BufferedReader reader2 = null;
		try {
			reader2 = new BufferedReader(new FileReader(rel2.getDataFile()));
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		String line2;

		/**
		 * Create the map from the tuples in the first relation
		 */
		Iterator<Tuple> itr1 = rel1.getTuples().iterator();
		while (itr1.hasNext()) {
			tuple1 = itr1.next();
			key = getKey(expressions, tuple1,rel1.getTupleSchema());
			if (map.containsKey(key)) {
				lst1 = map.get(key);
			} else {
				lst1 = new LinkedList<Tuple>();
			}
			lst1.add(tuple1);
			map.put(key, lst1);
		}

		/**
		 * Iterate over the second list and search in the map and join
		 */
		CommonUtil.createNewTupleSchema(rel2);
		BooleanValue bool = null;
		Expression selectExpression = rel2.getSelectExpression();
		Map<String, CustomColumn> tuple2Schema = rel2.getTupleSchema();
		try {
			CommonEval obj = new CommonEval(tuple2Schema);
			while ((line2 = reader2.readLine()) != null) {
				boolean addTuple = false;
				tuple2 = new Tuple(line2);
				// Filter out all rows with where conditions
				if (selectExpression != null) {
					obj.setTuple(tuple2);
					try {
						bool = (BooleanValue) obj.eval(selectExpression);
					} catch (SQLException e) {
						e.printStackTrace();
					}
					if (bool.getValue()) {
						addTuple = true;
					}
				} else {
					addTuple = true;
				}
				if (addTuple) {
					key = getKey(expressions, tuple2, rel2.getTupleSchema());
					if (map.containsKey(key)) {
						lst1 = map.get(key);
						for (Tuple t : lst1) {
							Tuple retTuple = new Tuple();
							List<LeafValue> lst = new ArrayList<LeafValue>();
							lst.addAll(Arrays.asList(t.getValues()));
							lst.addAll(Arrays.asList(tuple2.getValues()));
							retTuple.setValues(lst.toArray(new LeafValue[] {}));
							retTuples.add(retTuple);
						}
					}
				}
			}

		} catch (IOException e) {
			e.printStackTrace();
		}

		relation.setName(rel1.getName() + rel2.getName());
		relation.setTuples(retTuples);
		// modify tuple schema
		Map<String, CustomColumn> retTupleSchema = CommonUtil
				.mergeTupleSchemas(tuple1Schema, tuple2Schema);
		relation.setTupleSchema(retTupleSchema);

		return relation;
	}

	public static Relation externalJoin(Relation rel1,Relation rel2,Expression expression){
		Relation relation = new Relation();
		/*String fileName1=rel1.getDataFile();
		Map<String, CustomColumn> tupleSchema1 = rel1.getTupleSchema();
		String fileName2=rel2.getDataFile();
		Map<String, CustomColumn> tupleSchema2 = rel2.getTupleSchema();
		BufferedReader reader1 = null,reader2 = null;
		Map<String,Tuple> map = new HashMap<String,Tuple>();
		String line1,line2;
		Tuple tuple1,tuple2;
		LeafValue key1,key2,currval1,currval2;
		List<Tuple> interMediateLst1=new ArrayList<Tuple>(),interMediateLst2=new ArrayList<Tuple>(),retTuples=new ArrayList<Tuple>();
		Map<Integer,String> indexMap1,indexMap2;
		indexMap1 = CommonUtil.createNewTupleSchema1(rel1);
		indexMap2 = CommonUtil.createNewTupleSchema1(rel2);
		try {
			reader1 = new BufferedReader(new FileReader(fileName1));
			reader2 = new BufferedReader(new FileReader(fileName2));
			line1 = reader1.readLine();
			line2 = reader2.readLine();
			
			while (line1 != null && line2!= null) {
				CommonUtil.setColumnIndexMapForTuple(indexMap1);
				tuple1 = new Tuple(line1);
				key1=getKey(expression,tuple1,tupleSchema1,0);
				CommonUtil.setColumnIndexMapForTuple(indexMap2);
				tuple2 = new Tuple(line2);
				key2=getKey(expression,tuple2,tupleSchema2,1);
				int compareVal = compareLeafValues(key1,key2);
				if(compareVal==0){
					currval1 = key1;
					while(key1!=null && 0==compareLeafValues(key1,currval1)){
						interMediateLst1.add(tuple1);
						line1=reader1.readLine();
						if(line1!=null){
							CommonUtil.setColumnIndexMapForTuple(indexMap1);
							tuple1 = new Tuple(line1);
							key1=getKey(expression,tuple1,tupleSchema1,0);
						}else{
							key1 = null;
						}
					}
					
					currval2 = key2;
					while(key2!=null && 0==compareLeafValues(key2,currval1)){
						interMediateLst2.add(tuple2);
						line2=reader2.readLine();
						if(line2!=null){
							CommonUtil.setColumnIndexMapForTuple(indexMap2);
							tuple2 = new Tuple(line2);
							key2=getKey(expression,tuple2,tupleSchema2,1);
						}else{
							key2 = null;
						}
					}
					if(interMediateLst1.size()>0 && interMediateLst2.size()>0){
						for(Tuple tup1:interMediateLst1){
							for(Tuple tup2:interMediateLst2){
								Tuple retTuple = new Tuple();
								List<LeafValue> lst = new ArrayList<LeafValue>();
								lst.addAll(Arrays.asList(tup1.getValues()));
								lst.addAll(Arrays.asList(tup2.getValues()));
								retTuple.setValues(lst.toArray(new LeafValue[] {}));
								retTuples.add(retTuple);
							}
						}
					}
					interMediateLst1=new ArrayList<Tuple>();
					interMediateLst2=new ArrayList<Tuple>();
				}else{

					
					if(compareVal==-1){
						line1 = reader1.readLine();
					}
					else{
						line2 = reader2.readLine();
					}
					
				}
				
			}
			
			relation.setName(rel1.getName()+rel2.getName());
			relation.setTuples(retTuples);
			//modify tuple schema
			Map<String,CustomColumn> retTupleSchema = CommonUtil.mergeTupleSchemas(rel1.getTupleSchema(),rel2.getTupleSchema());
			relation.setTupleSchema(retTupleSchema);
			
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally{
			try {
				reader1.close();
				reader2.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
		}*/
		return relation;
	}
	/**
	 * Gives the key to be put inside / search in the map
	 * 
	 * @param expressions
	 * @param index
	 *            - 0 (gives the key for left expression) 1 (gives the key for
	 *            right expression)
	 */
	public static String getKey(List<BinaryExpression> expressions, Tuple tuple,Map<String,CustomColumn> tupleSchema) {
		BinaryExpression binExp ;
		StringBuilder key=new StringBuilder();
		Iterator<BinaryExpression> itr = expressions.iterator();
		Column leftCol,rightCol,colToEval;
		int index; // 0 - left table 1-right table
		while(itr.hasNext()){
			binExp=(BinaryExpression)itr.next();
			leftCol = (Column)binExp.getLeftExpression();
			rightCol = (Column)binExp.getRightExpression();	
			if(tupleSchema.get(leftCol.getWholeColumnName())!=null){
				colToEval = leftCol;
			}
			else{
				colToEval = rightCol;

			}
			
			CommonEval obj = new CommonEval(tupleSchema);
			obj.setTuple(tuple);
			try {
				LeafValue val = obj.eval(colToEval);
				key.append(val.toString()).append(CommonConstants.PIPE);
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
		}

		return key.toString();

	}

	static int compareLeafValues(LeafValue val1, LeafValue val2) {
		if (val1 instanceof DoubleValue) {
			return new Double(((DoubleValue) val1).getValue())
					.compareTo(new Double(((DoubleValue) val2).getValue()));
		} else if (val1 instanceof LongValue) {
			return new Long(((LongValue) val1).getValue()).compareTo(new Long(
					((LongValue) val2).getValue()));
		} else if (val1 instanceof StringValue) {
			return new String(((StringValue) val1).getValue())
					.compareTo(new String(((StringValue) val2).getValue()));
		} else if (val1 instanceof DateValue && val2 instanceof DateValue) {
			return ((DateValue) val1).getValue().compareTo(
					((DateValue) val2).getValue());
		}
		return -1;
	}

}

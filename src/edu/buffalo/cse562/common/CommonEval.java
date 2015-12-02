package edu.buffalo.cse562.common;

import java.sql.SQLException;
import java.util.Map;

import net.sf.jsqlparser.expression.LeafValue;
import net.sf.jsqlparser.schema.Column;
import edu.buffalo.cse562.Eval;
import edu.buffalo.cse562.schema.CustomColumn;
import edu.buffalo.cse562.schema.Tuple;

public class CommonEval extends Eval {

	private Map<String, CustomColumn> tupleSchema;
	private Tuple tuple;

	public CommonEval(Map<String, CustomColumn> tupleSchema) {
		this.tupleSchema = tupleSchema;
	}

	public Tuple getTuple() {
		return tuple;
	}

	public void setTuple(Tuple tuple) {
		this.tuple = tuple;
	}

	@Override
	public LeafValue eval(Column x) throws SQLException {
		String colName = x.getWholeColumnName();
		CustomColumn column = tupleSchema.get(colName);
		int index = column.getIndex();
		return tuple.getValues()[index];
	}

}

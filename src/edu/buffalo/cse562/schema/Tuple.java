package edu.buffalo.cse562.schema;

import java.util.Map;

import net.sf.jsqlparser.expression.LeafValue;
import net.sf.jsqlparser.expression.StringValue;
import edu.buffalo.cse562.common.CommonConstants;
import edu.buffalo.cse562.common.CommonUtil;

public class Tuple {

	private LeafValue[] values;

	private static Map<Integer, String> indexMap = CommonUtil
			.getColumnIndexMapForTuple();

	public Tuple() {

	}

	/**
	 * indexMap - <index of the column,datatype of tthe column>
	 * 
	 * @param param
	 * @param indexMap
	 */
	public Tuple(String param) {
		indexMap = CommonUtil.getColumnIndexMapForTuple();
		String[] arr = param.split("\\|");
		values = new LeafValue[indexMap.size()];
		int index = 0;
		for (int key : indexMap.keySet()) {
			values[index++] = CommonUtil
					.getLeafVal(arr[key], indexMap.get(key));
		}
	}

	public String getStringVal() {
		StringBuilder sb = new StringBuilder();
		int length = values.length;
		for (int i = 0; i < length; i++) {
			LeafValue val = values[i];
			if (val instanceof StringValue) {
				// To prevent the extra single quotes from being added in the
				// beginning
				// Check the toString() of StringValue for more information
				if (i != length - 1) {
					sb.append(((StringValue) val).getNotExcapedValue()).append(
							CommonConstants.PIPE);
				} else {
					sb.append(((StringValue) val).getNotExcapedValue());
				}
			} else {
				if (i != length - 1) {
					sb.append(val).append(CommonConstants.PIPE);
				} else {
					sb.append(val);
				}
			}
		}
		return sb.toString();
	}

	/**
	 * @return the values
	 */
	public LeafValue[] getValues() {
		return values;
	}

	/**
	 * @param values
	 *            the values to set
	 */
	public void setValues(LeafValue[] values) {
		this.values = values;
	}

}

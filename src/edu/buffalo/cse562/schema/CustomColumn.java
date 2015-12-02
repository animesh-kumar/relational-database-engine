package edu.buffalo.cse562.schema;

import edu.buffalo.cse562.common.IndexType;
import net.sf.jsqlparser.schema.Column;

public class CustomColumn extends Column {

	public CustomColumn() {
		isAlias = false;
	}

	/**
	 * Indicates the index of the column in the schema
	 */
	private int originalIndex;

	/**
	 * Indicates the updated index of the column in the schema after selections,
	 * aggregates etc.
	 */
	private int updatedIndex;

	private boolean required;
	
	private IndexType indexType;

	/**
	 * Indicates the datatype of the column
	 */
	private String dataType;

	private Boolean isAlias;

	public String getDataType() {
		return dataType;
	}

	public void setDataType(String dataType) {
		this.dataType = dataType;
	}

	public int getOriginalIndex() {
		return originalIndex;
	}

	public void setOriginalIndex(int index) {
		this.originalIndex = index;
	}

	public Boolean getIsAlias() {
		return isAlias;
	}

	public void setIsAlias(Boolean isAlias) {
		this.isAlias = isAlias;
	}

	public int getIndex() {
		return updatedIndex;
	}

	public void setIndex(int updatedIndex) {
		this.updatedIndex = updatedIndex;
	}

	public boolean isRequired() {
		return required;
	}

	public void setRequired(boolean required) {
		this.required = required;
	}

	public IndexType getIndexType() {
		return indexType;
	}

	public void setIndexType(IndexType indexType) {
		this.indexType = indexType;
	}

}

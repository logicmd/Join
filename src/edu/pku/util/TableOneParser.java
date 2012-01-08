package edu.pku.util;

public class TableOneParser {
	String values[];
	String PrimaryKey;
	String ContentColumn;
	
	public TableOneParser() {
		return;
	}
	
	public boolean parse(Object value) {
		values = value.toString().split("\t");
		if(values.length != 2)
			return false;
		else {
			try {
				PrimaryKey = new String(values[0]);
				ContentColumn = new String(values[1]);
			} catch (IndexOutOfBoundsException e) {
				e.printStackTrace();
			}
			return true;
		}
	}
	
	public String getPrimaryKey() {
		return PrimaryKey;
	}
	
	public String getContentColumn() {
		return ContentColumn;
	}
	
	public String getTag() {
		return "0";
	}
	
}

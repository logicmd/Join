package edu.pku.util;

public class TableTwoParser {
	String values[];
	String PrimaryKey;
	String ContentColumns;
	
	public TableTwoParser() {
		return;
	}
	
	public boolean parse(Object value) {
		ContentColumns = new String();
		values = value.toString().split("\t");
		if(values.length < 3)
			return false;
		else {
			try {
				PrimaryKey = new String(values[0]);
				for(int i = 1; i < values.length; i++) {
					ContentColumns = ContentColumns.concat(values[i]);
					if(i != values.length - 1) {
						ContentColumns = ContentColumns.concat("\t");
					}
				}
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
		return ContentColumns;
	}
	
	public String getTag() {
		return "1";
	}
	
}

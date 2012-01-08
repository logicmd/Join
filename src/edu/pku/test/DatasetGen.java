package edu.pku.test;

import edu.pku.util.DatasetFactory;

public class DatasetGen {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		//DatasetFactory.table1(1000000);
		//DatasetFactory.table2(1000000);
		DatasetFactory.skewTable1(100000);
		DatasetFactory.skewTable2(100000);
	}

}

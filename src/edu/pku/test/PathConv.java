package edu.pku.test;

public class PathConv {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		String[] outputPath = {"/home/hadoop/join/in/table1.seq", "/home/hadoop/join/in/table2.seq"};
		String[] newPath = new String[2];
		for (int i = 0; i < outputPath.length; i++) {
			newPath[i] = outputPath[i].replaceFirst(".seq$", ".txt");
			System.out.println(newPath[i]);
		}
		
	}

}

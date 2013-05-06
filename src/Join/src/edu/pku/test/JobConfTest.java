package edu.pku.test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

import org.apache.hadoop.mapred.JobConf;

public class JobConfTest {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		JobConf conf = new JobConf();
		conf.set("SB", "/home/hadoop/join/in/table1.txt");
		String t = new String(conf.get("SB"));
		File T1 = new File(t);
		try {
			BufferedReader br = new BufferedReader(new FileReader(T1));
			System.out.println(br.readLine());
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}

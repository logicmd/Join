package edu.pku.util;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;

public class DatasetFactory {
	public static final String[] course = { "Alogrithm", "Database",
			"SoftwareTesting", "NLP", "ComplexNetwork", "ParallelComputing",
			"DataMining", "GraphMining", "DSP", "ImageP", "Security",
			"Networking", "Optimazing", "MobileComputing", "ML",
			"InfoHiding", "AI", "3DRecons", "ComputerGraphics", 
			"DistributedCom", "WebServer", "iOSDev", "AndroidDev",
			"InfoRetriving", "FrontendDev", "WPDev", "VideoInfoP", "Multimedia",
			"Codec", "InfoExtract"};

	public static void table1(int num, String path) {
		PrintWriter pw = null;
		FileWriter writer = null;
		File file = null;
		StringBuffer sb = new StringBuffer();
		int a = 0;
		int initial = estimateSize(num) + 1;
		int n = initial + num - 1;
		boolean[] barr = new boolean[n];
		java.util.Random r = new java.util.Random();
		int j = 0, x = 0;
		for (int i = initial; i <= n; i++) {
			while (j < num) {
				x = r.nextInt(num);
				if (!barr[x]) {
					j++;
					barr[x] = true;
					break;
				}
			}
			sb.append(x + initial - 1);
			sb.append("\t");
			for (int k = 0; k < 5; k++) {
				a = (int) (96 + 27 * Math.random());
				while (true) {
					if (a > 96 & a < 123)
						break;
					else
						a = (int) (96 + 27 * Math.random());
				}
				sb.append((char) a);
			}

			sb.append("\n");

		}

		file = new File(path);

		try {
			writer = new FileWriter(file);
			pw = new PrintWriter(writer);

			pw.println(sb.toString());
			pw.flush();
			writer.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	public static void table1(int num) {
		table1(num, "/home/hadoop/join/in/dataset1.txt");
	}

	public static void table1() {
		table1(1000, "/home/hadoop/join/in/dataset1.txt");
	}

	public static void table2(int num, String path, int classTotalNum) {
		PrintWriter pw = null;
		FileWriter writer = null;
		File file = null;
		StringBuffer sb = new StringBuffer();
		int classNum = 0;
		int score = 0;
		int index = 0;
		int initial = estimateSize(num) + 1;
		int n = initial + num - 1;
		boolean[] barr = new boolean[n];
		java.util.Random r = new java.util.Random();
		int j = 0, x = 0;
		for (int i = initial; i <= n; i++) {
			while (j < num) {
				x = r.nextInt(num);
				if (!barr[x]) {
					j++;
					barr[x] = true;
					break;
				}
			}

			classNum = ((int) (99 * Math.random()) % classTotalNum) + 1;

			for (int jj = 0; jj < classNum; jj++) {
				if (jj == 0) {
					index = ((int) (99 * Math.random())) % classTotalNum;
				} else {
					index = (index + 1) % classTotalNum;
				}
				sb.append(x + initial - 1);
				sb.append("\t");
				sb.append(course[index]);
				sb.append("\t");
				score = (int) (60 + 40 * Math.random());
				sb.append(score);
				sb.append("\n");
			}

		}

		file = new File(path);

		try {
			writer = new FileWriter(file);
			pw = new PrintWriter(writer);

			pw.println(sb.toString());
			pw.flush();
			writer.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	public static void table2(int num) {
		table2(num, "/home/hadoop/join/in/dataset2.txt", 3);
	}

	public static void table2() {
		table2(1000, "/home/hadoop/join/in/dataset2.txt", 3);
	}
	
	//filename s for skew.
	public static void skewTable2(int num) {
		table2(num, "/home/hadoop/join/in/dataset2s.txt", 30);
	}

	public static void skewTable2() {
		table2(1000, "/home/hadoop/join/in/dataset2s.txt", 30);
	}
	
	//filename s for skew.
	public static void skewTable1(int num) {
		table1(num, "/home/hadoop/join/in/dataset1s.txt");
	}

	public static void skewTable1() {
		table1(1000, "/home/hadoop/join/in/dataset1s.txt");
	}


	public static int estimateSize(int num) {
		int threshold = 1;
		while (num / threshold > 0) {
			if (num / threshold / 10 == 0) {
				break;
			} else {
				threshold *= 10;
			}
		}
		if (num / threshold >= 9)
			return (threshold == 1) ? 10 : threshold * 10;
		else
			return (threshold == 1) ? 0 : threshold;
	}

}

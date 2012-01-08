package edu.pku.broadcast;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class BroadcastMapper extends MapReduceBase implements
		Mapper<LongWritable, Text, Text, Text> {

	File T1;
	HashMap<String, ArrayList<String>> ht = new HashMap<String, ArrayList<String>>();

	public void configure(JobConf conf) {
		
		T1 = new File(conf.get("TableOnePath"));

		BufferedReader br = null;
		String line = null;
		try {
			br = new BufferedReader(new FileReader(T1));
			while ((line = br.readLine()) != null) {
				String record[] = line.split("\t", 2);
				if (record.length == 2) {
					if (ht.containsKey(record[0])) {
						ht.get(record[0]).add(record[1]);
					} else {
						ArrayList<String> value = new ArrayList<String>();
						value.add(record[1]);
						ht.put(record[0], value);
					}
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void map(LongWritable LineNumber, Text value,
			OutputCollector<Text, Text> output, Reporter reporter)
			throws IOException {
		String[] rightRecord = value.toString().split("\t", 2);
		if (rightRecord.length == 2) {
			for (String leftRecord : ht.get(rightRecord[0])) {
				output.collect(new Text(rightRecord[0]), new Text(leftRecord
						+ "\t" + rightRecord[1]));
			}
		}
	}

}

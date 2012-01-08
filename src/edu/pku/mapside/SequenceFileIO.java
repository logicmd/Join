package edu.pku.mapside;

import java.io.BufferedReader;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;

public class SequenceFileIO {

	public static void write(String strInputPath, String strOutputPath) {
		String myreadline = null;
		FileReader fr = null;
		try {
			fr = new FileReader(strInputPath);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
		BufferedReader br = new BufferedReader(fr);

		Configuration conf = new Configuration();
		Path outputPath = new Path(strOutputPath);
		FileSystem fs = null;
		try {
			fs = FileSystem.get(URI.create(strOutputPath), conf);
		} catch (IOException e) {
			e.printStackTrace();
		}
		SequenceFile.Writer writer = null;
		
		BytesWritable key = new BytesWritable();
		Text value = new Text();
		try {
			writer = SequenceFile.createWriter(fs, conf, outputPath, key.getClass(),
					value.getClass());
		} catch (IOException e) {
			e.printStackTrace();
		}
		int i = 0;
		byte[] b = intToByteArray(i);
		try {
			while ((myreadline = br.readLine()) != null) {
				key.set(new BytesWritable(b));
				value.set(myreadline);
				writer.append(key, value);
				i++;
				b = intToByteArray(i);
			}
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			IOUtils.closeStream(writer);
		}
	}
	
	
	
	public static byte[] intToByteArray(int i) {
		byte[] result = new byte[4];
		result[0] = (byte) ((i >> 24) & 0xFF);
		result[1] = (byte) ((i >> 16) & 0xFF);
		result[2] = (byte) ((i >> 8) & 0xFF);
		result[3] = (byte) (i & 0xFF);
		return result;
	}

}

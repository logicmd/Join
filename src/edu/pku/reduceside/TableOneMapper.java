package edu.pku.reduceside;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import edu.pku.util.TableOneParser;
import edu.pku.util.TextPair;

public class TableOneMapper extends MapReduceBase implements
		Mapper<LongWritable, Text, TextPair, Text> {
	private TableOneParser parser = new TableOneParser();

	public void map(LongWritable key, Text value,
			OutputCollector<TextPair, Text> output, Reporter reporter)
			throws IOException {
		// String line = value.toString();
		// String values[] = line.split("\t");
		// output.collect(new TextPair(values[0], "0"), new Text(values[1]));
		if (parser.parse(value)) {
			output.collect(
					new TextPair(parser.getPrimaryKey(), parser.getTag()),
					new Text(parser.getContentColumn()));
		}
	}
}

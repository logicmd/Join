package edu.pku.reduceside;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import edu.pku.util.TableTwoParser;
import edu.pku.util.TextPair;

public class TableTwoMapper extends MapReduceBase implements
		Mapper<LongWritable, Text, TextPair, Text> {
	private TableTwoParser parser = new TableTwoParser();

	public void map(LongWritable key, Text value,
			OutputCollector<TextPair, Text> output, Reporter reporter)
			throws IOException {

		// String line = value.toString();
		// String values[] = line.split("\t");
		// String intendedValue = new String();
		// for(int i = 1; i < values.length; i++) {
		// intendedValue = intendedValue.concat(values[i]);
		// if(i != values.length) {
		// intendedValue = intendedValue.concat("\t");
		// }
		// }
		// output.collect(new TextPair(values[0], "1"), new
		// Text(intendedValue));
		if (parser.parse(value)) {
			output.collect(
					new TextPair(parser.getPrimaryKey(), parser.getTag()),
					new Text(parser.getContentColumn()));
		}
	}
}

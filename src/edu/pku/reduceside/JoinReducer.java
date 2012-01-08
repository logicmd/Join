package edu.pku.reduceside;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import edu.pku.util.TextPair;

public class JoinReducer extends MapReduceBase implements
		Reducer<TextPair, Text, Text, Text> {
	public void reduce(TextPair key, Iterator<Text> values,
			OutputCollector<Text, Text> output, Reporter reporter)
			throws IOException {

		Text stationName = new Text(values.next());
		while (values.hasNext()) {
			Text record = values.next();
			Text outValue = new Text(stationName.toString() + "\t"
					+ record.toString());
			output.collect(key.getFirst(), outValue);
		}
	}
}

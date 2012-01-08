package edu.pku.reduceside;
import java.util.Date;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Partitioner;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.lib.MultipleInputs;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import edu.pku.util.TextPair;

public class ReduceSideJoin extends Configured implements Tool {
	public static class KeyPartitioner implements Partitioner<TextPair, Text> {
		@Override
		public void configure(JobConf job) {
		}

		@Override
		public int getPartition(TextPair key, Text value, int numPartitions) {
			return (key.getFirst().hashCode() & Integer.MAX_VALUE)
					% numPartitions;
		}
	}

	@Override
	public int run(String[] args) throws Exception {
		if (args.length != 3) {
			System.err.println("ReduceSideJoin <table1 input> <table2 input> <output>");
			//JobBuilder.printUsage(this, "<table1 input> <table2 input> <output>");
			return -1;
		}
		JobConf conf = new JobConf(getConf(), getClass());
		conf.setJobName("Reduce Side Join using old API");

		Path tableOneInputPath = new Path(args[0]);
		Path tableTwoInputPath = new Path(args[1]);
		Path outputPath = new Path(args[2]);
		MultipleInputs.addInputPath(conf, tableOneInputPath, TextInputFormat.class,
				TableOneMapper.class);
		MultipleInputs.addInputPath(conf, tableTwoInputPath,
				TextInputFormat.class, TableTwoMapper.class);
		FileOutputFormat.setOutputPath(conf, outputPath);

		conf.setPartitionerClass(KeyPartitioner.class);
		conf.setOutputValueGroupingComparator(TextPair.FirstComparator.class);

		conf.setMapOutputKeyClass(TextPair.class);
		conf.setReducerClass(JoinReducer.class);

		conf.setOutputKeyClass(Text.class);

		Date startTime = new Date();
	    System.out.println("Job started: " + startTime);
		JobClient.runJob(conf);
		Date end_time = new Date();
	    System.out.println("Job ended: " + end_time);
	    System.out.println("The job took " + 
	        (end_time.getTime() - startTime.getTime()) /1000 + " seconds.");
		return 0;
	}

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new ReduceSideJoin(), args);
		System.exit(exitCode);

	}
}

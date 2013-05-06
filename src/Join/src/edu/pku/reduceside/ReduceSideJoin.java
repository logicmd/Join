package edu.pku.reduceside;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

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
		int num_maps = 1;
		int num_reduces = 1;
		boolean Default = false;
		// specified for input/output path
	    List<String> otherArgs = new ArrayList<String>(); 

		if (args.length < 3) {
			System.err
					.println("ReduceSideJoin [-m MapperNum] [-r ReducerNum] [-Default] <table1 input> <table2 input> <output>");
			// JobBuilder.printUsage(this,
			// "<ncdc input> <station input> <output>");
			return -1;
		} else {
			for (int i = 0; i < args.length; ++i) {
				try {
					if ("-m".equals(args[i])) {
						num_maps = Integer.parseInt(args[++i]);
					} else if ("-r".equals(args[i])) {
						num_reduces = Integer.parseInt(args[++i]);
					} else if ("-Default".equals(args[i])) {
						Default = true;
					} else {
						otherArgs.add(args[i]);
					}
				} catch (NumberFormatException except) {
					System.err.println("ERROR: Integer expected instead of "
							+ args[i]);
					// return printUsage();
					return -1;
				} catch (ArrayIndexOutOfBoundsException except) {
					System.err
							.println("ERROR: Required parameter missing from "
									+ args[i - 1]);
					return -1;
					// return printUsage(); // exits
				}
			}
		}

		JobConf conf = new JobConf(getConf(), getClass());
		conf.setJobName("Reduce Side Join using old API");

		// Set user-supplied (possibly default) job configs
		if (!Default) {
			conf.setNumMapTasks(num_maps);
			conf.setNumReduceTasks(num_reduces);
			System.out.println("Maunal mode is enable, mapper num: " + num_maps + " reducer num: " + num_reduces);
		} else {
			System.out.println("Default mode is enable, mapper and reduce num is determined by system.");
		}
		
		Path tableOneInputPath = new Path(otherArgs.get(0).toString());
		Path tableTwoInputPath = new Path(otherArgs.get(1).toString());
		Path outputPath = new Path(otherArgs.get(2).toString());
		
		//Path tableOneInputPath = new Path(args[0]);
		//Path tableTwoInputPath = new Path(args[1]);
		//Path outputPath = new Path(args[2]);
		MultipleInputs.addInputPath(conf, tableOneInputPath,
				TextInputFormat.class, TableOneMapper.class);
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
		System.out.println("The job took "
				+ (end_time.getTime() - startTime.getTime()) / 1000
				+ " seconds.");
		return 0;
	}

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new ReduceSideJoin(), args);
		System.exit(exitCode);

	}
}

/**
 * 
 */
package edu.pku.reducesidenew;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import edu.pku.util.TableOneParser;
import edu.pku.util.TableTwoParser;
import edu.pku.util.TextPair;

/**
 * @author KevinTong
 * 
 */
public class ReduceSideJoinNew {

	/**
	 * @author logicmd
	 * 
	 * @category The function of the mapper is to make the [matching column] in
	 *           front as intermediate key, meanwhile adding [tag] right after
	 *           the intermediate key. The tag is corresponding to the table
	 *           number.
	 */

	public static class ReduceSideMapper extends
			Mapper<LongWritable, Text, TextPair, Text> {

		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			// Get the input pathname and filename
			String pathName = ((FileSplit) context.getInputSplit()).getPath()
					.toString();
			if (pathName.contains("1.txt")) {
				TableOneParser parser = new TableOneParser();
				if (parser.parse(value)) {
					context.write(
							new TextPair(parser.getPrimaryKey(), parser
									.getTag()),
							new Text(parser.getContentColumn()));
				}
			} else if (pathName.contains("2.txt")) {
				TableTwoParser parser = new TableTwoParser();
				if (parser.parse(value)) {
					context.write(
							new TextPair(parser.getPrimaryKey(), parser
									.getTag()),
							new Text(parser.getContentColumn()));
				}
			}
		}

	}

	/**
	 * @author logicmd
	 * 
	 *         The function of Partitioner is to partition the tuples with the
	 *         same matching column to the same reducer.
	 */

	public static class ReduceSidePartitionner extends
			Partitioner<TextPair, Text> {
		public int getPartition(TextPair key, Text value, int numParititon) {
			return Math.abs(key.getFirst().hashCode() * 127) % numParititon;
		}
	}

	/**
	 * @author logicmd
	 * 
	 *         The function of Comparator
	 */

	public static class ReduceSideComparator extends WritableComparator {
		public ReduceSideComparator() {
			super(TextPair.class, true);
		}

		@SuppressWarnings("rawtypes")
		public int compare(WritableComparable a, WritableComparable b) {
			TextPair t1 = (TextPair) a;
			TextPair t2 = (TextPair) b;
			return t1.getFirst().compareTo(t2.getFirst());
		}
	}

	public static class ReduceSideReducer extends
			Reducer<TextPair, Text, Text, Text> {
		protected void reduce(TextPair key, Iterable<Text> values,
				Context context) throws IOException, InterruptedException {
			Text pid = key.getFirst();
			String tableOneColumn = values.iterator().next().toString();
			while (values.iterator().hasNext()) {
				context.write(pid, new Text(tableOneColumn + "\t"
						+ values.iterator().next().toString()));
			}
		}
	}

	public static void main(String args[]) throws IOException,
			InterruptedException, ClassNotFoundException {
		int num_reduces = 1;
		boolean Default = false;
		// specified for input/output path
	    List<String> IOArgs = new ArrayList<String>(); 

		Configuration conf = new Configuration();
		GenericOptionsParser parser = new GenericOptionsParser(conf, args);
		String[] otherArgs = parser.getRemainingArgs();
		if (args.length < 3) {
			System.err
					.println("Usage: ReduceSideJoinNew [-m MapperNum] [-r ReducerNum] [-Default] <Table_one_path> <Table_two_path> <output>");
			System.exit(2);
		} else {
			for (int i = 0; i < args.length; ++i) {
				try {
					if ("-r".equals(args[i])) {
						num_reduces = Integer.parseInt(args[++i]);
					} else if ("-Default".equals(args[i])) {
						Default = true;
					} else {
						IOArgs.add(args[i]);
					}
				} catch (NumberFormatException except) {
					System.out.println("ERROR: Integer expected instead of "
							+ args[i]);
					// return printUsage();
					System.exit(2);
				} catch (ArrayIndexOutOfBoundsException except) {
					System.out
							.println("ERROR: Required parameter missing from "
									+ args[i - 1]);
					System.exit(2);
					// return printUsage(); // exits
				}
			}
			// conf.set("hadoop.job.ugi", "root,hadoop");
			Job job = new Job(conf, "ReduceSideJoinNew");
			// Set job
			if(!Default) {
				job.setNumReduceTasks(num_reduces);
				System.out.println("Maunal mode is enable,"+ " reducer num: " + num_reduces);
			} else {
				System.out.println("Default mode is enable, mapper and reduce num is determined by system.");
			}
			job.setJarByClass(ReduceSideJoinNew.class);
			// Set mapper
			job.setMapperClass(ReduceSideMapper.class);
			// Set mapper output
			job.setMapOutputKeyClass(TextPair.class);
			job.setMapOutputValueClass(Text.class);
			// Set partitioner
			job.setPartitionerClass(ReduceSidePartitionner.class);
			// Set grouping condition after partitioning
			job.setGroupingComparatorClass(ReduceSideComparator.class);
			// Set reducer
			job.setReducerClass(ReduceSideReducer.class);
			// Set reducer output
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			// Set input path and output path
			FileInputFormat.addInputPath(job, new Path(IOArgs.get(0).toString()));
			FileInputFormat.addInputPath(job, new Path(IOArgs.get(1).toString()));
			FileOutputFormat.setOutputPath(job, new Path(IOArgs.get(2).toString()));
			//FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
			//FileInputFormat.addInputPath(job, new Path(otherArgs[1]));
			//FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));
			// Execution, wait for the job to complete.
			Date startTime = new Date();
			System.out.println("Job started: " + startTime);
			boolean completeFlag = job.waitForCompletion(true);

			// System.exit(job.waitForCompletion(true) ? 0 : 1);
			if (completeFlag == true) {

				Date end_time = new Date();
				System.out.println("Job ended: " + end_time);
				System.out.println("The job took "
						+ (end_time.getTime() - startTime.getTime()) / 1000
						+ " seconds.");
				System.exit(0);
			} else {
				System.exit(1);

			}
		}
	}
}

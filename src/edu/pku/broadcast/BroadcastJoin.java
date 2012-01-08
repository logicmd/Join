package edu.pku.broadcast;


import java.io.IOException;
import java.util.Date;


import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;


public class BroadcastJoin {

	/**
	 * @param args
	 */
	public static void main(String[] args) throws IOException{
		// TODO Auto-generated method stub
		if (args.length != 3) {
			System.err.println("BroadcastJoin <table1 input> <table2 input> <output>");
			System.exit(-1);
		}
		JobConf conf = new JobConf(BroadcastJoin.class);
		conf.setJobName("Broadcast Join");
		conf.set("TableOnePath", args[0]);
		FileInputFormat.addInputPath(conf, new Path(args[1]));
		FileOutputFormat.setOutputPath(conf, new Path(args[2]));
		conf.setMapperClass(BroadcastMapper.class);
		
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);

		Date startTime = new Date();
	    System.out.println("Job started: " + startTime);
		JobClient.runJob(conf);
		Date end_time = new Date();
	    System.out.println("Job ended: " + end_time);
	    System.out.println("The job took " + 
	        (end_time.getTime() - startTime.getTime()) /1000 + " seconds.");
	    
	}

}

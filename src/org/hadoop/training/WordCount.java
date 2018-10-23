package org.hadoop.training;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class WordCount extends Configured implements Tool{



	@Override
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		if(args.length<2)
		{
			System.out.println("You gave wrong number of input parameters.\n Please write the input file and output file directories properly\n");
			return -1;
		}
		JobConf conf = new JobConf(WordCount.class);
		FileInputFormat.setInputPaths(conf,new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));
		conf.setMapperClass(WordMapper.class);
		conf.setReducerClass(WordReducer.class);
		
		conf.setMapOutputKeyClass(org.apache.hadoop.io.Text.class);
		conf.setMapOutputValueClass(IntWritable.class);
		
		JobClient.runJob(conf);
		
		return 0;
	}
	public static void main(String args[]) {
		int exitCode = 0;
		try {
			exitCode = ToolRunner.run(new WordCount(), args);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		System.exit(exitCode);
	}

}

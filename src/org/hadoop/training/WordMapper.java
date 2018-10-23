package org.hadoop.training;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class WordMapper extends MapReduceBase implements Mapper<LongWritable,Text,Text,IntWritable> {



	@Override
	public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter ar)
			throws IOException {
		// TODO Auto-generated method stub
		String words=value.toString();
		for(String  word:words.split(" ")) {
			if(word.length()>0) {
				output.collect(new Text(word),new IntWritable(1));
			}
		}
		
	}
}

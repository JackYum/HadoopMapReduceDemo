package com.atguigu.mapreduce.z_extendedcase.c_findblogsamefriends.solution04;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class FriendsDriver {

	public static void main(String[] args) throws Exception{
		
		args = new String[] {"D:\\mr\\input\\friends","D:\\mr\\output16"};
		
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		
		job.setJarByClass(FriendsDriver.class);
		
		job.setMapperClass(FriendsMapper.class);
		
		job.setMapOutputKeyClass(Text.class);
		
		job.setMapOutputValueClass(Text.class);
		
		job.setOutputKeyClass(Text.class);
		
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.setNumReduceTasks(0);
		
		job.waitForCompletion(true);
	}

}

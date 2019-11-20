package com.atguigu.mapreduce.a_overview.wordcount2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class TestWordCountDriver {
    public static void main(String[] args) throws Exception {

        //1. job
        Job job = Job.getInstance(new Configuration());

        //2. two class
        job.setJarByClass(String.class);

        job.setMapperClass(WordCountDriver.WordCountMapper.class);
        job.setReducerClass(WordCountDriver.WordCountReducer.class);

        //3. path
	job.setMapOutputKeyClass(Text.class);
	job.setMapOutputValueClass(IntWritable.class);

	job.setOutputKeyClass(Text.class);
	job.setOutputValueClass(Integer.class);

	FileInputFormat.setInputPaths(job,new Path("/Users/jiangren/Documents/便笺/1.txt"));
	FileOutputFormat.setOutputPath(job, new Path("/Users/jiangren/Documents/便笺/1-2.txt"));

	job.waitForCompletion(true);
    }
}

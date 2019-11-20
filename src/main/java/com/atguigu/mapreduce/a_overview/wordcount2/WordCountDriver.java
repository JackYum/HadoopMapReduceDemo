package com.atguigu.mapreduce.a_overview.wordcount2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;


public class WordCountDriver {
    public static void main(String[] args) throws Exception {

        //1. job
        Job job = Job.getInstance(new Configuration());

        //2. two class
        job.setJarByClass(WordCountDriver.class);

        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);

        //3. path
	job.setMapOutputKeyClass(Text.class);
	job.setMapOutputValueClass(IntWritable.class);
	//job.setCombinerClass(WordCountReducer.class);

	job.setOutputKeyClass(Text.class);
	job.setOutputValueClass(Integer.class);

	FileInputFormat.setInputPaths(job,new Path("/Users/jiangren/Documents/便笺/1.txt"));
	FileOutputFormat.setOutputPath(job, new Path("/Users/jiangren/Documents/便笺/1-2.txt"));

	job.waitForCompletion(true);
    }

    public static class WordCountMapper extends Mapper<LongWritable,Text, Text, IntWritable> {
	public WordCountMapper() {
	}

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
	    //1. get one line contain
	    String line = value.toString();
	    //2. split
	    String[] words = line.split(" ");
	    //3. write
	    for (String word : words) {
		context.write(new Text(word),new IntWritable(1));
	    }

	}
    }

    public static class WordCountReducer extends Reducer<Text, IntWritable,Text,IntWritable> {
	@Override
	protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
	    //1. get sum
	    int sum = 0;
	    for (IntWritable value : values) {
		sum += value.get();
	    }
	    //2. write
	    context.write(key,new IntWritable(sum));
	}
    }

}

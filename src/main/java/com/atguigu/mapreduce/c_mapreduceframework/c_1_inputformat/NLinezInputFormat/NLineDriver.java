package com.atguigu.mapreduce.c_mapreduceframework.c_1_inputformat.NLinezInputFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;


//3. Driver
public class NLineDriver {
    public static void main(String[] args) throws Exception {
	args = new String[] {"/Users/jiangren/Documents/便笺/1.txt"
		,"/Users/jiangren/Documents/便笺/1-1.txt"};
	Configuration conf = new Configuration();
	Job job = Job.getInstance(conf);

	job.setJarByClass(NLineDriver.class);
	job.setMapperClass(NLineMapper.class);
	job.setReducerClass(NLineReducer.class);

	job.setMapOutputKeyClass(Text.class);
	job.setMapOutputValueClass(LongWritable.class);
	job.setOutputKeyClass(Text.class);
	job.setOutputValueClass(LongWritable.class);

	job.setInputFormatClass(NLineInputFormat.class);
	NLineInputFormat.setNumLinesPerSplit(job, 2);

	FileInputFormat.setInputPaths(job,new Path(args[0]));
	FileOutputFormat.setOutputPath(job,new Path(args[1]));

	job.waitForCompletion(true);

    }

    //1. Mapper
    public static class NLineMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
	    //得到一行
	    String line = value.toString();
	    String[] words = line.split(" ");
	    //循环写出
	    for (String word : words) {
		context.write(new Text(word), new LongWritable(1));
	    }
	}
    }

    //2. Reducer
    public static class NLineReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
	@Override
	public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
	    long sum = 0L;
	    for (LongWritable value : values) {
		sum += value.get();
	    }
	    context.write(key, new LongWritable(sum));
	}
    }
}



package com.atguigu.mapreduce.a_overview.wordcount2;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class WordCountReducerBak extends Reducer<Text, IntWritable,Text,IntWritable> {
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

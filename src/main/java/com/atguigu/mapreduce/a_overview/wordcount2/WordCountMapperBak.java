package com.atguigu.mapreduce.a_overview.wordcount2;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class WordCountMapperBak extends Mapper<LongWritable,Text, Text, IntWritable> {
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

package com.atguigu.mapreduce.findcommonfriends.solution02;

import java.io.IOException;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

public class CommonFriendInputFormat extends FileInputFormat<Text, BytesWritable>{

	@Override
	public RecordReader<Text, BytesWritable> createRecordReader(InputSplit split, TaskAttemptContext context)
			throws IOException, InterruptedException {
	
		CommonFriendRecordReader recordReader = new CommonFriendRecordReader(); 
		return recordReader;
	}

}

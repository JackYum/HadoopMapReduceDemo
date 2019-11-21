package com.atguigu.mapreduce.findcommonfriends.group5.solution02;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MyReducer extends Reducer<Text, Text, Text, Text> {

	@Override
	protected void reduce(Text key, Iterable<Text> values, 
			Reducer<Text, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		//以key值归并，values遍历拼接。
		String value = "";
		for (Text text : values) {
			value = value + text.toString(); 
		}
		context.write(key, new Text(value));
	}
	
}

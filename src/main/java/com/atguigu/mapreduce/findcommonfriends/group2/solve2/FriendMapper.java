package com.lcb.commonfriend2;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class FriendMapper extends Mapper<LongWritable, Text, Text, Text>{
	
	private Text k = new Text();
	private Text v = new Text();
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		String line = value.toString();
		String[] split = line.split(":");
		if(split.length < 2) { //排除空行
			return;
		}
		//取出用户名
		String user = split[0];
		boolean flag = true;
		for (int i = 0; i < 26; i++) {
			String compare = Character.toString((char)(i+'A'));
			if(compare.equals(user) ) {
				flag = false;
				continue;
			}
			if(flag) {
				k.set(compare + "-" + user);
			}else {
				k.set(user + "-" + compare);
			}
			v.set(split[1]);
			context.write(k, v);
		}
	}
}

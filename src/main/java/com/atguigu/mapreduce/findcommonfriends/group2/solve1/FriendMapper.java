package com.lcb.commonfriend;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class FriendMapper extends Mapper<LongWritable, Text, Text, Text>{
	
	private int[][] arr = new int[26][26];
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
		char user = split[0].charAt(0);
		//取出好友列
		String[] friends = split[1].split(",");
		for (int i = 0; i < friends.length; i++) {
			char friend = friends[i].charAt(0);
			//有好友关系标记为1
			arr[user-'A'][friend-'A'] = 1;
		}
	}
	
	@Override
	protected void cleanup(Mapper<LongWritable, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		for (int i = 0; i < arr.length - 1; i++) {
			for (int j = i+1; j < arr.length; j++) {
				String k_temp = Character.toString( (char)( i +'A') ) + "-" + 
							Character.toString( (char)( j +'A') );
				String v_temp = "" ;
				for(int k=0; k < arr.length; k++) {
					if( (arr[i][k] & arr[j][k]) == 1) {
						v_temp += Character.toString( (char)(k+'A') ) + " ";
					}
				}
				if( ! "".equals(v_temp)) {
					k.set(k_temp);
					v.set(v_temp);
					context.write(k, v);
				}
			}
		}
	}
}

package com.lcb.commonfriend2;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class FriendReducer extends Reducer<Text, Text, Text, Text>{
	
	private Text v = new Text();
	
	@Override
	protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		int count = 0;
		String[] vs = new String[2];
		for (Text text : values) {
			vs[count] = text.toString();
			count++;
		}
		if(count >= 2) {	//表明两个用户都存在
			String[] u1 = vs[0].split(",");
			String[] u2 = vs[1].split(",");
			String v_temp = "";
			for (int i = 0; i < u1.length; i++) {
				for (int j = 0; j < u2.length; j++) {
					if( u1[i].equals(u2[j]) ) {
						v_temp += u1[i] + " ";
					}
				}
			}
			if(!"".equals(v_temp)) { //表明有共同好友
				v.set(v_temp);
				context.write(key,v);
			}
		}
	}
}

package com.atguigu.mapreduce.findcommonfriends.group1.solution02;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class CommonFriendReducer extends Reducer<Text, Text, Text, Text>{

	Text v = new Text();
	
	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
	
		for (Text value : values) {
			
            String[] split = value.toString().split(",");
            List<String> strings = Arrays.asList(split);
            HashSet<String> set = new HashSet<>();
            for (String str : strings) {
            	
                if (strings.indexOf(str) != strings.lastIndexOf(str)) {
                    set.add(str);
                }
            }
            if (set.size() != 0) {
            	
                v.set(set.toString());
                context.write(key, v);
            }
        }
		
	}
}

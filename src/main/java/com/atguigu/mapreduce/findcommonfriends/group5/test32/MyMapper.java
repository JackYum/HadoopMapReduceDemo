package com.examples.test32;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MyMapper extends Mapper<LongWritable, Text, Text, Text> {

	HashMap<String, ArrayList<String>> hmap = new HashMap<>();//key-value

	@Override
	protected void cleanup(Mapper<LongWritable, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		//value两两组合，当成key；原有key作为value传给reduce
		for (String key : hmap.keySet()) {
			ArrayList<String> alist = hmap.get(key);
			for (int i = 0; i < alist.size() - 1; i++) {
				for (int j = i + 1; j < alist.size(); j++) {
					context.write(new Text(alist.get(i) + "-" + alist.get(j)), new Text(key));
				}
			}
		}
	}

	@Override
	protected void map(LongWritable key, Text value, 
			Mapper<LongWritable, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		//获取中间变量：即a被那些人当成好友。。。存入hmap
		String[] fields = value.toString().split(":");
		if (fields.length > 1) {
			String[] twoFields = fields[1].split(",");
			for (int i = 0; i < twoFields.length; i++) {
				ArrayList<String> aList = hmap.get(twoFields[i]);
				if (aList == null) {
					aList = new ArrayList<>();
				}
				aList.add(fields[0]);
				hmap.put(twoFields[i], aList);
			}
		}
	}
}

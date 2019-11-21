package com.atguigu.mapreduce.findcommonfriends.group5.solution01;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.TreeMap;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MyMapper extends Mapper<LongWritable, Text, Text, Text> {

	HashMap<String, ArrayList<String>> hmap = new HashMap<>();
	TreeMap<String, ArrayList<String>> lastmap = new TreeMap<>();

	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		String[] fields = value.toString().split(":");
		if (fields.length > 1) {
			String[] fields2 = fields[1].split(",");
			ArrayList<String> arrayList = new ArrayList<>();
			for (String string : fields2) {
				arrayList.add(string);
			}
			hmap.put(fields[0], arrayList);
		}
	}

	@Override
	protected void cleanup(Mapper<LongWritable, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		for (String key : hmap.keySet()) {
			for (String key2 : hmap.keySet()) {
				if (!key.equals(key2)) {
					String finalkey = "";
					if (key.compareTo(key2) < 0) {
						finalkey = key +"-"+key2;
					} else {
						finalkey = key2 +"-"+ key;
					}
					ArrayList<String> arrayList1 = (ArrayList<String>) hmap.get(key).clone();
					ArrayList<String> arrayList2 = (ArrayList<String>) hmap.get(key2).clone();
					arrayList1.retainAll(arrayList2);
					lastmap.put(finalkey, arrayList1);
				}
			}
		}
		for (String key : lastmap.keySet()) {
			System.out.println("key:" + key);
			// System.out.println(lastmap.get(key).toString());
			String line = "";
			if (lastmap.get(key).size() != 0) {
				for (String lin : lastmap.get(key)) {
					line = line + lin;
				}
				System.out.println(line);
				context.write(new Text(key), new Text(line));
			}
		}
	}

}

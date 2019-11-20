package com.atguigu.mapreduce.findcommonfriends.solution01;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.*;

/**
 * @author jiangren
 */
public class OneShareFriendsMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

    private TreeMap<String, String[]> personFriends = new TreeMap<>();

    @Override
    protected void map(LongWritable key, Text value, Context context) {

	// 1 获取一行 A:B,C,D,F,E,O
	String line = value.toString();

	// 2 切割
	String[] fields = line.split(":");

	// 3 获取person和好友friends
	String person = fields[0];
	String[] friends = fields[1].split(",");

	// 4 将person和好友friends放到TreeMap中
	personFriends.put(person, friends);
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {

	//将TreeMap的key转换成String数组，由于TreeMap的key有序，所以得到的是有序数组
	String[] persons = new String[personFriends.size()];
	persons = personFriends.keySet().toArray(persons);

	// 5 遍历treeMap集合，
	for (int i = 0; i < persons.length; i++) {
	    for (int j = i + 1; j < persons.length; j++) {

		String result = persons[i] + "-" + persons[j] + "\t";

		String[] friends1 = personFriends.get(persons[i]);
		String[] friends2 = personFriends.get(persons[j]);
		String sameFriends = intersection(friends1, friends2);
		if (sameFriends.length() > 0) {
		    result += sameFriends;
		    context.write(new Text(result), NullWritable.get());
		}

	    }
	}
    }

    /**
     * 取两个字符串的交集，时间复杂度为O(n)
     *
     * @param string1 数组1
     * @param string2 数组2
     * @return 以字符串的形式返回交集，如:{"A", "E", "D", },{"C", "E", "D", }，返回"E D"
     */
    private String intersection(String[] string1, String[] string2) {
	Set<String> set1 = new HashSet<>();
	Collections.addAll(set1, string1);
	StringBuilder intersect = new StringBuilder();
	for (String s : string2) {
	    if (set1.contains(s)) {
		intersect.append(s).append(" ");
	    }
	}
	return intersect.toString();
    }
}
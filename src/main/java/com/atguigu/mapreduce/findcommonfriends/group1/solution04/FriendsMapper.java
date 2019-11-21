package com.atguigu.mapreduce.findcommonfriends.group1.solution04;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Set;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class FriendsMapper extends Mapper<LongWritable, Text, Text, Text>{

	//存放人的集合。例如:[A,B,C,D,...,O]
	ArrayList<String> personList = new ArrayList<>();
	//存放某个人的好友的集合，例如:[[B,C,D,F,E,O],[A,C,E,K],...,[A,H,I,J]]
	ArrayList<ArrayList<String>> friendsList = new ArrayList<>();
	//存放最终的每个人和每个人对应的好友的Map集合
	LinkedHashMap<String,ArrayList<String>> maps = new LinkedHashMap<>();
	
	//最终写出的key
	Text k = new Text();
	//最终写出的value
	Text v = new Text();

	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		
		//A:B,C,D,F,E,O
		String line = value.toString();
		//切割字符串，第一个作为人，后面的作为人的好友，例如第一行
		//A:B,C,D,F,E,O。：前面的A作为人，:后面的作为A的好友
		String[] splits = line.split(":");
		//将人放入personList集合中
		personList.add(splits[0]);
		//用,切割，切割后的就是人的所有好友的数组
		String[] splits2 = splits[1].split(",");
		ArrayList<String> list = new ArrayList<>();
		//将好友数组中的元素放入list集合中
		for (String str : splits2) {
			list.add(str);
		}
		//将list集合放入friendsList集合
		friendsList.add(list);
	}
	
	@Override
	protected void cleanup(Context context)
			throws IOException, InterruptedException {
		//遍历集合personList中的元素，并且将personList中的元素两两组合
		for(int i = 0;i < personList.size();i++) {
			for(int j = i+1;j < personList.size();j++) {
				//设置一个标志位，一开始设置为false，表示两个人中没有好友，如果后续的计算中，两个人有共同好友，则设置为true
				//只有当两个人有共同好友，即flag为true时，才将他俩以及他们的共同好友放入Map集合中
				boolean flag = false;
				//用-来拼接两个人
				String s = personList.get(i) + "-" + personList.get(j);
				//这个集合存放两个人的共同好友
				ArrayList<String> list = new ArrayList<>();
				//从friendsList中第一个人的索引对应的他的好友，和第二个人中对应的好友，找出他俩的共同好友，并放入list集合中
				for (String str : friendsList.get(i)) {
					if(friendsList.get(j).contains(str)) {
						list.add(str);
						flag = true;
					}
				}
				//如果他俩有共同好友，就放入Map中。否则，不放入
				if(flag) {
					maps.put(s, list);
				}
			}
		}
		
		//将Map中的内容写出
		Set<String> keySet = maps.keySet();
		for (String string : keySet) {
			k.set(string);
			v.set(maps.get(string).toString());
			context.write(k, v);
		}
	}
}

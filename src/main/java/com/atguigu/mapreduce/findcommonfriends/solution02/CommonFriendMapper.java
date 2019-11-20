package com.atguigu.mapreduce.findcommonfriends.solution02;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CommonFriendMapper extends Mapper<Text, BytesWritable, Text, Text>{

	 private Text k = new Text();
	 private Text v = new Text();
	
	@Override
	protected void map(Text key, BytesWritable value, Context context)
			throws IOException, InterruptedException {

		String data=new String(value.copyBytes());
        Map<String,String> map=new HashMap<>();
        List<String> list=new ArrayList<>();
        String[] strs = data.split("\r\n");
        for (String str : strs) {
            String[] splits = str.split(":");
            map.put(splits[0],splits[1]);
            list.add(splits[0]);
        }
        for(int i=0;i<list.size() - 1;i++){
            for(int j=i+1;j<list.size();j++){
                k.set(list.get(i)+"-"+list.get(j));
                v.set(map.get(list.get(i))+","+map.get(list.get(j)));
                context.write(k,v);
            }
        }
	}
}

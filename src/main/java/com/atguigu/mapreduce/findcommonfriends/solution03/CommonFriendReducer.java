package com.atguigu.mapreduce.findcommonfriends.solution03;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;

public class CommonFriendReducer extends Reducer<Text, Text, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        // set 去重
      /*  Set<String> set = new HashSet<>();
        for (Text value : values) {
            String[] split = value.toString().split(",");
            for (String s : split) {
                set.add(s);
            }
        }
        String substring = set.toString().substring(1, set.toString().length()-1);
        context.write(key, new Text(substring));*/
        String s = "";
        for (Text value : values) {
            s = value.toString();
        }
        context.write(key, new Text(s));
    }
}


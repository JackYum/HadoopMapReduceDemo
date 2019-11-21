package com.wm.mapreduce.solve;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * 什么也不做
 *
 * @author 99497
 */
public class FriendsReducer extends Reducer<Text, Text, Text, Text> {

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        context.getCounter("reduce", "count").increment(1);

        for (Text value : values) {

            // 对字符串添加指定分隔符(此处为" ")
            // String join = getString(value);

            // 再写出去
            context.write(key, new Text(value));
        }
    }


    /**
     * 添加分隔符的辅助方法 留存
     */
    private String getString(Text value) {
        char[] chars = value.toString().toCharArray();
        String[] strings = new String[chars.length];

        for (int i = 0; i < chars.length; i++) {
            strings[i] = String.valueOf(chars[i]);
        }

        return String.join(",", strings);
    }
}
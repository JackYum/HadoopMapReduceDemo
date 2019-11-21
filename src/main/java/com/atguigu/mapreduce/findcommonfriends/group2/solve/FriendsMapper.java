package com.wm.mapreduce.solve;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.*;

/**
 * Mapper
 *
 * @author 99497
 */
public class FriendsMapper extends Mapper<LongWritable, Text, Text, Text> {

    /**
     * 存放最终结果 用于写出到reduce阶段
     * TreeMap 使键保持字母序
     */
    private Map<String, String> resultMap = new TreeMap<>();

    /**
     * 把冒号后的字符串变成这个人拥有好友的List
     */
    private Map<String, List<String>> maps = new HashMap<>(16);


    @Override
    protected void map(LongWritable key, Text value, Context context) {

        // 获取一行 A:B,C,D,F,E,O
        String friends = value.toString();

        String[] split = friends.split(":");
        maps.put(split[0], stringToList(split[1]));

    }


    /**
     * map任务结束时 调用一次
     */
    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {

        Map<String, String> result = findFriends(maps);

        for (Map.Entry<String, String> entry : result.entrySet()) {
            String k = entry.getKey();
            String v = entry.getValue();

            // System.out.println(k + "\t" + v);

            // 此时写出去的 k-v 格式  <A-B>  <C E>
            context.write(new Text(k), new Text(v));
        }

    }

    private Map<String, String> findFriends(Map<String, List<String>> maps) {

        // 复制一个map 用于辅助
        Map<String, List<String>> helpMap = new HashMap<>(maps);

        // 核心遍历
        maps.forEach((person1, directs) -> {

            // 每个人的 一度好友  TODO (n度好友功能)
            Map<String, Integer> firstOut = new HashMap<>(16);

            for (String str : directs) {
                // 一度好友 赋值使不为空
                firstOut.put(str, 0);
            }

            helpMap.remove(person1);

            for (Map.Entry<String, List<String>> entry : helpMap.entrySet()) {
                for (String friend : entry.getValue()) {
                    // 得到的为 [B, C, D, F, E, O] 这种结构
                    Integer value = firstOut.get(friend);
                    if (value != null) {
                        String person2 = entry.getKey();
                        // 拼接字符串
                        resultMap.merge(person1 + "-" + person2, friend, String::concat);
                    }
                }
            }
        });

        return resultMap;

    }

    //  (o, n) -> o + "   " + n

    /**
     * 辅助方法 字符串变List列表
     */
    private List<String> stringToList(String str) {
        String[] splits = str.split(",");
        return new ArrayList<>(Arrays.asList(splits));
    }
}

package com.atguigu.mapreduce.findcommonfriends.group1.solution03;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class CommonFriendMapper extends Mapper<LongWritable,Text,Text,Text> {
	//创建一个全局map 存储缓存表中的数据
    private Map<String,String> map = Maps.newHashMap();
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        //从缓存中取出流
        URI[] files = context.getCacheFiles();
        String path = files[0].getPath();
        //缓存流读取
        BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(path), "UTF-8"));
        String line;
        //循环读取每行数据，拆分 放入 一个map中0
        while (StringUtils.isNotEmpty(line = br.readLine())){
            //按 ：进行拆分
        	String[] split = line.split(":");
           
            //所有人作为key，好友列表作为value
            map.put(split[0],split[1]);
        }
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String line = value.toString();
        //把value中的每行数据按 ： 拆分
        String[] lineArr = line.split(":");
        List<String> lineList = Lists.newArrayList();
        //把InputFormat 中读取的数据 进行拆分 ，放入一个集合中，为求 交集做准备
        String[] friends = lineArr[1].split(",");
     
        for (String friend : friends) { 
             lineList.add(friend);
        }
        //迭代map 进行比对
        Iterator<Map.Entry<String, String>> entries = map.entrySet().iterator();
       //遍历map整张表所有数据
        while (entries.hasNext()) {
            Map.Entry<String, String> entry = entries.next();
            //若是所有人与当前读取数据的key相同则跳出不进行比对
            if (!lineArr[0].equals(entry.getKey())) {
                //new 一个临时 list 存储当前 key的 值
                List<Object> temList = Lists.newArrayList();
                String[] val = entry.getValue().split(",");
                for (String s : val) {
                    temList.add(s);
                }
                //放入集合取交集
                temList.retainAll(lineList);
                //＞0 有交集则写出
                if (temList.size()>0) {
                    String join = StringUtils.join(temList, ",");
                    //设置key  调整顺序 如 A-K 与 K-A 调整至A-K
                    char c = lineArr[0].charAt(0);
                    char c1 = entry.getKey().charAt(0);
                    //分条件写出
                    String k;
                    //排序
                    if (c<c1) {
                        k=lineArr[0]+"-"+entry.getKey();
                    }else {
                        k=entry.getKey()+"-"+lineArr[0];
                    }
                    context.write(new Text(k),new Text(join));
                }
            }
        }
    }
}


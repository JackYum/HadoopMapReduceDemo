package com.atguigu.mapreduce.a_overview.wordcount1;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WordcountMapper extends Mapper<LongWritable, Text, Text, IntWritable>{

    Text k = new Text();

    IntWritable v = new IntWritable(1);

    /**
     *
     * @param key 文件读取位置的偏移量
     * @param value 读取到的一行数据内容
     * @param context 上下文工具，协调mapper执行的
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void map(LongWritable key, Text value, Context context)	throws IOException, InterruptedException {

	// 1 获取一行
	String line = value.toString();

	// 2 切割
	String[] words = line.split(" ");

	// 3 输出
	for (String word : words) {

	    k.set(word);
	    context.write(k, v);
	}
    }
}
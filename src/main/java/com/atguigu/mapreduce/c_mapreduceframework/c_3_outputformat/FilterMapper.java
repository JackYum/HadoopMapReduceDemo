package com.atguigu.mapreduce.c_mapreduceframework.c_3_outputformat;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class FilterMapper extends Mapper<LongWritable, Text, Text, NullWritable>{

    @Override
    protected void map(LongWritable key, Text value, Context context)	throws IOException, InterruptedException {

	// 写出
	context.write(value, NullWritable.get());
    }
}

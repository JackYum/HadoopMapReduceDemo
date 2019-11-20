package com.atguigu.mapreduce.c_mapreduceframework.c_3_outputformat;

import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class FilterOutputFormat extends FileOutputFormat {
    @Override
    public RecordWriter getRecordWriter(TaskAttemptContext job) throws IOException {
	//创建一个RecordWriter
	return new FilterRecordWriter(job);
    }
}

package com.atguigu.mapreduce.c_mapreduceframework.c_1_inputformat.custoerInputFormat;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.IOException;

public class WholeInputFormat extends FileInputFormat {
    @Override
    protected boolean isSplitable(JobContext context, Path filename) {
	return false;
    }

    @Override
    public RecordReader createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
	WholeRecordReader recordReader = new WholeRecordReader();
	recordReader.initialize(split, context);
        return null;
    }
}

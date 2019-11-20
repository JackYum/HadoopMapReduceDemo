package com.atguigu.mapreduce.z_extendedcase.c_findblogsamefriends.solution02;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class CommonFriendRecordReader extends RecordReader<Text, BytesWritable>{

	private FileSplit split;
	private Configuration conf;
	
	private Text key = new Text();
	private BytesWritable value = new BytesWritable();
	
	private boolean flag = true;

	@Override
	public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {

		this.split = (FileSplit) split;
        this.conf = context.getConfiguration();
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		if (flag) {

            Path path = split.getPath();
            FileSystem fileSystem = path.getFileSystem(conf);
            FSDataInputStream inputStream = fileSystem.open(path);
            byte[] buf = new byte[(int) split.getLength()];
            inputStream.read(buf);
            key.set(path.toString());
            value.set(buf, 0, buf.length);

            inputStream.close();

            flag = false;
            return true;
        }
        return false;
	}

	@Override
	public Text getCurrentKey() throws IOException, InterruptedException {
	
		return key;
		
	}

	@Override
	public BytesWritable getCurrentValue() throws IOException, InterruptedException {
	
		return value;
		
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public void close() throws IOException {
		// TODO Auto-generated method stub
		
	}
	
	
}

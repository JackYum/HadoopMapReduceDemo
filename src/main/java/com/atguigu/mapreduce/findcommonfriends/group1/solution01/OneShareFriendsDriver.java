package com.atguigu.mapreduce.findcommonfriends.group1.solution01;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class OneShareFriendsDriver {

    public static void main(String[] args) throws Exception {

	// 输入输出路径需要根据自己电脑上实际的输入输出路径设置
	args = new String[]{"/Users/jiangren/Documents/便笺/input/z_extendedcase/c_findblogsamefriends-2/"
		,"/Users/jiangren/Documents/便笺/output/z_extendedcase/c_findblogsamefriends-2/one_output2"};

	// 1 获取job对象
	Configuration configuration = new Configuration();
	Job job = Job.getInstance(configuration);

	// 2 指定jar包运行的路径
	job.setJarByClass(OneShareFriendsDriver.class);

	// 3 指定map/reduce使用的类
	job.setMapperClass(OneShareFriendsMapper.class);
	//job.setReducerClass(OneShareFriendsReducer.class);

	// 4 指定map输出的数据类型
	job.setMapOutputKeyClass(Text.class);
	job.setMapOutputValueClass(NullWritable.class);

	// 5 指定最终输出的数据类型
	job.setOutputKeyClass(Text.class);
	job.setOutputValueClass(NullWritable.class);

	//不需要reducer
	//job.setNumReduceTasks(0);

	// 6 指定job的输入原始所在目录
	FileInputFormat.setInputPaths(job, new Path(args[0]));
	FileOutputFormat.setOutputPath(job, new Path(args[1]));

	// 7 提交
	boolean result = job.waitForCompletion(true);

	System.exit(result?0:1);
    }
}
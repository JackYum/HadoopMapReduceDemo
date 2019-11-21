package com.atguigu.mapreduce.findcommonfriends.group1.solution02;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class CommonFriendDriver {

	public static void main(String[] args) throws Exception{
		
		args=new String[]{"D:\\inputFriendFile","D:\\outputFriendFile"};

        Configuration conf=new Configuration();
        Job job= Job.getInstance(conf);

        job.setJarByClass(CommonFriendDriver.class);
        job.setMapperClass(CommonFriendMapper.class);
        job.setReducerClass(CommonFriendReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setInputFormatClass(CommonFriendInputFormat.class);

        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));

        job.waitForCompletion(true);
       
	}
}

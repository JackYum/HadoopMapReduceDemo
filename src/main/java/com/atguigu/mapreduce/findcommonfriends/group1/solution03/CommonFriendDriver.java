package com.atguigu.mapreduce.findcommonfriends.group1.solution03;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class CommonFriendDriver {

	public static void main(String[] args) throws Exception{
		Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);

        job.setJarByClass(CommonFriendDriver.class);
        job.setMapperClass(CommonFriendMapper.class);
        job.setReducerClass(CommonFriendReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        //设置缓存文件
        job.addCacheFile(new URI("file:///D:/test/friends/friends.txt"));

        FileInputFormat.setInputPaths(job,new Path("D:/test/friends/friends.txt"));
        FileOutputFormat.setOutputPath(job,new Path("D:/test/friendsout4"));

        job.waitForCompletion(true);

	}
}

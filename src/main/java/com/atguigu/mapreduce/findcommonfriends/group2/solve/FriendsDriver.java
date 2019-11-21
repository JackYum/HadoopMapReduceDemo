package com.wm.mapreduce.solve;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @author 99497
 */
public class FriendsDriver {

    public static void main(String[] args)
            throws IOException, ClassNotFoundException, InterruptedException {

        args = new String[]{"C:\\Users\\99497\\Downloads\\input\\friends.txt",
                "C:\\Users\\99497\\Downloads\\output\\solve_output4\\"};

        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf);
        job.setJarByClass(FriendsDriver.class);

        job.setMapperClass(FriendsMapper.class);
        job.setReducerClass(FriendsReducer.class);
        job.setCombinerClass(FriendsReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // 设置ReduceTask个数为0 没有reduce阶段 就没有 shuffle阶段
        // job.setNumReduceTasks(0);

        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);
    }
}

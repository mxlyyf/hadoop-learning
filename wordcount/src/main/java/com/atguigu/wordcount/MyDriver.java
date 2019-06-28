package com.atguigu.wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.util.UUID;

public class MyDriver {
    public static void main(String[] args) throws Exception {
// 输入输出路径需要根据自己电脑上实际的输入输出路径设置
        args = new String[] { "D:\\Documents\\mrinput\\wordcount", "D:\\Documents\\mroutput\\wordcount "+ UUID.randomUUID() };
        //args = new String[]{"/mrinput/wordcount", "/mroutput/wordcount"};

        // 1 获取配置信息以及封装任务
        Configuration conf = new Configuration();

        //conf.set("fs.defaultFS", "hdfs://192.168.213.101:9000");
        //conf.set("mapreduce.framework.name", "yarn");

        Job job = Job.getInstance(conf);

        // 2 设置jar加载路径
        job.setJarByClass(MyDriver.class);

        // 3 设置map和reduce类
        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);

        // 4 设置map输出
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        // 5 设置Reduce输出
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // 6 设置输入和输出路径
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // 7 提交
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

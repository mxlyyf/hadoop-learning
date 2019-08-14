package com.atguigu.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MyMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //分割一行
        String[] words = value.toString().split(" ");

        Text keyout = new Text();

        for (String word : words) {
            keyout.set(word);
            context.write(keyout, new IntWritable(1));
        }
    }
}

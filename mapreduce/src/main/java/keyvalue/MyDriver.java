package keyvalue;

import com.atguigu.wordcount.MyMapper;
import com.atguigu.wordcount.MyReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueLineRecordReader;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MyDriver {
    public static void main(String[] args) throws Exception {
// 输入输出路径需要根据自己电脑上实际的输入输出路径设置
        args = new String[]{"D:\\Documents\\mrinput\\keyvalue", "D:\\Documents\\mroutput\\keyvalue\\ " + System.currentTimeMillis()};
        //args = new String[]{"/mrinput/wordcount", "/mroutput/wordcount"};

        // 1 获取配置信息以及封装任务
        Configuration conf = new Configuration();

        conf.set(KeyValueLineRecordReader.KEY_VALUE_SEPERATOR, "***");

        //conf.set("fs.defaultFS", "hdfs://192.168.213.101:9000");
        //conf.set("mapreduce.framework.name", "yarn");

        Job job = Job.getInstance(conf);

        // 2 设置jar加载路径
        job.setJarByClass(MyDriver.class);

        //设置InputFormat
        job.setInputFormatClass(KeyValueTextInputFormat.class);

        // 3 设置map和reduce类
        job.setMapperClass(KeyValueMapper.class);
        job.setReducerClass(KeyValueReducer.class);

        // 4 设置map输出
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        // 5 设置Reduce输出
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // 6 设置输入和输出路径
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // 7 提交
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

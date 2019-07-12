package combine;

import com.atguigu.wordcount.MyMapper;
import com.atguigu.wordcount.MyReducer;
import customserde.FlowBean;
import customserde.FlowBeanMapper;
import customserde.FlowBeanReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MyDriver {
    public static void main(String[] args) throws Exception {
// 输入输出路径需要根据自己电脑上实际的输入输出路径设置
        args = new String[] { "D:\\Documents\\mrinput\\combine", "D:\\Documents\\mroutput\\combine\\ "+ System.currentTimeMillis() };
        //args = new String[]{"/mrinput/wordcount", "/mroutput/wordcount"};

        // 1 获取配置信息以及封装任务
        Configuration conf = new Configuration();

        //conf.set("fs.defaultFS", "hdfs://192.168.213.101:9000");
        //conf.set("mapreduce.framework.name", "yarn");

        Job job = Job.getInstance(conf);

        // 2 设置jar加载路径
        job.setJarByClass(MyDriver.class);

        //设置InputFormat
        //CombineTextInputFormat用于小文件过多的场景，
        // 它可以将多个小文件从逻辑上规划到一个切片中，这样，多个小文件就可以交给一个MapTask处理。
        job.setInputFormatClass(CombineTextInputFormat.class);
        CombineTextInputFormat.setMaxInputSplitSize(job,4096);//虚拟存储切片最大值设置:单位为byte

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

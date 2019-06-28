package customserde;

        import org.apache.hadoop.io.LongWritable;
        import org.apache.hadoop.io.Text;
        import org.apache.hadoop.mapreduce.Mapper;

        import java.io.IOException;

public class FlowBeanMapper extends Mapper<LongWritable, Text, Text, FlowBean> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] strings = line.split("\t");
        String phone = strings[1];
        long upflow = Long.valueOf(strings[strings.length - 3]);
        long downflow = Long.valueOf(strings[strings.length - 2]);
        FlowBean flowBean = new FlowBean(upflow, downflow);
        context.write(new Text(phone),flowBean);
    }
}

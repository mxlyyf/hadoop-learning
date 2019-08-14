package customserde;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

public class FlowBeanReducer extends Reducer<Text, FlowBean, Text, FlowBean> {
    @Override
    protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {
        Iterator<FlowBean> iterator = values.iterator();
        long sumUpFlow = 0, sumDownFlow = 0;
        while (iterator.hasNext()) {
            FlowBean flowBean = iterator.next();
            //context.write(key,flowBean);
            sumUpFlow += flowBean.getUpFlow();
            sumDownFlow += flowBean.getDownFlow();
        }

        FlowBean out = new FlowBean(sumUpFlow, sumDownFlow);
        context.write(key, out);
    }
}

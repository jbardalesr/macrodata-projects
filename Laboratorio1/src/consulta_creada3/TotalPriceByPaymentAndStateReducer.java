package consulta3;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.util.Iterator;


public class TotalPriceByPaymentAndStateReducer extends MapReduceBase implements Reducer<Text, IntWritable, Text, FloatWritable> {

    public void reduce(Text t_key, Iterator<IntWritable> values, OutputCollector<Text, FloatWritable> output, Reporter reporter) throws IOException {
        Text key = t_key;
        float sumValue = 0;
        float valueCount = 0;
        while (values.hasNext()) {
            // replace type of value with the actual type of our value
            IntWritable value = (IntWritable) values.next();
            sumValue += value.get();
            valueCount ++;
        }
        output.collect(key, new FloatWritable(sumValue/valueCount));
    }
}
package consulta5;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.util.Iterator;


public class MeanByPaymentAndCountryReducer extends MapReduceBase implements Reducer<Text, FloatWritable, Text, FloatWritable> {

    public void reduce(Text t_key, Iterator<FloatWritable> values, OutputCollector<Text, FloatWritable> output, Reporter reporter) throws IOException {
        float sum_value = 0;
        float count_value  = 0;
        while (values.hasNext()) {
            FloatWritable value = (FloatWritable) values.next();
            sum_value += value.get();
            count_value++;
        }
        output.collect(t_key, new FloatWritable(sum_value/count_value));
    }
}

class MaxCountryByDayReducer extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
    @Override
    public void reduce(Text text, Iterator<Text> iterator, OutputCollector<Text, Text> outputCollector, Reporter reporter) throws IOException {
        float minMean = Integer.MAX_VALUE;
        String payment = "";
        while (iterator.hasNext()) {
            Text value = (Text) iterator.next();
            String[] countries_amount = value.toString().split("\t");
            float mean = Float.parseFloat(countries_amount[1]);

            if (minMean > mean){
                minMean = mean;
                payment = countries_amount[0];
            }
        }
        outputCollector.collect(new Text(text.toString() + ", " + payment), new Text(String.valueOf(minMean)));
    }
}
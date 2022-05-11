package consulta5;// Hadoop provides its own set of basic types that are optimized for network serialization

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;

public class MeanByPaymentAndCountryMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, FloatWritable> {
    @Override
    public void map(LongWritable key, Text value, OutputCollector<Text, FloatWritable> output, Reporter reporter) throws IOException {
        String valueString = value.toString();
        String[] SingleCountryData = valueString.split(",");

        String country = SingleCountryData[7];

        if(!country.equals("Country")){
            float amount = Float.parseFloat(SingleCountryData[2]);
            String payment = SingleCountryData[3];
            output.collect(new Text(country + ", " + payment), new FloatWritable(amount));
        }
    }
}

class MaxCountryByDayMapper  extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
    @Override
    public void map(LongWritable longWritable, Text text, OutputCollector<Text, Text> outputCollector, Reporter reporter) throws IOException {
        String valueString = text.toString();
        String[] data = valueString.split(",");
        String country = data[0];
        String payment_mean = data[1];
        outputCollector.collect(new Text(country), new Text(payment_mean));
    }
}

package consulta1;// Hadoop provides its own set of basic types that are optimized for network serialization

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.Mapper;

import java.io.IOException;

public class TotalPriceByPaymentAndCountryMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {
    IntWritable price;

    @Override
    public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
        String valueString = value.toString();
        String[] SingleCountryData = valueString.split(",");
        if (!"Price".equals(SingleCountryData[2])) {
            price = new IntWritable(Integer.parseInt(SingleCountryData[2]));
        } else {
            price = new IntWritable(0);
        }
        output.collect(new Text(SingleCountryData[7] + "-" + SingleCountryData[3]), price);
    }
}

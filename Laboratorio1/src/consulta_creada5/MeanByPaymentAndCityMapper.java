package consulta5;// Hadoop provides its own set of basic types that are optimized for network serialization

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
public class MeanByPaymentAndCityMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, FloatWritable> {
    @Override
    public void map(LongWritable key, Text value, OutputCollector<Text, FloatWritable> output, Reporter reporter) throws IOException {
        String valueString = value.toString();
        String[] SingleCountryData = valueString.split(",");

        String city = SingleCountryData[5];

        if(!city.equals("City")){
            float amount = Float.parseFloat(SingleCountryData[2]);
            String payment = SingleCountryData[3];
            output.collect(new Text(city + ", " + payment), new FloatWritable(amount));
        }
    }
}

class MaxCountryByDayMapper  extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
    @Override
    public void map(LongWritable longWritable, Text text, OutputCollector<Text, Text> outputCollector, Reporter reporter) throws IOException {
        String valueString = text.toString();
        String[] data = valueString.split(",");
        String day = data[0];
        String country_amount = data[1];
        outputCollector.collect(new Text(day), new Text(country_amount));
    }
}

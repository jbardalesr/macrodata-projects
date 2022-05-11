package consulta2;// Hadoop provides its own set of basic types that are optimized for network serialization

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.util.regex.Pattern;
import java.util.regex.Matcher;
public class ProductByDayAndCountryMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {
    @Override
    public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
        String valueString = value.toString();
        String[] SingleCountryData = valueString.split(",");
        Pattern pattern = Pattern.compile("\\/(\\d{1,2})\\/", Pattern.CASE_INSENSITIVE);
        Matcher matcher = pattern.matcher(SingleCountryData[0]);
        if (matcher.find()){
            String str_day = matcher.group(1);
            output.collect(new Text("Day " + str_day + ", " + SingleCountryData[7]), new IntWritable(Integer.parseInt(SingleCountryData[2])));
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

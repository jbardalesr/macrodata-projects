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
public class ProductByDayMapper extends MapReduceBase implements Mapper<LongWritable, Text, IntWritable, Text> {
    IntWritable price;

    @Override
    public void map(LongWritable key, Text value, OutputCollector<IntWritable, Text> output, Reporter reporter) throws IOException {
        String valueString = value.toString();
        String[] SingleCountryData = valueString.split(",");
        Pattern pattern = Pattern.compile("\\/(\\d{1,2})\\/", Pattern.CASE_INSENSITIVE);
        Matcher matcher = pattern.matcher(SingleCountryData[0]);
        if (matcher.find()){
            String str_day = matcher.group(1);
            IntWritable day = new IntWritable(Integer.parseInt(str_day));
            output.collect(day, new Text(SingleCountryData[1]));
        }
    }
}

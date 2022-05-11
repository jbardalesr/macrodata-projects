package consulta4;// Hadoop provides its own set of basic types that are optimized for network serialization

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;

public class ProductAfter6PMByCountryMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, DoubleWritable> {
    DoubleWritable price;

    @Override
    public void map(LongWritable key, Text value, OutputCollector<Text, DoubleWritable> output, Reporter reporter) throws IOException {
        String valueString = value.toString();
        String[] SingleCountryData = valueString.split(",");
        if (!"Transaction_date".equals(SingleCountryData[0])) {
            String date = SingleCountryData[0];
            String[] time = date.split(" ")[1].split(":");
            int hour = Integer.parseInt(time[0]);
            int minutes = Integer.parseInt(time[1]);

            if (hour >= 18 && minutes >= 0) {
                price = new DoubleWritable(Double.parseDouble(SingleCountryData[2]));
                output.collect(new Text(SingleCountryData[7] + "-" + SingleCountryData[1]), price);
            }
        }
    }
}

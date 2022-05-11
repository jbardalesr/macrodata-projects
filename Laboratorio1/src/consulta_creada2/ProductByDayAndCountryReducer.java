package consulta2;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;


public class ProductByDayAndCountryReducer extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {

    public void reduce(Text t_key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
        int frequencyForCountry = 0;
        while (values.hasNext()) {
            IntWritable value = (IntWritable) values.next();
            frequencyForCountry += value.get();
        }
        output.collect(t_key, new IntWritable(frequencyForCountry));
    }
}

class MaxCountryByDayReducer extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
    @Override
    public void reduce(Text text, Iterator<Text> iterator, OutputCollector<Text, Text> outputCollector, Reporter reporter) throws IOException {
        int maxValue = Integer.MIN_VALUE;
        String country_name = "";
        while (iterator.hasNext()) {
            Text value = (Text) iterator.next();
            String[] countries_amount = value.toString().split("\t");
            int price = Integer.parseInt(countries_amount[1]);

            if (maxValue < price){
                maxValue = price;
                country_name = countries_amount[0];
            }
        }
        outputCollector.collect(new Text(text.toString() + ", " + country_name), new Text(String.valueOf(maxValue)));
    }
}
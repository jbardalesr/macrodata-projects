package salescountrydriver;
import java.io.IOException;
import java.util.*;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

public class SalesCountryReducer extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {
    
    public void reduce(Text t_key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
        
        String[] SingleCountryData = t_key.toString().split(" - ");
        int frequencyForCountry = 0;
        
        while (values.hasNext()) {            
            IntWritable value = (IntWritable) values.next();
            frequencyForCountry += value.get();
        }
        if (frequencyForCountry > 1){
            output.collect(new Text(SingleCountryData[0] + " - " + SingleCountryData[1] + ":"), new IntWritable(frequencyForCountry));   
        }

    }
}

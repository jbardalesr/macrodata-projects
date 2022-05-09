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


public class ProductByDayReducer extends MapReduceBase implements Reducer<IntWritable, Text, IntWritable, Text> {

    public void reduce(IntWritable t_key, Iterator<Text> values, OutputCollector<IntWritable,Text> output, Reporter reporter) throws IOException {
        HashMap<String, Integer> charCountMap = new HashMap<String, Integer>();

        while (values.hasNext()) {
            String value = ((Text) values.next()).toString();
            if (charCountMap.containsKey(value)) {
                charCountMap.put(value, charCountMap.get(value) + 1);
            } else {
                charCountMap.put(value, 1);
            }
        }
        output.collect(t_key, new Text(charCountMap.toString()));
    }
}

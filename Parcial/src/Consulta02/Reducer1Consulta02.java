package Consulta02;

import java.io.IOException;
import java.util.*;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

public class Reducer1Consulta02 extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {

    public void reduce(Text t_key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
        Text key = t_key;
        int frequencyForEducationIncome = 0;
        while (values.hasNext()) {
            // replace type of value with the actual type of our value
            IntWritable value = (IntWritable) values.next();
            frequencyForEducationIncome += value.get();

        }
        output.collect(key, new IntWritable(frequencyForEducationIncome));
    }
}

class Reducer2Consulta02 extends MapReduceBase implements Reducer<Text, Text, Text, Text> {

    public void reduce(Text t_key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
        Text key = t_key;
        int max = 0;
        String bestIncome = "";
        while (values.hasNext()) {
            // replace type of value with the actual type of our value
            Text value = (Text) values.next();
            String[] vals = value.toString().split(","); //income,freq
            int freqIncome = Integer.parseInt(vals[1]);
            if (freqIncome > max) {
                max = freqIncome;
                bestIncome = vals[0];
            }
        }
        output.collect(key, new Text(bestIncome + " freq: " + Integer.toString(max)));
    }
}

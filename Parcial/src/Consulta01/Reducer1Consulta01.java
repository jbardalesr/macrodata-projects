package Consulta01;

import java.io.IOException;
import java.util.*;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

public class Reducer1Consulta01 extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {

    public void reduce(Text t_key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
        Text key = t_key;
        int frequencyForGeneroTarjeta = 0;
        while (values.hasNext()) {
            // replace type of value with the actual type of our value
            IntWritable value = (IntWritable) values.next();
            frequencyForGeneroTarjeta += value.get();

        }
        output.collect(key, new IntWritable(frequencyForGeneroTarjeta));
    }
}

class Reducer2Consulta02 extends MapReduceBase implements Reducer<Text, Text, Text, Text> {

    public void reduce(Text t_key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
        Text key = t_key;
        int max = 0;
        String tipoTarjeta = "";
        while (values.hasNext()) {
            // replace type of value with the actual type of our value
            Text value = (Text) values.next();
            String[] vals = value.toString().split(","); // tipoTarjeta - freq
            int freqCard = Integer.parseInt(vals[1]);
            if (freqCard > max) {
                max = freqCard;
                tipoTarjeta = vals[0];
            }
        }
        output.collect(key, new Text(" Tipo de tarjeta más común: "+ tipoTarjeta + " freq: " + Integer.toString(max)));
    }
}

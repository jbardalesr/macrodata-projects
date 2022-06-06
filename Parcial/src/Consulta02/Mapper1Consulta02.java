package Consulta02;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

public class Mapper1Consulta02 extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {

    private final static IntWritable one = new IntWritable(1);

    public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
        if (key.get() == 0) {
            return;
        }
        String valueString = value.toString();
        String[] singleRowData = valueString.split(",");
        // Eduacation Level - Income Category
        output.collect(new Text(singleRowData[5] + "," + singleRowData[7]), one);
    }
}

class Mapper2Consulta02 extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {

    public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {

        String[] rowData = value.toString().split("\t"); //key[education-income] - value(freq) 
        String[] rowValue = rowData[0].split(",");
        int valueInt = Integer.parseInt(rowData[1]); // freq
        output.collect(new Text(rowValue[0]), new Text(rowValue[1] + "," + valueInt)); //education - income,freq
    }
}

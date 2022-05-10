package SalesCountry;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

public class SalesMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {
	private final static IntWritable one = new IntWritable(1);

	public void map(
		LongWritable key,
		Text value,
		OutputCollector<Text,
		IntWritable> output,
		Reporter reporter
	  ) throws IOException {

		String valueString = value.toString();
		String[] SingleCountryData = valueString.split(",");
		String date = SingleCountryData[0];
		int low = date.indexOf(" ")+1;
		int high = date.indexOf(":");
		String hour ="not defined";
		if ( low != -1 && high != -1) 
			hour = date.substring(low,high );			

	

		output.collect(new Text(hour), one);
	}
}

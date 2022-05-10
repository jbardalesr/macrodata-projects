package SalesCountry;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

public class SalesMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {
	//private final static IntWritable one = new IntWritable(1);


	private static String searchq;
	public void configure(JobConf job) {
		searchq = job.get("search").toString();
	}



	public void map(
		LongWritable key,
		Text value,
		OutputCollector<Text,
		IntWritable> output,
		Reporter reporter
	  ) throws IOException {

		String valueString = value.toString();
		String[] SingleCountryData = valueString.split(",");
		String country = SingleCountryData[7];
		
		Integer price = 0;
		if (!"Price".equals(SingleCountryData[2]))
			price = Integer.parseInt(SingleCountryData[2]);
		IntWritable pricewrite = new IntWritable(price);

		if(country.toLowerCase().trim().contains(searchq.toLowerCase().trim()))
			output.collect(new Text(country), pricewrite);

	}
}

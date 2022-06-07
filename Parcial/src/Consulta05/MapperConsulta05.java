package SalesCountry;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

public class SalesMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {
	//private final static IntWritable one = new IntWritable(1);
	
	private static Integer range;
	public void configure(JobConf job) {
		range = Integer.parseInt( job.get("rango").toString());
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
		String age = SingleCountryData[2];
		String cLimit = SingleCountryData[13];
		

		Integer edad = 0;
		Integer creditLimit  = 0; 
		Integer supe  = 0; 
		Integer infe = 0; 
		String rangeStr  = "[]";
		Double dLim = 0.0;
		if (!age.contains("Age"))
			edad  =  Integer.parseInt(age);
			infe  = (edad / range) * range;	
			supe  =  infe + range - 1;
			rangeStr =  "[" + infe.toString() + ","  +  supe.toString()  + "]";

		if (!cLimit.contains("Limit"))
			dLim = Double.parseDouble(cLimit);
			creditLimit  =dLim.intValue();
		

		IntWritable limt = new IntWritable(creditLimit);

		output.collect(new Text(rangeStr), limt);

	}
}

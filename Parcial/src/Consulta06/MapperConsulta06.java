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
		//range = Integer.parseInt( job.get("rango").toString());
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
		String ratiostr = SingleCountryData[20];
		

		Integer edad = 0;
		Integer intRatio  = 0; 
		Integer supe  = 0; 
		Integer infe = 0; 
		Double  dInfe = 0.0;
		Double  dSupe = 0.0;
		Integer range  = 10;
		String rangeStr  = "[]";
		Double dLim = 0.0;
		Double dRatio = 0.0;

		if (!ratiostr.contains("Ratio"))
			dRatio = 	Double.parseDouble(ratiostr) * 100;
			intRatio = dRatio.intValue();
			infe  = (intRatio / range) * range;	
			supe  =  infe + range ;
			rangeStr =  "[" + infe.toString() + "% ,"  +  supe.toString()  + "%]";

		if (!age.contains("Age"))
			edad = Integer.parseInt(age);
	
		IntWritable eda = new IntWritable(edad);

		output.collect(new Text(rangeStr), eda);

	}
}

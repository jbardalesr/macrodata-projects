package Consulta05;
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

// Contar el número de ventas por tipo de pago en cada Estado y País.
public class SalesMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
	private final static IntWritable one = new IntWritable(1);
        IntWritable numero;
        Text txttotal = new Text("Min of Product1");
	public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
		String valueString = value.toString();
		String[] SingleCountryData = valueString.split(",");
                if(!"Price".equals(SingleCountryData[2])){
                    //7-> Country, 5-> City, 2-> Price
                    output.collect(new Text(SingleCountryData[7].trim()), new Text(SingleCountryData[5].trim()+"-"+SingleCountryData[2].trim()));
                }
		
	}
}


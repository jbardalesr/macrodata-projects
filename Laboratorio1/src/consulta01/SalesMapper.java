package Consulta01;
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

// Contar el número de ventas por tipo de pago en cada Estado y País.
public class SalesMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {
	private final static IntWritable one = new IntWritable(1);
        IntWritable numero;
        Text txttotal = new Text("Min of Product1");
	public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
		String valueString = value.toString();
		String[] SingleCountryData = valueString.split(",");
                /*if("Product1".equals(SingleCountryData[1])){
                    numero = new IntWritable(Integer.parseInt(SingleCountryData[2]));
                }else{
                    numero = new IntWritable(999999999);
                }*/
		output.collect(new Text(SingleCountryData[3].trim()+"-"+SingleCountryData[6].trim()+"-" + SingleCountryData[7].trim()), one);
	}
}


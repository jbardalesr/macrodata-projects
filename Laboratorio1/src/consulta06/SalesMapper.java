package salescountrydriver;
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

public class SalesMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {

    IntWritable cantidad;
    Text txttotal = new Text("cteSumaTotal");
    
    @Override
    public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
        
        String valueString = value.toString();
        String[] SingleCountryData = valueString.split(",");
        
        if(!"Account_Created".equals(SingleCountryData[8])){
            cantidad = new IntWritable(1);
        }else{
            cantidad = new IntWritable(0);
        }
        output.collect(new Text(SingleCountryData[4]+ " - "+ SingleCountryData[7] + " - "+ SingleCountryData[8]), cantidad);

    }
}

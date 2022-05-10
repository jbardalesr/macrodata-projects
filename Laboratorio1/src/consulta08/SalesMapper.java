package salescitycountry;
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

public class SalesMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {

    
    @Override
    public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
    
        String valueString = value.toString();
        String[] SingleCountryData = valueString.split(",");
        

        if(!"Country".equals(SingleCountryData[7])){
            output.collect(new Text("El pais que tiene mas ciudades:"), new Text( SingleCountryData[7]+"-"+SingleCountryData[5]));
        }

    }
}

package salesnamecountry;
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
        
//        Considerando a los campos "account_create y Last_login" como identificador

//        if(!"Name".equals(SingleCountryData[4])){
//            output.collect(new Text( SingleCountryData[4] + "-"+SingleCountryData[8]+"-"+SingleCountryData[9] ), new Text(SingleCountryData[7]));
//        }

//        Considerando al campo "name" como identificador

        if(!"Name".equals(SingleCountryData[4])){
            output.collect(new Text( SingleCountryData[4] ), new Text(SingleCountryData[7]));
        }

    }
}

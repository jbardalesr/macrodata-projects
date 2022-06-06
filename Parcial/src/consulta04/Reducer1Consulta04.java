package consulta04;

import java.io.IOException;
import java.util.*;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

public class Reducer1Consulta04 extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable>{
    
    @Override
    public void reduce(Text t_key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
        Text key = t_key;
        int freq = 0 ;
        while (values.hasNext()){
            IntWritable value = (IntWritable) values.next();
            freq += value.get();
        }
        output.collect(key, new IntWritable(freq));
    }
}

class Reducer2Consulta04 extends MapReduceBase implements Reducer<Text, Text, Text, Text>{
    
    @Override
    public void reduce(Text t_key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
        Text key = t_key;
        int max = 0 ;
        String selectedGenere = "";
        String selectedMarital = "";
        
        while (values.hasNext()){
            Text value = (Text) values.next();
            String[] features = value.toString().split(",");
            
            int freq = Integer.parseInt(features[2]);
            
            if (max < freq){
                max = freq;
                if ("F".equals(features[0])){
                    selectedGenere = "Female";
                }else{
                    selectedGenere = "Male";
                }
                selectedMarital = features[1];
            }
        }
        output.collect(new Text("Attrition_Flag: "+ key), 
                new Text(" -> " + selectedGenere + " , " + selectedMarital + ", frequency:  " + Integer.toString(max)));
    }
}

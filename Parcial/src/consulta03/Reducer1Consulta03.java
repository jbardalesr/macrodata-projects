package consulta03;
import java.io.IOException;
import java.util.*;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
public class Reducer1Consulta03 extends MapReduceBase implements Reducer<Text, Text, Text, Text>{
    @Override
    public void reduce(Text t_key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
        Text key = t_key;
        int sum = 0 ;
        int count = 0;
        while (values.hasNext()){
            Text value = (Text) values.next();
            sum += Integer.parseInt(value.toString());
            count += 1;
        }
        output.collect(key, new Text( Integer.toString(sum/count)+ ","+ Integer.toString(count)));
        
    }
}


class Reducer2Consulta03 extends MapReduceBase implements Reducer<Text, Text, Text, Text>{
    
    @Override
    public void reduce(Text t_key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
        Text key = t_key;
        int count_max = 0;
        String mean = "";
        String age = "";
        
        while (values.hasNext()){
            Text value = (Text) values.next();
            String[] features = value.toString().split(",");
            
            int count = Integer.parseInt(features[1]);
            
            if (count_max < count){
               count_max = count;
               age = features[0];
               mean = features[2];
            }
        } 
        output.collect(new Text(key), new Text("-> "+age+" years old,"+ Integer.toString(count_max) +" people, mean months on book: " + mean ));
    }
}

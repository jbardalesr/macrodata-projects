package salesnamecountry;
import java.io.IOException;
import java.util.*;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*; 


public class SalesCountryReducer extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
    @Override
    public void reduce(Text t_key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
        
        Text key = t_key;
        Map<String, Integer> mapCountry = new HashMap<>();
        
        while (values.hasNext()) {
            
            String country = values.next().toString();
            if(!mapCountry.containsKey(country)){
                mapCountry.put(country,1);
            }  
        }
   
        int countedCountries = mapCountry.size();
        if(countedCountries > 1){
            output.collect(key, new Text(String.valueOf(countedCountries)));
        }
    }
}

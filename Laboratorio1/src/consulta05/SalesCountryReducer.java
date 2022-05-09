package Consulta05;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

public class SalesCountryReducer extends MapReduceBase implements Reducer<Text, Text, Text, Text> {

	public void reduce(Text t_key, Iterator<Text> values, OutputCollector<Text,Text> output, Reporter reporter) throws IOException {
		Text key = t_key;
        Map<String, Integer> map = new HashMap<String,Integer>();
		//int frequencyForCountry = 0;
		int mayor_ventas = Integer.MIN_VALUE;
                while(values.hasNext()){
                    Text value = (Text) values.next();
                    if(map.containsKey(value.toString())){
                        int val_map = map.get(value.toString());
                        map.replace(value.toString(),val_map+1);
                    }else{
                        map.put(value.toString(),1);
                    }
                }
                for(Integer frec: map.values()){
                    if(frec>mayor_ventas){
                        mayor_ventas=frec;
                    }
                }
                for(String city: map.keySet()){
                    if(map.get(city)==mayor_ventas){
                        output.collect(key, new Text(city));
                        break;
                    }
                }
                
                
                
	}
}


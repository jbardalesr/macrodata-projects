package Consulta02;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

public class SalesCountryReducer extends MapReduceBase implements Reducer<Text, Text, Text, IntWritable> {

	public void reduce(Text t_key, Iterator<Text> values, OutputCollector<Text,IntWritable> output, Reporter reporter) throws IOException {
		Text key = t_key;
                Map<String, Integer> map = new HashMap<String,Integer>();
                int mayor_transferencia = Integer.MIN_VALUE;
                while(values.hasNext()){
                    Text value = (Text) values.next();
                    String price_Card[] = value.toString().split("-"); // 0 precio 1 tipo de pago
                    if(map.containsKey(price_Card[1])){
                        int val_map = map.get(price_Card[1]);
                        map.replace(price_Card[1],val_map+Integer.parseInt(price_Card[0]));
                    }else{
                        map.put(price_Card[1],Integer.parseInt(price_Card[0]));
                    }
                }
               for(Integer frec: map.values()){
                    if(frec>mayor_transferencia){
                        mayor_transferencia=frec;
                    }
                }
               for(String card: map.keySet()){
                    if(map.get(card)==mayor_transferencia){
                        output.collect( new Text(key.toString()+" - " +card), new IntWritable(mayor_transferencia));
                    }
                }
	}
}


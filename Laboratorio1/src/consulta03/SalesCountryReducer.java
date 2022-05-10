package Consulta03;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

public class SalesCountryReducer extends MapReduceBase implements Reducer<Text, Text, Text, Text> {

	public void reduce(Text t_key, Iterator<Text> values, OutputCollector<Text,Text> output, Reporter reporter) throws IOException {
		Text key = t_key;
                //Mapear tipo de pago : veces usadas
                Map<String, Integer> map = new HashMap<String,Integer>();
                Iterator<Text> values_cardType = values;
                
		int mayor_tipo = Integer.MIN_VALUE;
                int menor_tipo = Integer.MAX_VALUE;
                while(values_cardType.hasNext()){
                    Text value_cardType = (Text) values.next();
                    if(map.containsKey(value_cardType.toString())){
                        int val_map = map.get(value_cardType.toString());
                        map.replace(value_cardType.toString(),val_map+1);
                    }else{
                        map.put(value_cardType.toString(),1);
                    }
                }
                
                //Obtener el minimo y maximo
                for(Integer frec: map.values()){
                    if(frec>mayor_tipo){
                        mayor_tipo=frec;
                    }
                    if(frec<menor_tipo){
                        menor_tipo=frec;
                    }
                }
                //La salida son los registros que se encuentran en el rango de menor_tipo, mayor_tipo
                for(String k: map.keySet()){
                    int v = map.get(k);
                    if(v>menor_tipo && v<mayor_tipo){
                        output.collect(new Text(k + "-" + key.toString()), new Text(Integer.toString(v)));
                    } 
                }
	}
}


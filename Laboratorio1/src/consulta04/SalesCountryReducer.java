package Consulta04;

import java.io.IOException;
import java.text.ParseException;
import java.util.*;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

//Clases para date
import java.text.SimpleDateFormat;  
import java.util.Date;  
import java.util.logging.Level;
import java.util.logging.Logger;

public class SalesCountryReducer extends MapReduceBase implements Reducer<Text, Text, Text, Text> {

	public void reduce(Text t_key, Iterator<Text> values, OutputCollector<Text,Text> output, Reporter reporter) throws IOException {
		Text key = t_key;
                
                Date mas_reciente = new Date(0,5,3);
                String nombre = "";
		
                Date date = new Date();
                SimpleDateFormat formatter = new SimpleDateFormat("MM/dd/yy HH:mm");
                //convertimos cada fecha a tipo Date y comparamos hasta obtener el más reciente
                while(values.hasNext()){
                    String value = values.next().toString();
                    String nombreFecha [] = value.split("-");       
                    try {
                        date = formatter.parse(nombreFecha[1]);
                    } catch (ParseException ex) {
                        Logger.getLogger(SalesCountryReducer.class.getName()).log(Level.SEVERE, null, ex);
                    }
                    if(date.compareTo(mas_reciente)>0){
                        mas_reciente = date;
                        nombre = nombreFecha[0];
                    }
                }
                
                output.collect(new Text(nombre + "-" + key), new Text(formatter.format(mas_reciente)));
	}
}


package salesnamecity;
import java.io.IOException;
import java.util.*;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*; 

public class SalesCountryReducer extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
    
    public double distancia(double lat1,double lon1,double lat2,double lon2){
        
        if((lat1 == lat2) && (lon1 == lon2)){
            return 0;
        }
        double distancia = Math.sqrt( (Math.pow( (lat2 - lat1) ,2)) + (Math.pow( (lon2 - lon1), 2)));
        
        return distancia;
        
    }
    @Override
    public void reduce(Text t_key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
              
        Text key = t_key;

//      city - name, lat, lon
        Map<String, List<String>> map = new HashMap<String, List<String>>();

        while (values.hasNext()) {
            
            List<String> coordenada = new ArrayList<String>();
            
            String value = values.next().toString();
            String[] cityLatLon = value.split("_");
            
            String cityName = cityLatLon[0] + "-" + cityLatLon[1];
            
            if(!map.containsKey(cityName)){
                coordenada.add(cityLatLon[2]); 
                coordenada.add(cityLatLon[3]); 
                map.put(cityName, coordenada);
            }
        }
            
        if (map.size() > 1){
            double latMedia = 0;
            double lonMedia = 0;
            int n = 0;
            double distMax = Integer.MIN_VALUE;
            String cliente1 = "";
            double latCliente1 = 0;
            double lonCliente1 = 0;
            String cliente2 = "";
            double latCliente2 = 0;
            double lonCliente2 = 0;

            for(Map.Entry<String, List<String>> entry: map.entrySet()){
                List<String> latLonMap = entry.getValue();

                latMedia += Double.parseDouble(latLonMap.get(0));
                lonMedia += Double.parseDouble(latLonMap.get(1));
                n +=1;   
            }

            latMedia = latMedia/n;
            lonMedia = lonMedia/n;

            for(Map.Entry<String, List<String>> entry: map.entrySet()){
                List<String> latLonMap = entry.getValue();

                double latCliente = Double.parseDouble(latLonMap.get(0));
                double lonCliente = Double.parseDouble(latLonMap.get(1));

                double dist = distancia(latMedia, lonMedia, latCliente, lonCliente);

                if(dist > distMax){
                    distMax = dist;
                    cliente1 =  entry.getKey();
                    latCliente1 = latCliente;
                    lonCliente1 = lonCliente;
                }  
            }

            distMax = Integer.MIN_VALUE;

            for(Map.Entry<String, List<String>> entry: map.entrySet()){
                List<String> latLonMap = entry.getValue();

                double latCliente = Double.parseDouble(latLonMap.get(0));
                double lonCliente = Double.parseDouble(latLonMap.get(1));

                double dist = distancia(latCliente1, lonCliente1, latCliente, lonCliente);

                if(dist > distMax){
                    distMax = dist;
                    cliente2 =  entry.getKey();
                    latCliente2 = latCliente;
                    lonCliente2 = lonCliente;
                }  
            }

            output.collect(key, new Text("\n\t\t\t->"+cliente1 + "\n\t\t\t->"+cliente2 ));
        }else{
            output.collect(key, new Text("\n\t\t\t->"+map.keySet().toString()));
        }

    }
}

package salescitycountry;
import java.io.IOException;
import java.util.*;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*; 


public class SalesCountryReducer extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
    @Override
    public void reduce(Text t_key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
        
        Map<String, String> mapNameCityByCountry = new HashMap<>();
        Map<String, Integer> mapNumberCityByCountry = new HashMap<>();
        
        Text key = t_key;
        int maxNumberOfCities = Integer.MIN_VALUE;
        String selectedCountry ="";
        
        while (values.hasNext()) {
            
            String value = values.next().toString();
            String[] countryCityData = value.split("-");
            String country = countryCityData[0];
            String city = countryCityData[1];
            
            if(!mapNumberCityByCountry.containsKey(country)){
                mapNumberCityByCountry.put(country, 1);
            }
            if(!mapNameCityByCountry.containsKey(city)){
                mapNameCityByCountry.put(city,country);    
            }else{
                int cantCity = mapNumberCityByCountry.get(mapNameCityByCountry.get(city));
                mapNumberCityByCountry.put(country, cantCity+1);
            }
        }
        
        for(String country:mapNumberCityByCountry.keySet()){
            int numberOfCities = mapNumberCityByCountry.get(country);
            if(numberOfCities > maxNumberOfCities ){  
                maxNumberOfCities = numberOfCities;
                selectedCountry = country;
            }
        }
        
        output.collect(key , new Text(selectedCountry +" con "+ Integer.toString(maxNumberOfCities)+" ciudades"));
        
        
    }
}

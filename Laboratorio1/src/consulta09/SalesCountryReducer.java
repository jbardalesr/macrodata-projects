package saleslogincountry;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.*;
import java.time.format.*;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*; 

public class SalesCountryReducer extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
    @Override
    public void reduce(Text t_key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
        
        SimpleDateFormat formatter = new SimpleDateFormat("MM/dd/yy HH:mm");        
        Text key = t_key;
        Date lastLogin = new Date(0,0,1);
        Date login = null;
        String name="";
        
        while (values.hasNext()) {
            
            String value = values.next().toString();
            String[] nameLogin = value.split("-");
            
            try {
                login = formatter.parse(nameLogin[1]);
            } catch (ParseException ex) {
                Logger.getLogger(SalesCountryReducer.class.getName()).log(Level.SEVERE, null, ex);
            }
                       
            if(lastLogin.before(login)){
                lastLogin = login;
                name = nameLogin[0];
            }
        }
        
        output.collect(key, new Text("\n\t\t\tName: "+name+"\n\t\t\tLast login: "+lastLogin ));

    }
}

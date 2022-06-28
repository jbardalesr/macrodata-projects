package consulta03;
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

public class Mapper1Consulta03 extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {

     private final static IntWritable one = new IntWritable(1);
    @Override
    public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
        if (key.get() == 0) {
            return;
        }
        String valueString = value.toString();
        String[] singleRowData = valueString.split(",");
        //  Attrition Flag - Customer_Age - Months_on_Book
        if(singleRowData[1].equals("Attrited Customer")){
            output.collect(new Text(singleRowData[1] + "," + singleRowData[2]), new Text(singleRowData[9]));
        }
        
    }
}

class Mapper2Consulta03 extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text>{

    @Override
    public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
        
        //  value = Attrition_Flag - Customer_Age - Mean(Months_on_Book)
        String [] rowData = value.toString().split("\t");
        String [] features = rowData[0].split(",");
        
        String attritionFlag = features[0];
        String age  = features[1];
        
        //  ((Attrition_Flag),(Customer_Age, count, Mean(Months_on_Book) ))

            output.collect(new Text(attritionFlag), new Text(age + "," +rowData[1]));
        

    }
}

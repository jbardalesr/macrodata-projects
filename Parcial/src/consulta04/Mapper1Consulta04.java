package consulta04;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

public class Mapper1Consulta04 extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable>{

    private final static IntWritable one = new IntWritable(1);

    @Override
    public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
        if (key.get() == 0) {
            return;
        }
        String valueString = value.toString();
        String[] singleRowData = valueString.split(",");
        //  Attrition Flag - Gender - Marital Status
        output.collect(new Text(singleRowData[1] + "," + singleRowData[3] + "," + singleRowData[6]), one);
    }
}

class Mapper2Consulta04 extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text>{

    @Override
    public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {

        //  value = Attrition_Flag - Gender - Marital_Status  freq
        String [] rowData = value.toString().split("\t");
        String [] features = rowData[0].split(",");
        String freq = rowData[1];

        String attritionFlag = features[0];
        String gender  = features[1];
        String marital_status = features[2];

        //  (Attrition_Flag, Gender - Marital Status - freq)
        output.collect(new Text(attritionFlag), new Text(gender + "," + marital_status + "," + freq));
    }
}

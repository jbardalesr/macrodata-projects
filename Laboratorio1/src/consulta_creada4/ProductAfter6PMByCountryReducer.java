package consulta4;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.util.Iterator;


public class ProductAfter6PMByCountryReducer extends MapReduceBase implements Reducer<Text, DoubleWritable, Text, DoubleWritable> {

    @Override
    public void reduce(Text text, Iterator<DoubleWritable> iterator, OutputCollector<Text, DoubleWritable> outputCollector, Reporter reporter) throws IOException {
        double amount = 0.0;
//        double quantity = 0.0;
        while (iterator.hasNext()) {
            DoubleWritable value = (DoubleWritable) iterator.next();
            amount += value.get();
//            quantity++;
        }
        outputCollector.collect(text, new DoubleWritable(amount));
    }
}
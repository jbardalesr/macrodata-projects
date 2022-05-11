package consulta4;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

/*
Total Price by PaymentType in each Country
United States-Amex      121700
United States-Diners    45600
United States-Mastercard        204350
United States-Visa      366650
*/
public class ProductAfter6PMByCountry {
    public static void main(String[] args) {
        JobClient my_client = new JobClient();
        // Create a configuration object for the job
        JobConf job_conf = new JobConf(ProductAfter6PMByCountry.class);

        // Set a name of the Job
        job_conf.setJobName("ProductAfter6PMInContry");

        // Specify data type of output key and value
        job_conf.setOutputKeyClass(Text.class);
        job_conf.setOutputValueClass(DoubleWritable.class);

        // Specify names of Mapper and Reducer Class
        job_conf.setMapperClass(ProductAfter6PMByCountryMapper.class);
        job_conf.setReducerClass(ProductAfter6PMByCountryReducer.class);

        // Specify formats of the data type of Input and output
        job_conf.setInputFormat(TextInputFormat.class);
        job_conf.setOutputFormat(TextOutputFormat.class);

        // Set input and output directories using command line arguments,
        //arg[0] = name of input directory on HDFS, and arg[1] =  name of output directory to be created to store the output file.

        FileInputFormat.setInputPaths(job_conf, new Path(args[0]));
        FileOutputFormat.setOutputPath(job_conf, new Path(args[1]));

        my_client.setConf(job_conf);
        try {
            // Run the job
            JobClient.runJob(job_conf);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

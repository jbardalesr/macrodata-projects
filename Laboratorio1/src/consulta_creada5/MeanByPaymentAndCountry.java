package consulta5;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

/*
Total Price by PaymentType in each Country
United States-Amex      121700
United States-Diners    45600
United States-Mastercard        204350
United States-Visa      366650
*/

public class MeanByPaymentAndCountry {
    public static void main(String[] args) {
        JobClient my_client1 = new JobClient();
        // Create a configuration object for the job
        JobConf job_conf1 = new JobConf(MeanByPaymentAndCountry.class);
        // Set a name of the Job
        job_conf1.setJobName("JOB1");
        // Specify data type of output key and value
        job_conf1.setOutputKeyClass(Text.class);
        job_conf1.setOutputValueClass(FloatWritable.class);
        // Specify names of Mapper and Reducer Class
        job_conf1.setMapperClass(MeanByPaymentAndCountryMapper.class);
        job_conf1.setReducerClass(MeanByPaymentAndCountryReducer.class);
        // Specify formats of the data type of Input and output
        job_conf1.setInputFormat(TextInputFormat.class);
        job_conf1.setOutputFormat(TextOutputFormat.class);
        // Set input and output directories using command line arguments,
        //arg[0] = name of input directory on HDFS, and arg[1] =  name of output directory to be created to store the output file.
        FileInputFormat.setInputPaths(job_conf1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job_conf1, new Path(args[1]));
        my_client1.setConf(job_conf1);

        JobClient my_client2 = new JobClient();
        JobConf job_conf2 = new JobConf(MeanByPaymentAndCountry.class);
        job_conf2.setJobName("JOB2");
        job_conf2.setOutputKeyClass(Text.class);
        job_conf2.setOutputValueClass(Text.class);
        job_conf2.setMapperClass(MaxCountryByDayMapper.class);
        job_conf2.setReducerClass(MaxCountryByDayReducer.class);
        job_conf2.setInputFormat(TextInputFormat.class);
        job_conf2.setOutputFormat(TextOutputFormat.class);
        FileInputFormat.setInputPaths(job_conf2, new Path(args[1]));
        FileOutputFormat.setOutputPath(job_conf2, new Path(args[2]));
        my_client2.setConf(job_conf2);
        try {
            // Run the job
            JobClient.runJob(job_conf1);
            JobClient.runJob(job_conf2);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

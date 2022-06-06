package Consulta02;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

public class DriverConsulta02 {

    public static void main(String[] args) {
        JobClient my_client = new JobClient();
        JobConf job_conf1 = new JobConf(DriverConsulta02.class);
        job_conf1.setJobName("Sale1");
        job_conf1.setOutputKeyClass(Text.class);
        job_conf1.setOutputValueClass(IntWritable.class);
        job_conf1.setMapperClass(Consulta02.Mapper1Consulta02.class);
        job_conf1.setReducerClass(Consulta02.Reducer1Consulta02.class);
        job_conf1.setInputFormat(TextInputFormat.class);
        job_conf1.setOutputFormat(TextOutputFormat.class);

        FileInputFormat.setInputPaths(job_conf1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job_conf1, new Path(args[1]));
        my_client.setConf(job_conf1);
        //#########################################

        JobClient my_client2 = new JobClient();
        JobConf job_conf2 = new JobConf(DriverConsulta02.class);
        job_conf2.setJobName("Sale2");
        job_conf2.setOutputKeyClass(Text.class);
        job_conf2.setOutputValueClass(Text.class);
        job_conf2.setMapperClass(Consulta02.Mapper2Consulta02.class);
        job_conf2.setReducerClass(Consulta02.Reducer2Consulta02.class);
        job_conf2.setInputFormat(TextInputFormat.class);
        job_conf2.setOutputFormat(TextOutputFormat.class);

        FileInputFormat.setInputPaths(job_conf2, new Path(args[1]));
        FileOutputFormat.setOutputPath(job_conf2, new Path(args[2]));

        try {
            JobClient.runJob(job_conf1);
            JobClient.runJob(job_conf2);
        } catch (Exception e) {
            e.printStackTrace();  
        }

    }
}

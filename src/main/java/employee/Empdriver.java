package employee;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class Empdriver {


    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        Path inpath=new Path(args[0]);
        Path outpath=new Path(args[1]);
        Configuration conf=new Configuration();
        Job job=new Job(conf);
        job.setMapperClass(Empmapper.class);
        job.setReducerClass(Empreducer.class);
        job.setJarByClass(Empdriver.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        FileInputFormat.setInputPaths(job,inpath);
        FileOutputFormat.setOutputPath(job,outpath);

        if(job.waitForCompletion(true))// this is only used to check if the job is completed or not ie its not necessary
        {
            System.out.println("completed succesfully");
        }



    }
}

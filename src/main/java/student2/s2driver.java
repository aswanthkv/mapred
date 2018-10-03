package student2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;


public class s2driver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Path inp_path=new Path(args[0]);
        Path out_path=new Path(args[1]);
        Configuration conf=new Configuration();
        Job job=new Job(conf);
        job.setMapperClass(S2mapper.class);
        job.setReducerClass(S2reducer.class);
        job.setJarByClass(s2driver.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        FileInputFormat.setInputPaths(job,inp_path);
        FileOutputFormat.setOutputPath(job,out_path);

        if(job.waitForCompletion(true))// this is only used to check if the job is completed or not ie its not necessary
        {
            System.out.println("completed succesfully");
        }
    }
}

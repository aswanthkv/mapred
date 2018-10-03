package student3;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class S3mapper extends Mapper<LongWritable,Text,Text,IntWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String s=value.toString().trim();
        String[] str=s.split(" ");

        if((Integer.parseInt(str[3]))>50) {

            Text keyout = new Text(str[2]);
            IntWritable vout=new IntWritable(1);
            context.write(keyout,vout);
        }
    }
}

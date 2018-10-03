package student1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class S1mapper extends Mapper<LongWritable,Text,Text,IntWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String s=value.toString().trim();
        String[] str=s.split(" ");

        Text kout=new Text(str[0]);
        IntWritable vout=new IntWritable(Integer.parseInt(str[3]));

        context.write(kout,vout);
    }
}

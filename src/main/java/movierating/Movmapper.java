package movierating;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class Movmapper extends Mapper<LongWritable,Text,IntWritable,IntWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String s=value.toString().trim();
        String[] str=s.split("[\\s]+");
        IntWritable keyout=new IntWritable(Integer.parseInt(str[0]));
        IntWritable valout=new IntWritable(Integer.parseInt(str[2]));

        context.write(keyout,valout);

    }
}

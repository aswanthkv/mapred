package movie;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class Mmapper extends Mapper<LongWritable,Text,IntWritable,IntWritable> {

    IntWritable keyout;
    IntWritable valout;
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String s = value.toString();
        String[] str = s.split("[\\s]+");
        keyout=new IntWritable(Integer.parseInt(str[1]));
        valout=new IntWritable(Integer.parseInt(str[2]));
        IntWritable outk = new IntWritable(Integer.parseInt(str[0]));
        IntWritable outv = new IntWritable(Integer.parseInt(str[1]));

        context.write(outk,outv);

    }
}


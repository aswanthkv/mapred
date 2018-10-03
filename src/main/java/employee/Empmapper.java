package employee;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class Empmapper extends Mapper<LongWritable,Text,Text,Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String val=value.toString().trim();
        String[] s=val.split(" ");

            Text outkey=new Text(s[2]);
            Text outval=new Text(s[1]);
            context.write(outkey,outval);

        }
    }


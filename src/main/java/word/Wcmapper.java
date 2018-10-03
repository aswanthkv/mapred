package word;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class Wcmapper extends Mapper<LongWritable,Text,Text,IntWritable> {

    Text word;

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {


    String keyvalue=value.toString().trim();
        String[] s=keyvalue.split(" ");
        word=new Text(s[1]);

        for(String obj:s)
        {
            Text outkey=new Text(obj);
            IntWritable outval=new IntWritable(1);
            context.write(outkey,outval);

    }
}
}

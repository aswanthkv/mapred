package student3;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class S3reducer extends Reducer<Text,IntWritable,Text,IntWritable> {

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int count=0;
        for(IntWritable obj:values)
        {
            count++;
        }
        IntWritable vout=new IntWritable(count);
        context.write(key,vout);
    }
}

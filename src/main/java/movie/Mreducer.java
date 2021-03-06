package movie;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class Mreducer extends Reducer<IntWritable,IntWritable,IntWritable,IntWritable> {

    int count=0;
    @Override
    protected void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

        for(IntWritable obj:values)
        {
            count++;
        }

        IntWritable valout=new IntWritable(count);
        context.write(key,valout);
    }
}

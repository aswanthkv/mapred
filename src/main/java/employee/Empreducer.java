package employee;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class Empreducer extends Reducer<Text,Text,Text,IntWritable> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        int count=0;
        for(Text val:values)
        {
          count++;
        }
        context.write(key,new IntWritable(count));
    }
}

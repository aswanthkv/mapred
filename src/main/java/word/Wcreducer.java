package word;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class Wcreducer extends Reducer<Text,IntWritable,Text,IntWritable> {
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        //
        int sum=0;
        for(IntWritable obj:values)
        {
            sum=sum+obj.get();

        }

        IntWritable valout=new IntWritable(sum);
        context.write(key,valout);
    }
}

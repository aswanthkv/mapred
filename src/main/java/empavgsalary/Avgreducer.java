package empavgsalary;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class Avgreducer extends Reducer<Text,IntWritable,Text,IntWritable> {
    int avg=0;

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

        int s=0;
        int count=0;
        for(IntWritable obj: values)
        {
            s=s+obj.get();
            count++;
            avg=(s/count);

        }
        context.write(key,new IntWritable(avg));
    }
}

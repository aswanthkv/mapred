package electricity2;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class Ereducer extends Reducer<IntWritable,IntWritable,IntWritable,IntWritable>
{
    @Override
    protected void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {


        int s=0;
        int count=0;
        for(IntWritable obj:values)
        {
            s=s+obj.get();
            count++;
        }
        IntWritable valout=new IntWritable(count);

        context.write(key,valout);
    }
}

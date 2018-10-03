package movierating;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class Movreducer extends Reducer<IntWritable,IntWritable,IntWritable,IntWritable> {

    @Override
    protected void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

        int sum=0;
        int count=0;
        for(IntWritable obj:values)
        {
            sum=sum+obj.get();
            count++;
        }
        int p=(sum/count);
        int perc=p*100;
        IntWritable outval=new IntWritable((perc/5));
        context.write(key,outval);
    }
}

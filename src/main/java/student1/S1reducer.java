package student1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

class S1reducer extends Reducer<Text,IntWritable,Text,IntWritable> {

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

        int sum=0;
        int count=0;

        for(IntWritable obj:values)
        {
            sum=sum+obj.get();
            count++;
        }
        
        IntWritable valout=new IntWritable(sum/count);
        context.write(key,valout);
    }
}

package electricity3;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class Ereducer extends Reducer<IntWritable,Text,IntWritable,Text> {
    @Override
    protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {


        for (Text obj : values) {
            Text valout = new Text(obj);

            context.write(key, valout);
        }
    }
}

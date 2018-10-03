package student2;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class S2mapper extends Mapper<LongWritable,Text,Text,Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String s=value.toString().trim();
        String[] str=s.split(" ");

        if((str[2].equals("physics"))&&((Integer.parseInt(str[3]))>50)) {

            Text keyout = new Text(str[2]);
            Text valout = new Text(str[0]);
            context.write(keyout,valout);
        }
    }
}

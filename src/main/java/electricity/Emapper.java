package electricity;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;



public class Emapper extends Mapper<LongWritable,Text,IntWritable,IntWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String s=value.toString().trim();

        if(s.matches(".*[a-zA-Z]+.*"))
        {
        }
        else
        {
            String[] str=s.split("[\\s]+");
            IntWritable keyout=new IntWritable(Integer.parseInt(str[0]));
            for(int i=1;i<13;i++)
            {
                IntWritable valout=new IntWritable(Integer.parseInt(str[i]));
                context.write(keyout,valout);

            }
        }

    }
}

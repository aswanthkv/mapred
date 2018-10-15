package electricity2;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;


public class Emapper extends Mapper<LongWritable,Text,IntWritable,IntWritable> {


    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String s = value.toString().trim();

        if (s.matches(".*[a-zA-Z]+.*")) {
        } else {
            String[] str = s.split("[\\s]+");

            IntWritable keyout = new IntWritable(Integer.parseInt(str[0]));
            for (int i = 1; i < 13; i++) {
                if (Integer.parseInt(str[i]) > 30) {
                    IntWritable valout = new IntWritable(1);
                    context.write(keyout, valout);
                } else {
                }
            }
        }

    }
}


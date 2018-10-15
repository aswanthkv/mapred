package electricity4;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;


public class Emapper extends Mapper<LongWritable,Text,Text,IntWritable> {

    String[] stri;

    {
        stri = new String[13];
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String s=value.toString().trim();

        if(s.matches(".*[a-zA-Z]+.*"))
        {
            stri =s.split("[\\s]+");
        }

        else
        {
            String[] str=s.split("[\\s]+");


            for(int i=2;i<7;i++)
            {
                Text keyout1=new Text(stri[i-1]);

                IntWritable valout1=new IntWritable(Integer.parseInt(str[i]));
                context.write(keyout1, valout1);
            }
        }
        }

    }


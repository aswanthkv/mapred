package electricity1;

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
            stri=s.split("[\\s]+");
            for(int i=3;i<11;i++) {
                Text keyout =new Text(stri[i]);
                IntWritable valout=new IntWritable(0);
                context.write(keyout,valout);
            }

        }
        else
        {
            String[] str=s.split("[\\s]+");

            for(int i=3;i<12;i++)
            {
                Text keyout1=new Text(stri[i]);
                IntWritable valout1=new IntWritable(Integer.parseInt(str[i+1]));
                if(keyout1.equals("Dec"))
                {

                }
                else {
                    context.write(keyout1, valout1);
                }

            }
        }

    }
}

package electricity3;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;


public class Emapper extends Mapper<LongWritable,Text,IntWritable,Text> {

    int m=0;
    TreeMap<Integer,String> t=new TreeMap<Integer,String>();

    String[] stri;
    {
        stri = new String[14];
    }

    String[] k=new String[13];


    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String s = value.toString().trim();

        if (s.matches(".*[a-zA-Z]+.*")) {
            stri=s.split("[\\s]+");

        } else {

            String[] str = s.split("[\\s]+");
            IntWritable keyout=new IntWritable(Integer.parseInt(str[0]));

            for(int j=1; j<13; j++)
            {
                t.put(Integer.parseInt(str[j]),stri[j-1]);
            }

            for(Map.Entry<Integer,String> obj:t.entrySet())
            {
                for(int m=0;m<13;m++)
                {
                    k[m]=obj.getValue();
                }
            }


        for(int i=1;i<4;i++)
        {
            Text valout=new Text(k[i]);
            context.write(keyout,valout);
        }
        }
    }
}


package student2;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class S2reducer extends Reducer<Text,Text,Text,Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        StringBuilder Stri = new StringBuilder();


        for (Text obj : values) {
            Stri.append(obj.toString()) ;
        }
        Text vout = new Text(Stri.toString());
        context.write(key,vout);
    }
}

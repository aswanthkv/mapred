package word;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;

public class WcreducerTest {

    ArrayList<IntWritable> al=new ArrayList<IntWritable>();

    @Test
    public void wcred() throws IOException, InterruptedException {

        al.add(new IntWritable(1));
        al.add(new IntWritable(1));
        al.add(new IntWritable(1));

        Wcreducer obj=new Wcreducer();
        Reducer.Context c=mock(Reducer.Context.class);
        Text key=new Text("cat");
         Iterable<IntWritable> val=new Iterable<IntWritable>() {
             public Iterator<IntWritable> iterator() {
                 return al.iterator();
             }
         };
         obj.reduce(key,val,c);
         assertEquals(3,obj.sum);


    }

}
package empavgsalary;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;

public class AvgreducerTest {

    ArrayList<IntWritable> al=new ArrayList<IntWritable>();


    @Test
    public void avgtest() throws IOException, InterruptedException {
        al.add(new IntWritable(30000));
        al.add(new IntWritable(50000));
        al.add(new IntWritable(80000));

        Reducer.Context c=mock(Reducer.Context.class);
        Avgreducer obj=new Avgreducer();
        Text key=new Text("cse");
        Iterable<IntWritable> val=new Iterable<IntWritable>() {
            public Iterator<IntWritable> iterator() {
                return al.iterator();
            }
        };
        obj.reduce(key,val,c);
        assertEquals(53333,obj.avg);

    }

}
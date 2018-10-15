package movie;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;

public class MreducerTest {
    ArrayList<IntWritable> al=new ArrayList<IntWritable>();

    @Test
    public void mtest() throws IOException, InterruptedException {

        al.add(new IntWritable(2));
        al.add(new IntWritable(3));
        al.add(new IntWritable(4));
        Mreducer m=new Mreducer();
        Reducer.Context c=mock(Reducer.Context.class);
        IntWritable key=new IntWritable(201);
        Iterable<IntWritable> val=new Iterable<IntWritable>() {
            public Iterator<IntWritable> iterator() {
                return al.iterator();

            }
        };
        m.reduce(key,val,c);
        assertEquals(3,m.count);
    }

}


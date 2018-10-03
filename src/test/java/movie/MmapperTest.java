package movie;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;

public class MmapperTest {

    @Test
    public void movie() throws IOException, InterruptedException {
        Text value=new Text("186   242   3    1056745");
        LongWritable key=new LongWritable(123);
        Mapper.Context c=mock(Mapper.Context.class);
        Mmapper m=new Mmapper();
        m.map(key,value,c);
        assertEquals(new IntWritable(242),m.keyout);
        assertEquals(new IntWritable(3),m.valout);
    }

}
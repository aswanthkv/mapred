package word;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;

public class WcmapperTest {
    @Test
    public void map() {
        Mapper.Context c=mock(Mapper.Context.class);
        LongWritable key=new LongWritable(100);
        Text val=new Text("hai jishnu how are you");
        Wcmapper obj=new Wcmapper();
        try {
            obj.map(key,val,c);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        assertEquals(new Text("jishnu"),obj.word);
    }

}
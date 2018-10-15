package unittestingbasic;

import org.junit.Test;

import static org.junit.Assert.*;

public class Mult_twoTest {
    @Test
    public void testMult_twotest()
    {
        int a=10;
        int b=2;
        Mult_two obj=new Mult_two();
        obj.multiplytwo(a,b);
        System.out.println("result="+obj.pro);
        assertEquals(12,obj.pro);
    }

}
package ru.turko.mephi;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.io.BufferedReader;
import java.io.FileReader;


public class MapReduceTest {
    private Text testTextLine1;
    private Text testTextLine2;
    private MapDriver<LongWritable, Text, Text, IntWritable> mapDriver;
    private ReduceDriver<Text, IntWritable, Text, IntWritable> reduceDriver;
    private MapReduceDriver<LongWritable, Text, Text, IntWritable, Text, IntWritable> mapReduceDriver;

    @Before
    public void setUp() throws Exception {
        ClassMapper mapper = new ClassMapper();
        ClassReducer reducer = new ClassReducer();
        mapDriver = MapDriver.newMapDriver(mapper);
        reduceDriver = ReduceDriver.newReduceDriver(reducer);
        mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);

        // read first file for test
        StringBuffer fileData = new StringBuffer();
        BufferedReader reader = new BufferedReader(new FileReader("testLine1.txt"));
        char[] buf = new char[1024];
        int numRead=0;
        while((numRead=reader.read(buf)) != -1){
            String readData = String.valueOf(buf, 0, numRead);
            fileData.append(readData);
        }
        reader.close();
        String str1 =  fileData.toString();

        // read second file for test
        fileData = new StringBuffer();
        reader = new BufferedReader(new FileReader("testLine2.txt"));
        numRead=0;
        while((numRead=reader.read(buf)) != -1){
            String readData = String.valueOf(buf, 0, numRead);
            fileData.append(readData);
        }
        reader.close();
        String str2 =  fileData.toString();

        testTextLine1 = new Text(str1);
        testTextLine2 = new Text(str2);
    }

    @Test
    public void testMapper() throws Exception {
        mapDriver.withInput(new LongWritable(1),testTextLine1);
        mapDriver.withInput(new LongWritable(1),testTextLine2);
        mapDriver.withOutput(new Text("Two"), new IntWritable(3));
        mapDriver.withOutput(new Text("beer"), new IntWritable(4));
        mapDriver.withOutput(new Text("or"), new IntWritable(2));
        mapDriver.withOutput(new Text("not"), new IntWritable(3));
        mapDriver.withOutput(new Text("two"), new IntWritable(3));
        mapDriver.withOutput(new Text("beer"), new IntWritable(4));
        mapDriver.runTest();
    }

    @Test
    public void testReducer() throws Exception {
        List<IntWritable> v1 = new ArrayList<>();
        v1.add(new IntWritable(3));
        List<IntWritable> v2 = new ArrayList<>();
        v1.add(new IntWritable(4));
        reduceDriver.withInput(new Text("foo"), v1);
        reduceDriver.withInput(new Text("beer"), v2);
        reduceDriver.withOutput(new Text("beer"), new IntWritable(4));
        reduceDriver.runTest();
    }

    @Test
    public void testMapReduce() throws Exception {
        mapReduceDriver.withInput(new LongWritable(1), testTextLine1);
        mapReduceDriver.withOutput(new Text("beer"), new IntWritable(4));
        mapReduceDriver.runTest();
    }
}

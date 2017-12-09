package ru.turko.mephi;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;


public class ClassReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    private String maxWord;

    //set longest word
    @Override
    protected void setup(Context context) throws java.io.IOException, InterruptedException {
        maxWord = "";
    }

    // realization reducer
    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        if (key.toString().length() > maxWord.length()) {
            maxWord = key.toString();
        }
    }

    // write result in context
    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        context.write(new Text(maxWord), new IntWritable(maxWord.length()));
    }
}

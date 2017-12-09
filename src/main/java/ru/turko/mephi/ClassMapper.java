package ru.turko.mephi;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class ClassMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String txt = value.toString();
        // remove punctuation marks
        txt = txt.replaceAll("\\.", "");
        txt = txt.replaceAll("\\?", "");
        txt = txt.replaceAll(":-=_',!]", "");
        for(String word:txt.split(" ")) { // split into words
            if (word.length() > 0) {
                context.write(new Text(word), new IntWritable(word.length()));
            }
        }
    }
}

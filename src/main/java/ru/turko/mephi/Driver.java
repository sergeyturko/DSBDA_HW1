package ru.turko.mephi;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Driver extends Configured implements Tool {

    public static void main(String args[]) throws Exception {
        int exitCode = ToolRunner.run(new Driver(), args);
        System.exit(exitCode);
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        // check number of args
        if (args.length != 2) {
            System.out.println("Usage: "+getClass().getSimpleName()+" needs two arguments: input and output directories\n");
            return -1;
        }
        // output CSV format
        conf.set("mapreduce.output.textoutputformat.separator",",");
        // get job, set jar class and name
        Job job = Job.getInstance(conf);
        job.setJarByClass(Driver.class);
        job.setJobName("HW 1");

        // mapper params
        job.setMapperClass(ClassMapper.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setMapOutputKeyClass(Text.class);

        // reducer params
        job.setReducerClass(ClassReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        // set paths
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        int returnValue = job.waitForCompletion(true) ? 0 : 1;
        if(job.isSuccessful()) {
            System.out.println("Job was successful!");
        } else if(!job.isSuccessful()) {
            System.out.println("Job was not successful!!!");
        }
        return returnValue;
    }
}

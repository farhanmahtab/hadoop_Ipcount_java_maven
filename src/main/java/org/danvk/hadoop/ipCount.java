package org.danvk.hadoop;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class ipCount {
    public static void main(String[] args)
            throws IOException,InterruptedException,ClassNotFoundException {
        Path inputpath = new Path(args[0]);
        Path outputDir = new Path(args[1]);

        //create configuartion
        Configuration configuration = new Configuration(true);

        //create job
        Job job = new Job(configuration,"IpCount");
        job.setJarByClass(ipCountMapper.class);

        //set map-reduce
        job.setMapperClass(ipCountMapper.class);
        job.setReducerClass(ipCountReducer.class);
        job.setNumReduceTasks(1);

        //specify (key,value)
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        //Input
        FileInputFormat.addInputPath(job, inputpath);
        job.setInputFormatClass(TextInputFormat.class);

        //output
        FileOutputFormat.setOutputPath(job,outputDir);
        job.setOutputFormatClass(TextOutputFormat.class);

        // Delete output if exists
        FileSystem hdfs = FileSystem.get(configuration);
        if (hdfs.exists(outputDir))
            hdfs.delete(outputDir, true);

        // Execute job
        int code = job.waitForCompletion(true) ? 0 : 1;
        System.exit(code);
    }
}

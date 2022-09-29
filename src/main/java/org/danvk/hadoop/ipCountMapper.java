package org.danvk.hadoop;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class ipCountMapper extends
        Mapper<Object,Text,Text,IntWritable> {
    private final IntWritable ONE = new IntWritable(1);
    private Text ip = new Text();

    public void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {

        String[] data = value.toString().split(" ");
        String[] path = data[6].split("/");
        if (path.length > 0) {
            String[] filePath = path[path.length - 1].split("\\?");
            if (filePath.length > 0) {
                //context.write(new Text("!!!!!!!!"), ONE);
                ip.set(filePath[0]);
                context.write(ip, ONE);


            }
        }
    }
}



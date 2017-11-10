package nl.sara.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.Reducer;

public class MyReducer extends Reducer<Text, Text, Text, Text> {
    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) {
        int tweets = 0;
        int mentions = 0;

        for (Text val : values) { 
           String s = val.toString();
           String[] array = s.split(" ");
           tweets += Integer.parseInt(array[0]);
           mentions += Integer.parseInt(array[1]);
        }
        try { 
           context.write(key, new Text(tweets+" "+mentions));
        } catch (Throwable e) {
           System.out.println("Error: " + e + " " + e.getMessage());
        }
    }
}

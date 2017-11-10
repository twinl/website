package nl.sara.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Reducer;

public class MyReducer extends Reducer<Text, Text, Text, Text> {
    private int tokens; // total number of words in collection
    private int types;  // total number of distinct words in collection

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) {
        int total = 0;
        Text description = new Text("");
        for (Text val : values) { total++; description = val; } // we don't use the count

        try { 
           context.write(key, description);
        } catch (Throwable e) {
           System.out.println("Error: " + e + " " + e.getMessage());
        }
    }
}

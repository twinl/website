package nl.sara.hadoop;

import java.io.IOException;
import java.lang.InterruptedException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class MyReducer extends Reducer<Text, Text, Text, Text> {
    private MultipleOutputs multipleOutputs;

    @Override
    public void setup(Context context) {
        multipleOutputs = new MultipleOutputs(context);
        /* reduce has no named input file! */
    }

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) {
        int count = 0;

        for (Text val : values) { count += Integer.parseInt(val.toString()); }
        try { 
           // context.write(key, new Text(nbrOfMatches+" "+nbrOfMisses));
           multipleOutputs.write("text", new Text(count+""), key, "counts");
        } catch (Throwable e) {
           // catch io errors from FileInputStream or readLine()
           System.out.println("Error: " + e + " " + e.getMessage());
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        multipleOutputs.close();
    }
}

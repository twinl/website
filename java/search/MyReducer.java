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
    private String search;

    @Override
    public void setup(Context context) {
        multipleOutputs = new MultipleOutputs(context);
        /* get search query from configuration */
        Configuration conf = context.getConfiguration();
        search = conf.get("search");
        /* reduce has no named input file! */
    }

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) {
        int nbrOfMatches = 0;
        int nbrOfMisses = 0;

        for (Text val : values) {
           String[] fields = val.toString().split(" ");
           nbrOfMatches += Integer.parseInt(fields[0]);
           nbrOfMisses += Integer.parseInt(fields[1]);
        }
        try { 
           // context.write(key, new Text(nbrOfMatches+" "+nbrOfMisses));
           multipleOutputs.write("text", key, new Text(nbrOfMatches+" "+nbrOfMisses), "counts-"+search);
        } catch (Throwable e) {
           // catch io errors from FileInputStream or readLine()
           System.out.println("Error: " + e + " " + e.getMessage());
        }
//        try { 
//           // context.write(key, new Text(nbrOfMatches+" "+nbrOfMisses));
//           context.write(key, new Text(nbrOfMatches+" "+nbrOfMisses));
//        } catch (Throwable e) {
//           // catch io errors from FileInputStream or readLine()
//           System.out.println("Error: " + e + " " + e.getMessage());
//        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        multipleOutputs.close();
    }
}

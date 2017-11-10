package nl.sara.hadoop;

import org.apache.hadoop.mapreduce.Mapper;

import java.util.*;
import org.apache.hadoop.io.*;
import java.util.regex.Pattern;
import java.util.regex.Matcher;
import org.apache.hadoop.conf.Configuration;
import java.util.Date;
import java.text.SimpleDateFormat;

public class MyMapper extends Mapper<Text, Text, Text, IntWritable> {

    @Override
    public void map(Text key, Text value, Context context) {
        String tweet = value.toString();
        String[] tokens = tweet.split(" ");
        for (int i=0; i<tweet.length(); i++) {
            try { 
               context.write(new Text(tokens[i]), new IntWritable(1));
               if (i > 0 && tokens[i-1].equals("#")) {
                  context.write(new Text("#"+tokens[i]), new IntWritable(1));
               }
            } catch (Throwable e) { // (IOException e) {
               System.out.println("Error: " + e + " " + e.getMessage());
            }
        }
    }
}

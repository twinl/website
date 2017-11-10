package nl.sara.hadoop;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.*;
import java.lang.Character;
import org.apache.hadoop.io.*;
import org.json.simple.*;
import org.json.simple.JSONArray;
import java.util.regex.Pattern;
import java.util.regex.Matcher;
import org.apache.hadoop.conf.Configuration;
import java.util.Date;
import java.text.SimpleDateFormat;

import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class MyMapper extends Mapper<LongWritable, Text, Text, Text> {
    @Override
    public void map(LongWritable key, Text value, Context context) {
        /* get JSON record (single line) and extract the tweet */
        String text;
        String screen_name;
        String tweet = value.toString();
        Map tweetMap = (Map) JSONValue.parse(tweet);
        if (tweetMap.get("user") != null) {
            Map user = (Map) tweetMap.get("user");
            screen_name = (String) user.get("screen_name");
            try {
               /* return tweets containing query */
               context.write(new Text(screen_name), new Text("1 0"));
            } catch (Throwable e) { // (IOException e) {
               // catch errors
               System.out.println("Error: " + e + " " + e.getMessage());
            }
            if (tweetMap.get("entities") != null) {
               Map entities = (Map) tweetMap.get("entities");
               if (entities.get("user_mentions") != null) {
                  List mentions =  (List) entities.get("user_mentions");
                  int i = 0;
                  while (i < mentions.size()) {
                     Map mentionMap = (Map) mentions.get(i);
                     String name = (String) mentionMap.get("screen_name");
                     if (! name.equals(screen_name)) {
                        try {
                           context.write(new Text(name), new Text("0 1"));
                        } catch (Throwable e) {
                           System.out.println("Error: " + e + " " + e.getMessage());
                        }
                     }
                     i++;
                  }
               }
            }
       }
    }
}

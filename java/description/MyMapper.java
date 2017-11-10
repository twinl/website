package nl.sara.hadoop;

import org.apache.hadoop.mapreduce.Mapper;

import java.util.*;
import org.apache.hadoop.io.*;
import java.util.regex.Pattern;
import java.util.regex.Matcher;
import org.apache.hadoop.conf.Configuration;
import java.util.Date;
import java.text.SimpleDateFormat;
import org.json.simple.*;

public class MyMapper extends Mapper<LongWritable, Text, Text, Text> {
    @Override
    public void map(LongWritable key, Text value, Context context) {
        /* get JSON record (single line) and extract the tweet */
        String tweet = value.toString();
        Map obj = (Map) JSONValue.parse(tweet);
        /* process the tweet, if available */
        if (obj != null && obj.get("user") != null &&
            (obj.get("twinl_lang") == null || obj.get("twinl_lang").equals("dutch"))) {
            Map user = (Map) obj.get("user");
            String description = (String) user.get("description");
            if (description != null && ! description.equals("")) {
               String user_id_str = (String) user.get("id_str");
               String screen_name = (String) user.get("screen_name");
               String name = (String) user.get("name");
               name = name.replaceAll("\\s"," ");
               description = description.replaceAll("\\s"," ");
               try { 
                  context.write(new Text(user_id_str),new Text(screen_name+"\t"+name+"\t"+description));
               } catch (Throwable e) { // (IOException e) {
                  System.out.println("Error: " + e + " " + e.getMessage());
               }
            }
        }
    }
}

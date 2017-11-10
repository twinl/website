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
import java.util.regex.Pattern;
import java.util.regex.Matcher;
import org.apache.hadoop.conf.Configuration;
import java.util.Date;
import java.text.SimpleDateFormat;

import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class MyMapper extends Mapper<LongWritable, Text, Text, Text> {
    private MultipleOutputs mos;

    @Override
    public void setup(Context context) {
        /* get search query from configuration */
        Configuration conf = context.getConfiguration();
        mos = new MultipleOutputs(context);
    }

    public boolean isOther(char c) {
        return(! Character.isLetter(c) && ! Character.isDigit(c) && ! Character.isWhitespace(c));
    }

    /**
     * This function holds the mapper logic
     * @param key The key of the K/V input pair
     onsimpl @param value The value of the K/V input pair
     * @param context The context of the application
     */
    @Override
    public void map(LongWritable key, Text value, Context context) {
        /* get JSON record (single line) and extract the tweet */
        String text;
        String tweet = value.toString();
        Map obj = (Map) JSONValue.parse(tweet);
        /* test if tweet contains text */
        if (obj != null && obj.get("text")!=null &&
            (obj.get("twinl_lang") == null || obj.get("twinl_lang").equals("dutch"))) {
            String id_str = (String) obj.get("id_str");
            text = (String) obj.get("text");
            /* tokenize tweet text */   
            char[] cArray = text.toCharArray();
            String tokenized = "";
            char lastC = ' ';
            for (int i=0;i<cArray.length;i++) {
               char c = cArray[i];
               if (isOther(c) && ! Character.isWhitespace(lastC)) {
                  tokenized += " "; // insert " " between char and nonchar
               } else if (isOther(lastC) && ! Character.isWhitespace(c)) {
                  tokenized += " ";
               }
               if (! Character.isWhitespace(c)) {
                  tokenized += c;
               } else if (! Character.isWhitespace(lastC)) {
                  tokenized += " "; // \w+ becomes ' '
               }
               lastC = c;
            }
            /* determine input file name */
            FileSplit IS = (FileSplit) context.getInputSplit();
            String IF = IS.getPath().toString();
            String[] IFList = IF.split("/");
            String fileName = IFList[IFList.length-1];
            // hdfs://p-head03.alley.sara.nl/user/eriktks/twitter/2012/09/01/20120901-05.out.gz
            // combine fileName and id and task in key
            // fileName = fileName + " " + id + " tok"; // cant we get task tok from configuration?
            try {
               /* return tweets containing query */
               mos.write("text",new Text(id_str), new Text(tokenized), "tok-"+fileName);
            } catch (Throwable e) { // (IOException e) {
               // catch errors
               System.out.println("Error: " + e + " " + e.getMessage());
            }
        }
    }

    protected void cleanup(Context context) throws IOException, InterruptedException {
        mos.close();
    }
}

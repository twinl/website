/******************************************************************************
 * Copyright 2012-2013 Erik Tjong Kim Sang / Netherlands eScience Center
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *****************************************************************************/

package nl.sara.hadoop;

import java.io.IOException;
import java.lang.InterruptedException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.util.*;
import java.util.Date;
import java.util.regex.Pattern;
import java.util.regex.Matcher;
import java.text.SimpleDateFormat;
import org.json.simple.*;

import cmu.arktweetnlp.Twokenize;

public class MyMapper extends Mapper<LongWritable, Text, Text, Text> {
    private Pattern p;
    private String date;
    private String hour;
    private int followers = 200;
    private int SPAMLENGTH = 100;
    private MultipleOutputs mos;
    Map<String,Object> maleNames = new HashMap<String,Object>();
    Map<String,Object> femaleNames = new HashMap<String,Object>();

    /* collect: tweets, users, relations, geo, counts */

    @Override
    public void setup(Context context) {
        // output to different data files
        mos = new MultipleOutputs(context);
        // get search query from configuration
        Configuration conf = context.getConfiguration();
        // split for sentiment search
        // cheack for user search: keyword is @user
        /* determine input file name */
        FileSplit IS = (FileSplit) context.getInputSplit();
        String IF = IS.getPath().toString();
        String[] IFList = IF.split("[/.-]");
        hour = IFList[IFList.length-3];
        date = IFList[IFList.length-4];
    } 

    @Override
    public void map(LongWritable key, Text value, Context context) {
        /* get JSON record (single line) and extract the tweet */
        String tweetAsText = value.toString();
        Map obj = (Map) JSONValue.parse(tweetAsText);
        /* process the tweet, if available */
        if (obj != null && obj.get("text") != null && obj.get("twinl_lang") != null && obj.get("twinl_lang").equals("dutch")) {
            String tweetText = (String) obj.get("text");
            if (obj.get("extended_tweet") != null) {
                Map extendedTweet = (Map) obj.get("extended_tweet");
                if (extendedTweet.get("full_text") != null) {
                    tweetText = (String) extendedTweet.get("full_text");
                }
            }
            String text = tweetText.replaceAll("\\s"," ");
            Pattern RTPattern = Pattern.compile("RT\\s+@.*",
                                Pattern.UNICODE_CASE);
            Matcher r = RTPattern.matcher(text);
            Pattern httpPattern = Pattern.compile(".*http.*",
                                  Pattern.CASE_INSENSITIVE |
                                  Pattern.UNICODE_CASE);
            Matcher h = httpPattern.matcher(text);
            // skip retweets and spam
            if (! r.matches() && (! h.matches() || text.length() <= SPAMLENGTH)) {
                List<String> toks = Twokenize.tokenizeRawTweetText(text);
                // Pattern userPattern = Pattern.compile("^@");
                Pattern userPattern = Pattern.compile("@.*",
                                      Pattern.CASE_INSENSITIVE |
                                    Pattern.UNICODE_CASE);
                Pattern urlPattern = Pattern.compile("http.*",
                                     Pattern.CASE_INSENSITIVE |
                                     Pattern.UNICODE_CASE);
                for (int i=0; i<toks.size(); i++) {
                    String token = toks.get(i);
                    Matcher u = userPattern.matcher(token);
                    if (u.matches()) { token = "@USER"; }              
                    Matcher p = urlPattern.matcher(token);
                    if (p.matches()) { token = "http://URL"; }              
                    try { context.write(new Text(token), new Text("1")); } 
                    catch (Throwable e) { System.out.println("Error: " + e + " " + e.getMessage()); }
                }
            }
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        mos.close();
    }
}

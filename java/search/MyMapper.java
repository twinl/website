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

public class MyMapper extends Mapper<LongWritable, Text, Text, Text> {
    private Pattern p;
    private String date;
    private String hour;
    private String search;
    private String searchTwinl = "";
    private int followers = 200;
    private MultipleOutputs mos;
    private int userSearch = 0;
    Map<String,Object> maleNames = new HashMap<String,Object>();
    Map<String,Object> femaleNames = new HashMap<String,Object>();
    GuessAge ga = new GuessAge();
    Sentiment sent = new Sentiment();

    /* collect: tweets, users, relations, geo, counts */

    @Override
    public void setup(Context context) {
        // output to different data files
        mos = new MultipleOutputs(context);
        // get search query from configuration
        Configuration conf = context.getConfiguration();
        search = conf.get("search");
        // split for sentiment search
        String[] list = search.split("[+]");
        // test if search keyword has special format: twinl-*
        Pattern q = Pattern.compile("twinl-.*",
                           Pattern.CASE_INSENSITIVE |
                           Pattern.UNICODE_CASE);
        Matcher m = q.matcher(search);
        if (m.matches()) {
            // special format found
            // test if the search keyword contains a second part: *+* 
            if (list.length > 1) {
                // second part found
                // put task in variable searchTwinl and search keyword (part 2 ) in variable search
                searchTwinl = list[0];
                search = list[1];
                for (int i=2;i<list.length;i++) { search += "+"+list[i]; }
                // process followers keywords: twinl-followers-(min|max)-number
                String[] twinlList = searchTwinl.split("[-]");
                if (twinlList.length > 2 && twinlList[1].equals("followers")) {
                   searchTwinl = "twinl-followers-"+twinlList[2];
                   if (twinlList.length > 3) { followers = Integer.parseInt(twinlList[3]); }
                }
            } else {
                // no second part found: search for everything
                searchTwinl = search;
                if (! search.equals("twinl-smiley") && ! search.equals("twinl-frowny")) { search = "echtalles"; }
                // process followers keywords: twinl-followers-(min|max)-number
                String[] twinlList = searchTwinl.split("[-]");
                if (twinlList.length > 2 && twinlList[1].equals("followers")) {
                   searchTwinl = "twinl-followers-"+twinlList[2];
                   if (twinlList.length > 3) { followers = Integer.parseInt(twinlList[3]); }
                }

            }
        }
        // check for user search: keyword is @user
        char[] sArray = search.toCharArray();
        // removed on 20180125
        // if (sArray[0] == '@') {
        //    // user search detected
        //    char[] newArray = new char[sArray.length-1];
        //    for (int i=1;i<sArray.length;i++) { newArray[i-1] = sArray[i]; }
        //    userSearch = 1;
        //    search = new String(newArray);
        //    p = Pattern.compile(".*\\b"+search+"\\b.*",
        //                        Pattern.CASE_INSENSITIVE |
        //                        Pattern.UNICODE_CASE);
        // } else 
        if (search.equals("twinl-smiley")) {
            p = Pattern.compile(".*(:-\\)|:\\)).*",
                                Pattern.CASE_INSENSITIVE |
                                Pattern.UNICODE_CASE);
        } else if (search.equals("twinl-frowny")) {
            p = Pattern.compile(".*(:-\\(|:\\().*",
                                Pattern.CASE_INSENSITIVE |
                                Pattern.UNICODE_CASE);
        } else {
            // no user search
            char[] newArray = new char[sArray.length];
            for (int i=0;i<sArray.length;i++) { newArray[i] = sArray[i]; }
            search = new String(newArray);
            if (newArray[0] == '#') { // \b# matches nothing so omit one \b
               p = Pattern.compile(".*"+search+"\\b.*",
                                   Pattern.CASE_INSENSITIVE |
                                   Pattern.UNICODE_CASE);
            } else {
               p = Pattern.compile(".*\\b"+search+"\\b.*",
                                   Pattern.CASE_INSENSITIVE |
                                   Pattern.UNICODE_CASE);
            }
        }
        /* determine input file name */
        FileSplit IS = (FileSplit) context.getInputSplit();
        String IF = IS.getPath().toString();
        String[] IFList = IF.split("[/.-]");
        hour = IFList[IFList.length-3];
        date = IFList[IFList.length-4];
        ga.setup();
        sent.setup();
    } 

    private Collection processGeo(Object geo) {
        Map geoObj = (Map) geo;
        Collection list = new LinkedList();
        String c1 = "";
        String c2 = "";
        if (geoObj.get("coordinates") != null) {
            try {
                JSONArray array = (JSONArray) geoObj.get("coordinates"); // object
                c1 = array.get(0).toString();
                c2 = array.get(1).toString();
            } catch (Throwable e) {
                System.out.println("Error: " + e + " " + e.getMessage());
            }
            try { // store location
                mos.write("text", NullWritable.get(), new Text(c1+" "+c2),"geo-"+date+"-"+hour+"-"+search);
            } catch (Throwable e) {
                System.out.println("Error: " + e + " " + e.getMessage());
            }
        }
        list.add(c1);
        list.add(c2);
        return(list);
    }

    private void countTweets(Context context, Object inDate, int match, int miss) {
        String datePlusHour = "????";
        String tmpDate = "????";
        String tmpHour = "????";
        if (inDate != null) {
            /* date format: Fri Aug 31 22:00:08 +0000 2012 */
            String dateString = (String) inDate;
            String expectedPattern = "EEE MMM dd HH:mm:ss ZZZZZ yyyy";
            SimpleDateFormat formatIn = new SimpleDateFormat(expectedPattern);
            try {
                Date parsedDate = formatIn.parse(dateString);
                /* convert inDate to format: date hour minutes */
                SimpleDateFormat formatOut = new SimpleDateFormat("yyyyMMddHHmm");
                datePlusHour = formatOut.format(parsedDate);
                SimpleDateFormat formatOutD = new SimpleDateFormat("yyyyMMdd");
                tmpDate = formatOutD.format(parsedDate);
                SimpleDateFormat formatOutH = new SimpleDateFormat("HH");
                tmpHour = formatOutH.format(parsedDate);
            } catch (Throwable e) {
                System.out.println("Error: " + e + " " + e.getMessage());
            }
        }
        // only count date and times that belong to this file
        // if (tmpDate.equals(date) && tmpHour.equals(hour)) { // condition removed 20131115: data problem 2011/05/31
            try {
                /* count tweets */
                context.write(new Text(datePlusHour), new Text(match+" "+miss));
            } catch (Throwable e) { // (IOException e) {
                // catch errors
                System.out.println("Error: " + e + " " + e.getMessage());
            }
        // }
    }

    public String processTweet(LongWritable key, Text value, Context context, Map obj, String outFileBase) {
        /* test if tweet contains query */
        String tweetText = (String) obj.get("text");
        if (obj.get("extended_tweet") != null) {
            Map extendedTweet = (Map) obj.get("extended_tweet");
            if (extendedTweet.get("full_text") != null) {
                tweetText = (String) extendedTweet.get("full_text");
            }
        }
        String text = tweetText.replaceAll("\\s"," ");
        // keep retweets
        Matcher m = p.matcher(text);
        /* we are interested in tweet text, hour, minutes and gps coordinates  */
        // check if tweet text contains search query
        String match = "0";
        String searchName = search;
        /* date format: Fri Aug 31 22:00:08 +0000 2012 */
        String dateString = (String) obj.get("created_at");
        String expectedPattern = "EEE MMM dd HH:mm:ss ZZZZZ yyyy";
        SimpleDateFormat formatIn = new SimpleDateFormat(expectedPattern);
        String tweetDate = "";
        String tweetHour = "";
        try {
            Date parsedDate = formatIn.parse(dateString);
            /* convert date to format: hour minutes */
            SimpleDateFormat formatDate = new SimpleDateFormat("yyyy-MM-dd");
            SimpleDateFormat formatHour = new SimpleDateFormat("HH:mm:ss");
            tweetDate = formatDate.format(parsedDate);
            tweetHour = formatHour.format(parsedDate);
        } catch (Throwable e) {
            System.out.println("Error: " + e + " " + e.getMessage());
        }
        String screen_name = "";
        String retweeted_status_id_str = "null";
        String in_reply_to_screen_name = "";
        String in_reply_to_status_id_str = "null";
        String name = "";
        String user_id_str = "";
        String gender = "";
        int sentiment = sent.computeSentiment(tweetText,"","","","");
        int age = -1;
        Map user = null;
        String description = "";
        int nbrOfFollowers = -1;
        if (obj.get("user") != null) {
            user = (Map) obj.get("user");
            screen_name = (String) user.get("screen_name");
            if (user.get("in_reply_to_screen_name") != null) {
               in_reply_to_screen_name = (String) user.get("in_reply_to_screen_name");
            }
            name = (String) user.get("name");
            user_id_str = (String) user.get("id_str");
            description = (String) user.get("description");
            if (description != null && name != null && screen_name != null) {
                age = Integer.parseInt(ga.guessAge(screen_name,description));
                gender = ga.guessGender(screen_name,name,description);
            }
            if (user.get("followers_count") != null) {
                Long tmpLong = (Long) user.get("followers_count");
                nbrOfFollowers = tmpLong.intValue();
            }
        }
        if (obj.get("in_reply_to_status_id_str") != null) {
           in_reply_to_status_id_str = (String) obj.get("in_reply_to_status_id_str");
        }
        if (obj.get("retweeted_status") != null) {
           Map original_tweet = (Map) obj.get("retweeted_status");
           if (original_tweet.get("id_str") != null) {
              retweeted_status_id_str = (String) original_tweet.get("id_str");
           }
        }
        if (m.matches() || 
            search.equals("echtalles") || 
            outFileBase.equals("original") ||
            (userSearch > 0 && screen_name.toLowerCase().equals(search.toLowerCase())) ||
            (userSearch > 0 && in_reply_to_screen_name.toLowerCase().equals(search.toLowerCase()))) {
            if (searchTwinl.equals("") ||
                searchTwinl.equals("twinl-smiley") || searchTwinl.equals("twinl-frowny") ||
                outFileBase.equals("original") ||
                (searchTwinl.equals("twinl-retweet") && ! retweeted_status_id_str.equals("null")) ||
                (searchTwinl.equals("twinl-sent-pos") && sentiment > 0) ||
                (searchTwinl.equals("twinl-sent-neg") && sentiment < 0) ||
                (searchTwinl.equals("twinl-gender-m") && gender.equals("M")) ||
                (searchTwinl.equals("twinl-gender-f") && gender.equals("F")) ||
                (searchTwinl.equals("twinl-age-17") && age >=  0 && age <18) ||
                (searchTwinl.equals("twinl-age-21") && age >= 18 && age <26) ||
                (searchTwinl.equals("twinl-age-26") && age >= 26 && age <90) ||
                (searchTwinl.equals("twinl-followers-min") && nbrOfFollowers >= followers) ||
                (searchTwinl.equals("twinl-followers-max") && nbrOfFollowers <= followers && nbrOfFollowers != -1) ||
                (searchTwinl.equals("twinl-geo") && obj.get("geo") != null)) {
                String id_str = (String) obj.get("id_str");
                if (! searchTwinl.equals("") && ! search.equals("echtalles") && ! search.equals("twinl-smiley") && ! search.equals("twinl-frowny")) {
                   searchName = searchTwinl+"+"+search;
                }
                // process gps coordinates
                Collection list = new LinkedList();
                if (obj.get("geo") != null) { list = processGeo(obj.get("geo")); }
                // 20130325: only add loc data when there is no geo data
                if (list == null && user.get("location") != null) { 
                   String location = user.get("location").toString();
                   if (! location.equals("") && outFileBase.equals("text")) {
                        try { /* store tweet */
                            mos.write("text", NullWritable.get(), new Text(user.get("location").toString()),"loc-"+date+"-"+hour+"-"+searchName);
                        } catch (Throwable e) {
                            System.out.println("Error: " + e + " " + e.getMessage());
                        }
                    }
                }
                // store tweet in text format
                try {
                   if (searchTwinl.equals("twinl-geo")) {
                      Iterator i = list.iterator();
                      mos.write("text", NullWritable.get(), new Text(id_str+"\t"+user_id_str+"\t"+tweetDate+"\t"+tweetHour+"\t"+in_reply_to_status_id_str+"\t"+retweeted_status_id_str+"\t"+screen_name+"\t"+text+"\t"+i.next()+"\t"+i.next()),outFileBase+"-"+date+"-"+hour+"-"+searchName);
                   } else {
                      mos.write("text", NullWritable.get(), new Text(id_str+"\t"+user_id_str+"\t"+tweetDate+"\t"+tweetHour+"\t"+in_reply_to_status_id_str+"\t"+retweeted_status_id_str+"\t"+screen_name+"\t"+text),outFileBase+"-"+date+"-"+hour+"-"+searchName);
                   }
                } catch (Throwable e) {
                   System.out.println("Error: " + e + " " + e.getMessage());
                }
                if (outFileBase.equals("text")) {
                    // store tokenized tweet
                    String tokenized = tokenize.tokenize(text);
                    try { // store tokenized tweet
                       mos.write("textPair", new Text(id_str), new Text(tokenized),"tok-"+date+"-"+hour+"-"+searchName);
                    } catch (Throwable e) {
                       System.out.println("Error: " + e + " " + e.getMessage());
                    }
                }
                if (obj.get("retweeted_status") != null) {
                    Map original_tweet = (Map) obj.get("retweeted_status");
                    if (original_tweet.get("id_str") != null) {
                        // process original tweet
                        String tmp = processTweet(key,value,context,original_tweet,"original");
                    }
                }
                match = "1";
            }
        }
        return(match+" "+date+"-"+hour+"-"+searchName);
    }

    @Override
    public void map(LongWritable key, Text value, Context context) {
        /* get JSON record (single line) and extract the tweet */
        String tweetAsText = value.toString();
        Map obj = (Map) JSONValue.parse(tweetAsText);
        /* process the tweet, if available */
        if (obj != null && obj.get("text") != null &&
            /* 20130821 added Swedish word gött for Anja Schuppert */
            /* 20181012 removed it */
            (obj.get("twinl_lang") == null || obj.get("twinl_lang").equals("dutch"))) {

            /* get JSON record (single line) and extract the tweet */
            String returnString = processTweet(key,value,context,obj,"text");
            String[] returnStrings = returnString.split(" ");
            int match = Integer.parseInt(returnStrings[0]);
            String fileTail = returnStrings[1];
            if (match == 1) {
               // store tweet in json format
               try {
                  mos.write("text", NullWritable.get(), new Text(tweetAsText),"tweets-"+fileTail);
               } catch (Throwable e) {
                  System.out.println("Error: " + e + " " + e.getMessage());
               }
            }
            // prepare counts for reduce phase 
            // for sentiment searches: only count relevant nonmatching tweets
            // first check if query requests sentiment information
            Pattern q = Pattern.compile("twinl-sent-.*",
                                Pattern.CASE_INSENSITIVE |
                                Pattern.UNICODE_CASE);
            Matcher t = q.matcher(searchTwinl);
            // second check if tweets matches query
            String tweetText = (String) obj.get("text");
            if (obj.get("extended_tweet") != null) {
                Map extendedTweet = (Map) obj.get("extended_tweet");
                if (extendedTweet.get("full_text") != null) {
                    tweetText = (String) extendedTweet.get("full_text");
                }
            }
            String text = tweetText.replaceAll("\\s"," ");
            Matcher m = p.matcher(text);
            if ((! t.matches()) || m.matches() || search.equals("echtalles")) {
               countTweets(context, obj.get("created_at"), match, 1-match);
            }
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        mos.close();
    }
}

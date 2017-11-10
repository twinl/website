package nl.sara.hadoop;

import java.io.IOException;
import java.lang.InterruptedException;
import java.io.*;

import java.util.*;
import java.util.Date;
import java.util.regex.Pattern;
import java.util.regex.Matcher;
import java.text.SimpleDateFormat;

public class SentimentNew {
    static Map<String,Object> positive = new HashMap<String,Object>();
    static Map<String,Object> negative = new HashMap<String,Object>();
    static boolean debugMode = false;

    private static String splitCamelCaseNew(String text) {
        char[] cArray = text.toCharArray();
        String tokenized = "";
        char lastC = ' ';
        for (int i=0;i<cArray.length;i++) {
           if (Character.isLowerCase(lastC) && Character.isUpperCase(cArray[i])) {
              tokenized += " "; // insert " " between lower case char and upper case char
           }
           tokenized += cArray[i];
           lastC = cArray[i];
        }
        return(tokenized);
    }

    public static void setupNew() {
        // source: http://www.roseindia.net/java/beginners/java-read-file-line-by-line.shtml
        try {
            FileInputStream fstream = new FileInputStream("/home/cloud/tmp/pos");
            DataInputStream in = new DataInputStream(fstream);
            BufferedReader br = new BufferedReader(new InputStreamReader(in));
            String strLine;
            while ((strLine = br.readLine()) != null) { positive.put(strLine,1); }
            in.close();
        } catch (Exception e) { System.err.println("Error: " + e.getMessage()); }
        try {
            FileInputStream fstream = new FileInputStream("/home/cloud/tmp/neg");
            DataInputStream in = new DataInputStream(fstream);
            BufferedReader br = new BufferedReader(new InputStreamReader(in));
            String strLine;
            while ((strLine = br.readLine()) != null) { negative.put(strLine,1); }
            in.close();
        } catch (Exception e) { System.err.println("Error: " + e.getMessage()); }
//        String[] listP = {};
//        for (int i=0;i<listP.length;i++) { 
//            String[] listwords = listP[i].split(" ");
//            for (int j=0;j<listwords.length;j++) {
//                positive.put(listwords[j],1); 
//            }
//        };
//        String[] listN = {};
//        for (int i=0;i<listN.length;i++) { 
//            String[] listwords = listN[i].split(" ");
//            for (int j=0;j<listwords.length;j++) {
//                negative.put(listwords[j],1); 
//            }
//        }
    } 

    public static int computeSentimentNew(String tweet, String userId, String tweetId, String latitude, String longitude) {
        tweet = tweet.toLowerCase();
        tweet = splitCamelCaseNew(tweet);
        tweet = tokenize.tokenize(tweet);
        String[] list = tweet.split(" ");
        int sentiment = 0;
        int total = 0;
        String out = "";
        String last = "";
        String last2 = "";
        String last3 = "";
        for (int i=0;i<list.length;i++) {
            last3 = last2+list[i];
            last2 = last+list[i];
            if (positive.containsKey(list[i]) || positive.containsKey(last2) || positive.containsKey(last3)) { sentiment = 1; total++; out += " +++"+list[i]; }
            else {
               if (negative.containsKey(list[i]) || negative.containsKey(last2) || negative.containsKey(last3)) { sentiment = -1; total--; out += " ---"+list[i]; }
               else { out += " "+list[i]; }
            }
            // removed from previous loop 20130325
            // negation of negative sentiment => positive
            if ((last.equals("geen") || last.equals("niet") || last.equals("not")) && negative.containsKey(last) && negative.containsKey(list[i])) { sentiment = 1; total += 2; }
            if ((last.equals("geen") || last.equals("niet") || last.equals("not")) && negative.containsKey(last) && positive.containsKey(list[i])) { sentiment = -1; total -= 2; }
            last = list[i];
        }
        if (total > 0) { sentiment = 1; }
        else if (total < 0) { sentiment = -1; }
        if (debugMode) {
           if (sentiment <= 0) { System.out.println(sentiment+"\t"+tweetId+"\t"+userId+"\t"+out+"\t"+latitude+"\t"+longitude); }
           else { System.out.println("+"+sentiment+"\t"+tweetId+"\t"+userId+"\t"+out+"\t"+latitude+"\t"+longitude); }
        }
        return(sentiment);
    }

    public static void main(String args[]) {
        BufferedReader stdin = new BufferedReader(new InputStreamReader(System.in));
        String line = "";
        setupNew();

        double pos = 0;
        double neg = 0;
        double neu = 0;
        if (args.length >= 1) { debugMode = true; }
        while (true) {
            try { line = stdin.readLine(); }
            catch (Throwable e) { System.out.println("Error: " + e + " " + e.getMessage()); }
            if (line == null) break;
            String[] fields = line.split("\t");
            if (fields.length >= 8) {
                int sentiment = 0;
                if (fields.length < 10) { sentiment = computeSentimentNew(fields[7],fields[1],fields[0],"0","0"); }
                else { sentiment = computeSentimentNew(fields[7],fields[1],fields[0],fields[8],fields[9]); }
                if (sentiment > 0) { pos++; }
                else { 
                   if (sentiment < 0) { neg++; }
                   else { neu++; }
                }
            }
        }
        if (pos+neg+neu > 0 && ! debugMode) { System.out.println((pos-neg)/(pos+neg+neu)); }
    }
}

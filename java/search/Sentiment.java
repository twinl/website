package nl.sara.hadoop;

import java.io.IOException;
import java.lang.InterruptedException;
import java.io.*;

import java.util.*;
import java.util.Date;
import java.util.regex.Pattern;
import java.util.regex.Matcher;
import java.text.SimpleDateFormat;

public class Sentiment {
    static Map<String,Object> positive = new HashMap<String,Object>();
    static Map<String,Object> negative = new HashMap<String,Object>();
    static boolean debugMode = false;

    private static String splitCamelCase(String text) {
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

    public static void setup() {
        String[] list = "hoera gelukkig blij vrolijk vrolijke vrolijker blije blijer haha hahaha hihi hihihi xd winnen win wint gewonnen ja yes whoehoe wajoo nice mooi mooier mooie mooist lach lacht lachen dank bedankt dankje goed gelukkig gelukkige gelukkiger geluk gelukt leuk leuker leukst aardig aardiger aardigst veel luxe geniaal genialer geniaalst happy lol lekker meest fijn fijne sexy sexier mooiso goed beter best beste wel fack hou succes cool gaaf top prachtige prachtig :) ;) :-) ;-) :b :d :> :-b :-d :-> =) trots super briljant amazing geweldig geweldige grapje grap grappen plezier xx :p :$ :-$ :') lt;3 <3 smile smiley ok oke okej okey okay".split(" ");
        for (int i=0;i<list.length;i++) { positive.put(list[i],1); } 
        list = "fail slecht boos bozer arrogant arrogante haat haten kut lul gvd fuck fucking fuckin faal faalhaas faalhazen slecht slechter slechtst rot rotter rotst eindeloos eindeloze nergens verlies verliest verliezen verloren argh bah nee no vadsig vadsige crimineel criminele schurk schurken boef boeven bandiet bandieten rover rovers sukkel sukkels liegen lieg liegt jokken jok jokt liegbeest liegbeesten jokkebrok jokkebrokken achterlijk achterlijke dom domme sukkel sukkels smoesje smoesjes pijn klaag klaagt klagen huil huilt huilen verdriet verdrietig fuckte homo niet weinig wijf wijven lui chagerijnig chagerijnige chagerijniger ranzig ranziger ranzigst minder minst fout fouter foutst fouten corrupt corruptst shit paardenlul irriteer irriteert irriteren vies vieze last dwing dwingt dwingen gedwongen sla slaat slaan geslagen idioot idiote idioter gek gekker kaulo holo hoer hoeren tering kolere kanker godverdomme verkakt kk konjo bek irritant minacht minachting meh holy mislukt mislukking mislukte foei jezus erg erge nare zelfmoord verwend gezeik :( ;( :< :-( ;-( :-< =( onzin jank jankt janken huil huilt huilen schreeuw schreeuwt schreeuwen lauw lauwe raar rare nep cc fck watje nondejuu nondeju flikker debiel debiele ongelooflijke ongelooflijk mongoool mongolen stink stinkt stinken ruzie ruzies ruzien stress gestresst bedrieg bedriegt bedriegen bot schok schokkend shocking probleem problemen sterf sterft sterven scheld scheldt schelden gescheld schold scholden ziekte ziekten ziektes zooi boeit boeien pff pfff spoort sporen kloot klote klootzak klootzakken ziek scheldwoord scheldwoordennrespectloosi leugen slet sletteni tyfus zwak schokkend schokkende godver not geen".split(" ");
        for (int i=0;i<list.length;i++) { negative.put(list[i],1); } 
    } 

    public static int computeSentiment(String tweet, String userId, String tweetId, String latitude, String longitude) {
        tweet = tweet.toLowerCase();
        tweet = splitCamelCase(tweet);
        tweet = tokenize.tokenize(tweet);
        String[] list = tweet.split(" ");
        int sentiment = 0;
        String out = "";
        String last = "";
        String last2 = "";
        String last3 = "";
        int total = 0;
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
        setup();

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
                if (fields.length < 10) { sentiment = computeSentiment(fields[7],fields[1],fields[0],"0","0"); }
                else { sentiment = computeSentiment(fields[7],fields[1],fields[0],fields[8],fields[9]); }
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

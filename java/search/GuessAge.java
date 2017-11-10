package nl.sara.hadoop;

import java.io.IOException;
import java.lang.InterruptedException;
import java.io.*;

import java.util.*;
import java.util.Date;
import java.util.regex.Pattern;
import java.util.regex.Matcher;
import java.text.SimpleDateFormat;

public class GuessAge {
    static Map<String,Object> maleNames = new HashMap<String,Object>();
    static Map<String,Object> femaleNames = new HashMap<String,Object>();

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
        String[] list = "sander mark max mike bart bas danny dennis eric frank hans henk jan jeroen john joost kevin marcel marco mark martijn martin michel nick niels peter rick rob robert roy ruben sander thomas tim tom vincent michael patrick jordy stefan wouter jeffrey lars jasper david marc maarten jesse daniel paul chris jelle thijs erik alex bram stephan willem joey richard pieter luuk ronald pako michiel johan stan bob wesley pim justin dylan rik andre christian ben bo steven niek jordi remco jos sven arjan sjoerd gijs leon stijn wim menno matthijs brian ramon luc edwin kees jamie julian simon charles bjorn alexander roel harry glenn giovanni ruud yannick rico gerard geert dirk mitchell hugo mathijs luke guido frans dave ton ricardo edwin timo rutger roger robbert nico maurice lucas job angelo daan mr pascal muhammad erwin herman andreas jens floris bryan ryan jari andy thom jim dion melvinleo jimmy jaap lorenzo jesper jelmer tobias rens mehmet casper theo ron kayliegh adam".split(" ");
        for (int i=0;i<list.length;i++) { maleNames.put(list[i],1); } 
        list = "laura lisa kim anne sanne anouk michelle romy iris melissa amber demi anna britt mandy chantal daphne demi denise ellen eva femke fleur ilse inge jennifer jessica joyce judith julia karin kelly linda lisanne lotte maaike manon marieke melanie melissa merel michelle monique naomi nina petra romy roos sabine sandra saskia sharon sophie tessa vera wendy yvonne esther emma rianne sarah eline marjolein claudia danielle nicole esmee tamara dominique danique charlotte susan kimberley samantha nienke carmen alexis kirsten suzanne myrthe ingrid nikk amy sofie stephanie celine anita simone marit floor angela maud nathalie cheyenne priscilla marije bianca sara astrid natascha jolanda rosa marleen larissa kyra patricia maria elise marianne lynn kimberley jette dani rachel marloes jenny caroline mariska lieke kirsten cindy vanessa tara renate emily kimberly miss ashley nikki nadia lisette irene rebecca nadine nicky mirjam maartje isa angelique janine cynthia leonie miranda marie brenda janneke amanda pauline nunke loes kiki daisy veerle louise lara".split(" ");
        for (int i=0;i<list.length;i++) { femaleNames.put(list[i],1); } 
    } 

    public static String guessGender(String screen_name,String name,String description) {
        screen_name = splitCamelCase(screen_name);
        screen_name = screen_name.toLowerCase();
        screen_name = screen_name.replaceAll("x","x ");
        screen_name = tokenize.tokenize(screen_name);
        String[] list = screen_name.split(" ");
        for (int i=0;i<list.length;i++) {
            if (maleNames.containsKey(list[i])) { return("M"); }
            if (femaleNames.containsKey(list[i])) { return("F"); }
        }
        name = name.toLowerCase();
        name = tokenize.tokenize(name);
        list = name.split(" ");
        for (int i=0;i<list.length;i++) {
            if (maleNames.containsKey(list[i])) { return("M"); }
            if (femaleNames.containsKey(list[i])) { return("F"); }
        }

        Pattern p = Pattern.compile(".*\\b(papa|vader|grootvader|opa|echtgenoot|dad|husband|male)\\b.*",
                            Pattern.CASE_INSENSITIVE |
                            Pattern.UNICODE_CASE);
        Matcher m = p.matcher(description);
        if (m.matches()) { return("M"); }

        p = Pattern.compile(".*\\b(mama|moeder|grootmoeder|oma|echtgenote|vrouw|mom|wife)\\b.*",
                            Pattern.CASE_INSENSITIVE |
                            Pattern.UNICODE_CASE);
        m = p.matcher(description);
        if (m.matches()) { return("F"); }

        return("");
    }

    public static String guessAge(String screen_name,String description) {
        Pattern p;
        int thisYear = Calendar.getInstance().get(Calendar.YEAR);

        p = Pattern.compile(".*\\b(\\d\\d)\\s*(jaar|jaren|jaartjes|year|y/o).*",
                            Pattern.CASE_INSENSITIVE |
                            Pattern.UNICODE_CASE);
        Matcher m = p.matcher(description);
        if (m.matches()) { return(m.group(1)); }
       
 
        p = Pattern.compile(".*(19\\d\\d|2000|2001|2002|2003)",
                            Pattern.CASE_INSENSITIVE |
                            Pattern.UNICODE_CASE);
        m = p.matcher(screen_name);
        if (m.matches()) { 
            int year = Integer.parseInt(m.group(1));
            return(Integer.toString(thisYear-year));
        }

        p = Pattern.compile(".*(docent|directeur|manager|getrouwd|kids|kinderen|kindjes|married|partner|papa|vader|echtgenoot|dad|husband|mama|moeder|echtgenote|mom|wife).*",
                            Pattern.CASE_INSENSITIVE |
                            Pattern.UNICODE_CASE);
        m = p.matcher(description);
        if (m.matches()) { return("35"); }

        p = Pattern.compile(".*(student|hogeschool|universiteit).*",
                            Pattern.CASE_INSENSITIVE |
                            Pattern.UNICODE_CASE);
        m = p.matcher(description);
        if (m.matches()) { return("21"); }

        p = Pattern.compile(".*(college|gymnasium|havo|mavo|klas|vmbo|vwo).*",
                            Pattern.CASE_INSENSITIVE |
                            Pattern.UNICODE_CASE);
        m = p.matcher(description);
        if (m.matches()) { return("15"); }

        p = Pattern.compile(".*\\b(school)\\b.*",
                            Pattern.CASE_INSENSITIVE |
                            Pattern.UNICODE_CASE);
        m = p.matcher(description);
        if (m.matches()) { return("15"); }

        p = Pattern.compile(".*\\b(19\\d\\d|2000|2001|2002)\\b.*",
                            Pattern.CASE_INSENSITIVE |
                            Pattern.UNICODE_CASE);
        m = p.matcher(description);
        if (m.matches()) { 
            int year = Integer.parseInt(m.group(1));
            return(Integer.toString(thisYear-year));
        }

        return("-1");
    }

    /* offline usage:
       hadoop fs -cat cache/DATE/Search/QUERY/tweets-* | 
          gunzip -c |
          getJsonFields id_str,user/id_str,user/screen_name,user/name,user/description |
          java nl/sara/hadoop/GuessAge debug
    */
    public static void main(String args[]) {
        int age17 = 0;
        int age21 = 0;
        int age26 = 0;
        int genderM = 0;
        int genderF = 0;

        BufferedReader stdin = new BufferedReader(new InputStreamReader(System.in));
        String line = "";
        setup();

        while (true) {
            try { line = stdin.readLine(); }
            catch (Throwable e) { System.out.println("Error: " + e + " " + e.getMessage()); }
            if (line == null) break;
            String[] fields = line.split("\t");
            if (fields.length > 3) {
                //String name = "";
                //if (fields.length > 3) { name = fields[3]; }
                //String description = "";
                //if (fields.length >= 4) { description = fields[4]; }
                int age = Integer.parseInt(guessAge(fields[1],fields[3]));
                String gender = guessGender(fields[1],fields[2],fields[3]);
                if (age >= 10 && age < 18) { age17++; }
                else if (age >= 18 && age < 26) { age21++; }
                else if (age >= 26 && age < 90) { age26++; }
                if (gender.equals("M")) { genderM++; }
                else if (gender.equals("F")) { genderF++; }
                // if called with argument: give analysis for every tweet
                if (args.length > 0) { System.out.println(age+"\t"+gender+"\t"+line); }
            }
        }
        // if called without argument: give summary of analysis
        if (args.length <= 0) {
           if (age17+age21+age26 > 0) {
                int total = age17+age21+age26;
                int perc17 = 100*age17/total;
                int perc21 = 100*age21/total;
                int perc26 = 100*age26/total;
                // make sure total is equal to 100%
                total = perc17+perc21+perc26;
                if (perc17 > perc21 && perc17 > perc26) { perc17 += 100-total; }
                else if (perc21 > perc17 && perc21 > perc26) { perc21 += 100-total; }
                else { perc26 += 100-total; }
                System.out.println("leeftijd <  18: "+Integer.toString(perc17)+"%"); 
                System.out.println("leeftijd <  26: "+Integer.toString(perc21)+"%"); 
                System.out.println("leeftijd >= 26: "+Integer.toString(perc26)+"%"); 
            } else {
                System.out.println("leeftijd <  18: 0%\nleeftijd <  26: 0%\nleeftijd >= 26: 0%");
            }
            if (genderM+genderF > 0) {
                int total = genderM+genderF;
                int percM = 100*genderM/total;
                int percF = 100*genderF/total;
                // make sure total is equal to 100%
                total = percM+percF;
                if (percM > percF) { percM += 100-total; }
                else { percF += 100-total; }
                System.out.println("        mannen: "+Integer.toString(percM)+"%"); 
                System.out.println("       vrouwen: "+Integer.toString(percF)+"%"); 
            } else {
                System.out.println("        mannen: 0%\n       vrouwen: 0%");
            }
        }
    }
}

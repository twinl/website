package nl.sara.hadoop;

import java.io.*;
import java.util.*;

public class tokenize {
    public static boolean isOther(char c) {
        return(! Character.isLetter(c) && ! Character.isDigit(c) && ! Character.isWhitespace(c));
    }

    public static String tokenize(String text) {
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
           } else if (Character.isUpperCase(c) && Character.isLowerCase(lastC)) {
              tokenized += " ";
           }
           if (! Character.isWhitespace(c)) {
              tokenized += c;
           } else if (! Character.isWhitespace(lastC)) {
              tokenized += " "; // \w+ becomes ' '
           }
           lastC = c;
        }
        return(tokenized);
    }

    public static void main(String args[]) { 
        InputStreamReader converter = null;
        try { converter = new InputStreamReader(System.in, "UTF-8"); }
        catch (Throwable e) { System.out.println("Error: " + e + " " + e.getMessage()); }
        BufferedReader in = new BufferedReader(converter);
        // get input line
        String CurLine = "";
        try { CurLine = in.readLine(); } 
        catch (Throwable e) { System.out.println("Error: " + e + " " + e.getMessage()); }
        while (CurLine != null) {
            String tokenized = tokenize(CurLine);
            System.out.println(tokenized);
            try { CurLine = in.readLine(); } 
            catch (Throwable e) { System.out.println("Error: " + e + " " + e.getMessage()); }
        }
    } 
}

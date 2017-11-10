package nl.sara.hadoop;

import java.io.IOException;
import java.lang.Math;

import org.apache.hadoop.io.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Reducer;

public class MyReducer extends Reducer<Text, Text, Text, Text> {
    String subtract = "no";

    public void setup(Context context) {
        // get search query from configuration 
        Configuration conf = context.getConfiguration();
        subtract = conf.get("subtract");
    }

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) {
        double frequencyQuery = 0.0;
        double frequencyReference = 0.0;
        double typesQuery = 0.0;
        double typesReference = 0.0;
        double tokensQuery = 0.0;
        double tokensReference = 0.0;
        long frequencyQueryLong = 0;
        long frequencyReferenceLong = 0;
        long typesQueryLong = 0;
        long typesReferenceLong = 0;
        long tokensQueryLong = 0;
        long tokensReferenceLong = 0;
        // process the data associated with a word
        for (Text val : values) {
            String s = val.toString();
            s = s.replaceAll("\n"," ");
            String[] fields = s.split(" ");
            // if this data is query data
            if (fields[1].equals("yes")) { // store query data
                frequencyQueryLong = Long.parseLong(fields[0]); frequencyQuery = Double.parseDouble(fields[0]);
                typesQueryLong = Long.parseLong(fields[2]); typesQuery = Double.parseDouble(fields[2]);
                tokensQueryLong = Long.parseLong(fields[3]); tokensQuery = Double.parseDouble(fields[3]);
            } else { // store reference data
                frequencyReferenceLong = Long.parseLong(fields[0]); frequencyReference = Double.parseDouble(fields[0]);
                typesReferenceLong = Long.parseLong(fields[2]); typesReference = Double.parseDouble(fields[2]);
                tokensReferenceLong = Long.parseLong(fields[3]); tokensReference = Double.parseDouble(fields[3]);
            }
        }
        // if we need to subtract query data from reference data
        if (subtract.equals("yes")) {
           // subtract subset counts from complete set
           frequencyReference -= frequencyQuery;
           // types is assumed to remain the same
           tokensReference -= tokensQuery;
        }
        // use add-0.5 smoothing: hanks et al 1991 page 9
        frequencyQuery += 0.5;
        frequencyReference += 0.5;
        tokensQuery += typesQuery*0.5;
        tokensReference += typesReference*0.5;
        // t-test score: obtained from "Using Statistics in Lexical Analysis"
        // by Kenneth Church, William Gale, Patrick Hanks and Donald Hindle.
        // In Uri Zernik (ed) "Lexical Acquisition", 1991. (page 9/123)
        double tScore = 0.0;
        if (tokensQuery > 0 && tokensReference > 0) {
            tScore = (frequencyQuery/tokensQuery)-(frequencyReference/tokensReference); // nominator
            tScore = tScore/Math.sqrt(frequencyQuery/Math.pow(tokensQuery,2.0)+frequencyReference/Math.pow(tokensReference,2.0));
        } else if (tokensQuery > 0) {
            tScore = frequencyQuery/tokensQuery;
            tScore = tScore/Math.sqrt(frequencyQuery/Math.pow(tokensQuery,2.0));
        } // ignore case: tokensQuery == 0
        if (tokensQuery > 0) {
            try { 
                context.write(key, new Text(Double.toString(tScore)+"\t"+frequencyQueryLong+"\t"+tokensQueryLong+"\t"+typesQueryLong+"\t"+frequencyReferenceLong+"\t"+tokensReferenceLong+"\t"+typesReferenceLong));
            } catch (Throwable e) {
                System.out.println("Error: " + e + " " + e.getMessage());
            }
        }
    }
}

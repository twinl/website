package nl.sara.hadoop;

import java.util.*;
import java.util.regex.Pattern;
import java.util.regex.Matcher;
import java.util.Date;
import java.text.SimpleDateFormat;
import java.net.URL;
import java.io.InputStream;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.*;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;

public class MyMapper extends Mapper<Text, Text, Text, Text> {
    String types = ""; // number of types
    String tokens = ""; // number of tokens
    String inputFromQuery = "no";
 
    @Override
    public void setup(Context context) {
        Configuration conf = context.getConfiguration();
        String queryPath = conf.get("queryPath"); // path for query data
        // get number of tokens and types: default = reference data values
        String[] typesAndTokens = new String[2];
        types = conf.get("typesReference");
        tokens = conf.get("tokensReference");
        // test if we are processing query data or reference data
        FileSplit IS = (FileSplit) context.getInputSplit();
        String IF = IS.getPath().toString();
        // if current path = path of query data
        if (IF.equals(queryPath)) {
            // set query data flag 
            inputFromQuery = "yes";
            // use types and tokens of query data
            types = conf.get("typesQuery");
            tokens = conf.get("tokensQuery");
        }
    }

    @Override
    public void map(Text word, Text freq, Context context) {
        String f = freq.toString();
        String[] chars = word.toString().split("");
        // note: splitting a string on "" yields a list with an empty first element
        if (chars.length > 1 && ! chars[1].equals("#")) {
            try { 
                 context.write(word, new Text(f+" "+inputFromQuery+" "+types+" "+tokens));
            } catch (Throwable e) {
                System.out.println("Error(4): " + e + " " + e.getMessage());
            }
        }
    }
}

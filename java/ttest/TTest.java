package nl.sara.hadoop;

import java.io.IOException;
import java.util.Map.Entry;
import java.util.zip.*;
import java.io.FileInputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;

public class TTest extends Configured implements Tool {
    static int ARGMAX = 9; // ARGMAX = 2 (-libjars TTest.jar) + 3 other arguments

    // start the job
    private int runMapReduceJob(String pathQuery, String pathReference, 
                                String typesQuery, String tokensQuery,
                                String typesReference, String tokensReference,
                                String subtract) throws IOException, InterruptedException, ClassNotFoundException {
        String[] typesAndTokens = new String[2];
        Configuration conf = getConf();
        // we want to store the absolute path in the job definition
        // we can only get it from a defined job
        // but we need to store it before defining the job!
        // so creating temprary job variable (which we won't use)
        Job tmpjob = Job.getInstance(conf);
        // store the relative paths
        FileInputFormat.addInputPath(tmpjob, new Path(pathQuery));
        FileInputFormat.addInputPath(tmpjob, new Path(pathReference));
        // get the absolute paths
        Path[] tmppaths = FileInputFormat.getInputPaths(tmpjob);
        // store the absolute path in the job
        conf.set("queryPath",tmppaths[0].toString());
        // store the subtract flag in the job
        conf.set("subtract",subtract);

        // put the numbers of types and tokens in the job definition
        // it would be better to read this information from disk
        // but it is stored in a compressed file and it seems 
        // arbitrary compressed files cannot be opened in Hadoop
        conf.set("typesQuery",typesQuery);
        conf.set("tokensQuery",tokensQuery);
        conf.set("typesReference",typesReference);
        conf.set("tokensReference",tokensReference);

        // define the job
        Job job = Job.getInstance(conf);
        job.setJarByClass(TTest.class);
        job.setJar("TTest.jar");
        job.setMapperClass(MyMapper.class);
        // we don't need a combiner
        //job.setCombinerClass(MyReducer.class);
        job.setReducerClass(MyReducer.class);
        // output key definitions
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        // input format definitions
        job.setInputFormatClass(KeyValueTextInputFormat.class);
        // data files should be compressed      
        FileOutputFormat.setCompressOutput(job,true);
        FileOutputFormat.setOutputCompressorClass(job,GzipCodec.class);
        // set the two input files
        FileInputFormat.addInputPath(job, new Path(pathQuery));
        FileInputFormat.addInputPath(job, new Path(pathReference));
        // define the output directory
        pathQuery = pathQuery.replaceAll("/part-r-00000.gz","");
        pathReference = pathReference.replaceAll("/part-r-00000.gz","");
        pathReference = pathReference.replaceAll("^cache/","");
        pathReference = pathReference.replaceAll("^annotations/","");
        FileOutputFormat.setOutputPath(job, new Path(pathQuery+"/TTest/"+pathReference+"/TTest"));
        // start job
        if (!job.waitForCompletion(true)) { return 1; }
        return 0;
    }

    @Override
    // arg0 will contain three arguments: pathQuery, pathReference, subtract flag: pass on
    public int run(String[] arg0) throws Exception { return runMapReduceJob(arg0[0],arg0[1],arg0[2],arg0[3],arg0[4],arg0[5],arg0[6]); }
    
    public static void main(String[] args) throws Exception {
        // we allow invoking the program with two or three arguments
        // but we internally always use three arguments
        // so first: add a default value for the missing third argument
        String[] argsCopy = new String[ARGMAX];
        for (int i=0;(i<ARGMAX && i<args.length);i++) { argsCopy[i] = args[i]; }
        if (args.length < ARGMAX) { argsCopy[args.length] = "no"; }
        // test if we have the right number of arguments
        if (argsCopy.length != ARGMAX || argsCopy[ARGMAX-1] == null) {
            System.err.println("usage: TTest pathQuery pathReference [yes|no] (args="+args.length+")");
            System.exit(1);
        }
        // ok, we have the right number of arguments: continue
        System.exit(ToolRunner.run(new TTest(), argsCopy));
    }
}

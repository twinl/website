package nl.sara.hadoop;

import java.io.IOException;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.compress.GzipCodec;

import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.io.LongWritable;

public class Experts extends Configured implements Tool {

     private int runMapReduceJob(String path) throws IOException, InterruptedException, ClassNotFoundException {
         Configuration conf = getConf();
         Job job = Job.getInstance(conf);
         job.setJarByClass(Experts.class);
         job.setJar("Experts.jar");
         job.setMapperClass(MyMapper.class);
         // no need for combiners or reducers here
         job.setCombinerClass(MyReducer.class);
         job.setReducerClass(MyReducer.class);

         // 20120920 added by erikt from hadoop book page 24
         job.setMapOutputKeyClass(Text.class);
         job.setOutputKeyClass(Text.class);
 
         FileInputFormat.addInputPath(job, new Path(path+"/tweet*"));
         path = path.replaceAll("^twitter","cache");
         FileOutputFormat.setOutputPath(job, new Path(path+"/Experts"));
         FileOutputFormat.setCompressOutput(job,true);
         FileOutputFormat.setOutputCompressorClass(job,GzipCodec.class);

         //FileOutputFormat.setCompressOutput(job,true);
         //FileOutputFormat.setOutputCompressorClass(job,GzipCodec.class);
            
         if (!job.waitForCompletion(true)) { return 1; }
         return 0;
     }

     @Override
     public int run(String[] arg0) throws Exception { return runMapReduceJob(arg0[0]); }
    
     public static void main(String[] args) throws Exception {
         if (args.length != 3) { // +2 because of -libjars files
             System.err.println("usage: Experts date");
             System.exit(1);
         }
         System.exit(ToolRunner.run(new Experts(), args));
     }
}

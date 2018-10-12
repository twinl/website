package nl.sara.hadoop;

import java.io.IOException;
import java.util.Map.Entry;
import java.util.Calendar;
import java.text.SimpleDateFormat;
import java.net.URLDecoder;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class GetNgrams extends Configured implements Tool {

     // for selecting input and output directories
     private String year;
     private String month;
     private String day;
     private String hour;

     // get maximum number of days per month
     private String maxDays(String year, String month) {
        if (month.equals("04") || month.equals("06") || 
            month.equals("09") || month.equals("11")) { return("30"); }
        else if (month.equals("02")) {
            int y = Integer.parseInt(year);
            if (y % 400 == 0 || (y % 4 == 0 && y % 100 != 0)) {
               return("29"); // leap year
            } else { return("28"); }
        } else { return("31"); } // most common value
     }

     private void setInputPathDay(Job job, String year, String month, String day, String startHour, String endHour) {
          if (startHour.equals("00") && endHour.equals("23")) {
              try {
                  FileInputFormat.addInputPath(job, new Path("twitter/"+year+"/"+month+"/"+day));
              } catch (Throwable e) {
                  System.out.println("Error: " + e + " " + e.getMessage());
              }
          } else {
              int startHourInt = Integer.parseInt(startHour);
              int endHourInt = Integer.parseInt(endHour);
              for (int i=startHourInt; i<=endHourInt; i++) {
                  String hour = Integer.toString(i);
                  if (i < 10) { hour = "0"+hour; }
                  try {
                      FileInputFormat.addInputPath(job, new Path("twitter/"+year+"/"+month+"/"+day+"/"+year+month+day+"-"+hour+".out.gz"));
                  } catch (Throwable e) {
                      System.out.println("Error: " + e + " " + e.getMessage());
                  }
              }
          }
          return;
     }

     private void setInputPathMonth(Job job, String year, String month, String startDay, String endDay, String startHour, String endHour) {
         if (startDay.equals("01") && startHour.equals("00") && endDay.equals(maxDays(year,month)) && endHour.equals("23")) {
             try {
                 FileInputFormat.addInputPath(job, new Path("twitter/"+year+"/"+month+"/*"));
             } catch (Throwable e) {
                 System.out.println("Error: " + e + " " + e.getMessage());
             }
         } else {
             int startDayInt = Integer.parseInt(startDay);
             int endDayInt = Integer.parseInt(endDay);
             for (int i=startDayInt; i<=endDayInt; i++) {
                 String day = Integer.toString(i);
                 if (i < 10) { day = "0"+day; }
                      if (i == startDayInt && i == endDayInt) { setInputPathDay(job,year,month,day,startHour,endHour); }
                 else if (i == startDayInt && i != endDayInt) { setInputPathDay(job,year,month,day,startHour,"23"); }
                 else if (i != startDayInt && i != endDayInt) { setInputPathDay(job,year,month,day,"00"     ,"23"); }
                 else if (i != startDayInt && i == endDayInt) { setInputPathDay(job,year,month,day,"00"     ,endHour); }
             }
         }
     }

     private void setInputPathYear(Job job, String year, String startMonth, String endMonth, String startDay, String endDay, String startHour, String endHour) {
         if (startMonth.equals("01") && startDay.equals("01") && startHour.equals("00") && endMonth.equals("12") && endDay.equals(maxDays(year,endMonth)) && endHour.equals("23")) {
             try {
                 FileInputFormat.addInputPath(job, new Path("twitter/"+year+"/*/*"));
             } catch (Throwable e) {
                 System.out.println("Error: " + e + " " + e.getMessage());
             }
         } else {
             int startMonthInt = Integer.parseInt(startMonth);
             int endMonthInt = Integer.parseInt(endMonth);
             for (int i=startMonthInt; i<=endMonthInt; i++) {
                 String month = Integer.toString(i);
                 if (i < 10) { month = "0"+month; }
                      if (i == startMonthInt && i == endMonthInt) { setInputPathMonth(job,year,month,startDay,endDay             ,startHour,endHour); }
                 else if (i == startMonthInt && i != endMonthInt) { setInputPathMonth(job,year,month,startDay,maxDays(year,month),startHour,"23"); }
                 else if (i != startMonthInt && i != endMonthInt) { setInputPathMonth(job,year,month,"01"    ,maxDays(year,month),"00"     ,"23"); }
                 else if (i != startMonthInt && i == endMonthInt) { setInputPathMonth(job,year,month,"01"    ,endDay             ,"00"     ,endHour); }
             }
         }
     }

     private void setInputPath(Job job, String date) {
         char[] dateArray = date.toCharArray();
         String year1 = Character.toString(dateArray[0])+Character.toString(dateArray[1])+Character.toString(dateArray[2])+Character.toString(dateArray[3]);
         String month1 = Character.toString(dateArray[4])+Character.toString(dateArray[5]);
         String day1 = Character.toString(dateArray[6])+Character.toString(dateArray[7]);
         String hour1 = Character.toString(dateArray[8])+Character.toString(dateArray[9]);
         String year2 = Character.toString(dateArray[11])+Character.toString(dateArray[12])+Character.toString(dateArray[13])+Character.toString(dateArray[14]);
         String month2 = Character.toString(dateArray[15])+Character.toString(dateArray[16]);
         String day2 = Character.toString(dateArray[17])+Character.toString(dateArray[18]);
         String hour2 = Character.toString(dateArray[19])+Character.toString(dateArray[20]);

         int startYearInt = Integer.parseInt(year1);
         int endYearInt = Integer.parseInt(year2);
         for (int i=startYearInt; i<=endYearInt; i++) {
             String year = Integer.toString(i);
                  if (i == startYearInt && i == endYearInt) { setInputPathYear(job,year,month1,month2,day1,day2,hour1,hour2); }
             else if (i == startYearInt && i != endYearInt) { setInputPathYear(job,year,month1,"12"  ,day1,"31",hour1,"23"); }
             else if (i != startYearInt && i != endYearInt) { setInputPathYear(job,year,"01"  ,"12"  ,"01","31","00" ,"23"); }
             else if (i != startYearInt && i == endYearInt) { setInputPathYear(job,year,"01"  ,month2,"01",day2,"00" ,hour2); }
         }
         return;
     }

     private int runMapReduceJob(String date) throws IOException, InterruptedException, ClassNotFoundException {
         Configuration conf = getConf();
         Job job = Job.getInstance(conf);
         job.setJarByClass(GetNgrams.class);
         job.setJar("GetNgrams.jar");
         job.setMapperClass(MyMapper.class);
         //job.setCombinerClass(MyReducer.class);
         job.setReducerClass(MyReducer.class);

         // 20120920 added by erikt from hadoop book page 24
         job.setOutputKeyClass(Text.class);
         job.setOutputValueClass(Text.class);
 
         // 20121129: testing
         job.setNumReduceTasks(24);

         // select input and output directorie
         setInputPath(job,date);
         // FileInputFormat.addInputPath(job, paths[0]);
         String outDir = "cache/"+date+"/GetNgrams";
         FileOutputFormat.setOutputPath(job, new Path(outDir));

         // define alternative text output stream
         MultipleOutputs.addNamedOutput(job,"text",TextOutputFormat.class,NullWritable.class,Text.class);
         MultipleOutputs.addNamedOutput(job,"textPair",TextOutputFormat.class,Text.class,Text.class);
         FileOutputFormat.setCompressOutput(job,true);
         FileOutputFormat.setOutputCompressorClass(job,GzipCodec.class);

         // run job
         if (!job.waitForCompletion(true)) { return 1; }
         return 0;
     }

     @Override
     public int run(String[] arg0) throws Exception {
         return runMapReduceJob(arg0[0]);
     }
    
     public static void main(String[] args) throws Exception {
         if (args.length != 3) { // +2 because of -libjars files
             System.err.println("usage: GetNgrams date (args="+args.length+")");
             System.exit(1);
         }
         System.exit(ToolRunner.run(new GetNgrams(), args));
     }
}

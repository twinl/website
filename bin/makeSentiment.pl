#!/usr/bin/perl -w
# makeSentiment.pl: make data associated with sentiment
# usage: makeSentiment.pl date search
# 20130104 erikt(at)xs4all.nl

use strict;

my $command = $0;
my $date = shift(@ARGV);
my $search = shift(@ARGV);

if (not defined $date or not defined $search or $date eq "" or $search eq "") {
   die "usage: $command date search\n";
}

my $hadoopDir = "/home/cloud/software/hadoop-0.20.2-cdh3u4/bin/hadoop";
my $javaDir = "/home/cloud/java/search";
my $cacheDir = "/var/www/cache";
my $plotFile = "$cacheDir/sentiment.$date.$search.txt";
my $sentFile = "$cacheDir/sentiment.$date.$search.data";

my $sentiment = "";
chdir($javaDir);
if (open(INFILE,"hadoop fs -cat \"cache/$date/Search/$search/text*\" | gunzip -c | make -s runsentiment | tee \"$sentFile\" |")) {
   $sentiment = <INFILE>;
   if (not defined $sentiment) { die "$command: hadoop produced empty data file\n"; }
   chomp($sentiment);
   close(INFILE);
} else { die "$command: error cannot run hadoop\n"; }
if ($sentiment eq "") {
   die("$command: error in sentiment computation: no data available?");
}
if ($sentiment > 0) { $sentiment = sprintf "%d",0.5+$sentiment*100; }
else { $sentiment = sprintf "%d",-0.5+$sentiment*100; }
# convert sentiment value to color: -0.5-+0.5 -> -FF-+FF
my $color = abs(255*$sentiment/(100*0.5));
if ($color > 255) { $color = 255; }
my $mouth = 2*$color/255;
$color = 255-$color;
# avoid colors close to white (invisible)
$color *= 0.8;
$color = sprintf "%x",$color;
if ($color !~ /../) { $color = "0$color"; }
if ($sentiment >= 0) {
   $color = "FFFF".$color; # white to yellow
} else {
   $color = "FF".$color.$color; # white to red
   $mouth = -$mouth;
}

if (not open(OUTFILE,">$plotFile")) { die "$command: cannot write $plotFile\n"; }
print OUTFILE <<THEEND;
set term svg enhanced font 'arial,11' size 320,320
set output "$cacheDir/sentiment.$date.$search.svg"

set size ratio -1
set style fill solid 1.0 border -1
set obj 10 circle at screen .5,.5 size screen .4 behind lw 0
set obj 10 circle arc [ 0 : 360] fc rgb "#$color" 
set obj 11 circle at screen .625,.625 size screen .08 front
set obj 11 circle arc [ 0 : 360] fc rgb "black" 
set obj 12 circle at screen .375,.625 size screen .08 front
set obj 12 circle arc [ 0 : 360] fc rgb "black" 
set title ""
set xlabel ""
unset key
unset xtics
unset ytics
unset border
set xrange [0:1]
set yrange [0:1]

plot [-0.25:0.25] $mouth*x**2.0+0.30-$mouth/40.0 with lines lw 20 lt -1
THEEND
close(OUTFILE);
system("gnuplot \"$plotFile\"");

exit(0);

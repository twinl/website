#!/usr/bin/perl -w
# makeSentMap: create sentiment map from hadoop data of twinl-geo query
# usage: makeSentMap date query
# example: makeSentMap 2013091200-2013091223 twinl-geo
# 20130913 erikt(at)xs4all.nl

use strict;

my $command = $0;
my $binDir = "/home/cloud/bin";
my $fontSize = 11;
my $largeFontSize = 2*$fontSize;
my $minTweets = 10; # minimum of region tweets for visualization
my $outDir = "/var/www/cache";
my $dump = 0;
if (defined $ARGV[0] and $ARGV[0] =~ /^-/) { $dump = 1; shift(@ARGV); }
my ($date,$query) = @ARGV;
if (not defined $query) { die "usage: $command date query\n"; }
my $outFile = "$outDir/sentmap.$date.$query.svg";
my $largeOutFile = "$outDir/sentmap.$date.$query.large.svg";
my %polygons = ();
# coordinates obtained with bin/transformKML -perl -2.55 -50.69 100 100 < etc/provinces.kml (originally in directory /var/www/maptests)
$polygons{"Oost-Vlaanderen"} = "58.8,219.0 62.3,222.0 68.6,221.0 67.9,216.0 86.8,221.0 86.8,226.0 118.3,211.0 123.9,233.0 113.4,236.0 118.3,242.0 103.6,268.0 94.5,273.0 64.4,270.0 58.8,219.0";
$polygons{"Antwerpen"} = "118.3,211.0 123.9,233.0 113.4,236.0 118.3,242.0 133.7,246.0 152.6,244.0 170.1,242.0 188.3,232.0 188.3,220.0 177.1,199.0 165.9,207.0 155.4,203.0 160.3,198.0 154.0,196.0 148.4,203.0 138.6,204.0 139.3,198.0 129.5,201.0 129.5,206.0 118.3,211.0";
$polygons{"Groningen"} = "254.8,5.0 263.2,37.0 275.1,25.0 317.8,62.0 326.9,45.0 326.2,22.0 298.2,0.0 254.8,5.0";
$polygons{"West-Vlaanderen"} = "57.4,209.0 43.4,213.0 0.0,237.0 5.6,265.0 25.2,277.0 42.0,267.0 44.1,270.0 64.4,270.0 58.8,219.0 57.4,209.0";
$polygons{"Zeeland"} = "57.4,209.0 58.8,219.0 62.3,222.0 68.6,221.0 67.9,216.0 86.8,221.0 86.8,226.0 118.3,211.0 129.5,206.0 129.5,186.0 87.5,166.0 80.5,173.0 61.6,193.0 57.4,209.0";
$polygons{"Noord-Brabant"} = "129.5,206.0 129.5,186.0 171.5,166.0 234.5,166.0 235.9,172.0 245.0,190.0 230.3,189.0 236.6,207.0 218.4,215.0 214.2,224.0 213.5,226.0 205.1,216.0 188.3,220.0 177.1,199.0 165.9,207.0 155.4,203.0 160.3,198.0 154.0,196.0 148.4,203.0 138.6,204.0 139.3,198.0 129.5,201.0 129.5,206.0";
$polygons{"Gelderland"} = "171.5,166.0 234.5,166.0 241.5,171.0 297.5,153.0 297.5,136.0 248.5,116.0 231.7,94.0 199.5,126.0 213.5,146.0 178.5,156.0 171.5,166.0";
$polygons{"Utrecht"} = "178.5,156.0 162.4,152.0 157.5,124.0 175.0,117.0 175.0,129.0 185.5,129.0 190.4,118.0 199.5,126.0 213.5,146.0 178.5,156.0";
$polygons{"Limburg BE"} = "188.3,220.0 188.3,232.0 170.1,242.0 179.2,242.0 179.2,275.0 219.8,265.0 227.5,236.0 213.5,226.0 205.1,216.0 188.3,220.0";
$polygons{"Drenthe"} = "291.9,82.0 259.7,77.0 249.9,60.0 271.6,49.0 263.2,37.0 275.1,25.0 317.8,62.0 314.3,83.0 291.9,82.0";
$polygons{"Zuid-Holland"} = "87.5,166.0 135.8,113.0 157.5,124.0 162.4,152.0 178.5,156.0 171.5,166.0 129.5,186.0 87.5,166.0";
$polygons{"Noord-Holland"} = "157.5,124.0 175.0,117.0 175.0,129.0 185.5,129.0 190.4,118.0 170.8,109.0 191.8,73.0 172.2,53.0 157.5,50.0 164.5,38.0 164.5,32.0 161.0,27.0 151.2,41.0 151.2,50.0 135.8,113.0 156.8,124.0 157.5,124.0";
$polygons{"Friesland"} = "226.8,65.0 249.9,60.0 271.6,49.0 263.2,37.0 254.8,5.0 212.8,16.0 196.0,39.0 201.6,61.0 221.2,61.0 226.8,65.0";
$polygons{"Vlaams-Brabant"} = "118.3,242.0 133.7,246.0 152.6,244.0 170.1,242.0 179.2,242.0 179.2,275.0 154.7,265.0 147.0,266.0 147.0,271.0 117.6,276.0 112.0,273.0 96.6,277.0 94.5,273.0 103.6,268.0 118.3,242.0";
$polygons{"Flevoland"} = "231.7,94.0 199.5,126.0 190.4,118.0 174.3,111.0 213.5,86.0 213.5,69.0 221.2,61.0 226.8,65.0 240.1,80.0 228.9,85.0 231.7,94.0 231.7,94.0";
$polygons{"Overijssel"} = "226.8,65.0 240.1,80.0 228.9,85.0 231.7,94.0 248.5,116.0 297.5,136.0 315.7,122.0 311.5,100.0 290.5,97.0 291.9,82.0 259.7,77.0 249.9,60.0 226.8,65.0";
$polygons{"Limburg NL"} = "234.5,166.0 235.9,172.0 245.0,190.0 230.3,189.0 236.6,207.0 218.4,215.0 214.2,224.0 213.5,226.0 227.5,236.0 219.8,265.0 219.8,271.0 248.5,271.0 248.5,256.0 234.5,246.0 255.5,226.0 255.5,186.0 234.5,166.0";
# make large polygons
my %largePolygons = ();
foreach my $key (keys %polygons) {
   $largePolygons{$key} = "";
   my @coordinates = split(/\s+|,+/,$polygons{$key});
   for (my $i=0;$i<=$#coordinates;$i++) {
      if ($i > 0 and 2*int($i/2) == $i) { $largePolygons{$key} .= " "; }
      elsif ($i > 0) { $largePolygons{$key} .= " "; }
      $largePolygons{$key} .= 2*$coordinates[$i];
   }
}

# get the data from hadoop
my $tmpFile = "/tmp/makeSentMap.$date.$query.$$";
system("/home/cloud/software/hathi-client/hadoop-2.6.0/bin/hadoop fs -cat cache/$date/Search/$query/text-* | gunzip -c | grep -vi MeteoWestzaan > $tmpFile");
if (-z $tmpFile) { die "$command: cannot retrieve data from Hadoop\n"; }

# find locations in data
my @locations = ();
my %keys = ();
if (open(INFILE,"rev < $tmpFile | cut -f1,2 | rev | $binDir/pointInPolygon.pl |")) {
   while (<INFILE>) {
      my $line = $_;
      chomp($line);
      $line =~ s/\t.*$//;
      push(@locations,$line);
      $keys{$line} = 1;
   }
   close(INFILE);
}

# find sentiments in data
my %positive = ();
my %negative = ();
my %neutral = ();
chdir("/home/cloud/java/search");
if (open(INFILE,"rev < $tmpFile | cut -f3- | rev | java nl/sara/hadoop/Sentiment showAll |")) {
   my $i = 0;
   while (<INFILE>) {
      my $line = $_;
      chomp($line);
      if ($line =~ /^\+1\s/) { $positive{$locations[$i]} = defined $positive{$locations[$i]} ? $positive{$locations[$i]}+1 : 1; }
      elsif ($line =~ /^-1\s/) { $negative{$locations[$i]} = defined $negative{$locations[$i]} ? $negative{$locations[$i]}+1 : 1; }
      elsif ($line =~ /^0\s/) { $neutral{$locations[$i]} = defined $neutral{$locations[$i]} ? $neutral{$locations[$i]}+1 : 1; }
      else { die "$command: unknown sentiment value $line on data line $i!\n"; }
      if ($dump) { print "$locations[$i] $line\n"; }
      $i++;
   }
   close(INFILE);
}
# remove temporary file
unlink($tmpFile);
if ($dump) { exit(0); }

# process results
if (not open(OUTFILE,">$outFile")) { die "$command: cannot write $outFile\n"; }
if (not open(LARGEOUTFILE,">$largeOutFile")) { die "$command: cannot write $largeOutFile\n"; }
print OUTFILE <<THEEND;
<svg xmlns="http://www.w3.org/2000/svg" version="1.1" width="515" height="285">
THEEND
print LARGEOUTFILE <<THEEND;
<svg xmlns="http://www.w3.org/2000/svg" version="1.1" width="1030" height="570">
THEEND
my %sentiment = ();
my %totalTweets = ();
foreach my $k (keys %keys) {
   if (not defined $positive{$k}) { $positive{$k} = 0; }
   if (not defined $negative{$k}) { $negative{$k} = 0; }
   if (not defined $neutral{$k}) { $neutral{$k} = 0; }
   $totalTweets{$k} = $positive{$k}+$negative{$k}+$neutral{$k};
   $sentiment{$k} = 0;
   if ($totalTweets{$k} > 0) { $sentiment{$k} = 100*($positive{$k}-$negative{$k})/$totalTweets{$k}; }
   if ($sentiment{$k} >= 0) { $sentiment{$k} = int(0.5+$sentiment{$k}); }
   else { $sentiment{$k} = int(-0.5+$sentiment{$k}); }
   if (defined $polygons{$k}) {
      if ($totalTweets{$k} < $minTweets) {
         print OUTFILE <<THEEND;
  <polygon points="$polygons{$k}" style="fill:#CCCCCC;stroke:purple;stroke-width:1;opacity:0.5;"/>
THEEND
         print LARGEOUTFILE <<THEEND;
  <polygon points="$largePolygons{$k}" style="fill:#CCCCCC;stroke:purple;stroke-width:1;opacity:0.5;"/>
THEEND
      } else {
         my $color = &sent2color($sentiment{$k});
         if (defined $polygons{$k}) { 
            print OUTFILE <<THEEND;
     <polygon points="$polygons{$k}" style="fill:#$color;stroke:purple;stroke-width:1;opacity:0.5;"/>
THEEND
            print LARGEOUTFILE <<THEEND;
     <polygon points="$largePolygons{$k}" style="fill:#$color;stroke:purple;stroke-width:1;opacity:0.5;"/>
THEEND
         }
         my ($cx,$cy) = &polygonCenter($polygons{$k});
         if ($sentiment{$k} > 0) { $sentiment{$k} = "+$sentiment{$k}"; }
         print OUTFILE "  <text x=\"$cx\" y=\"$cy\" text-anchor=\"middle\" font-size=\"$fontSize\">$sentiment{$k}</text>\n";
         print LARGEOUTFILE "  <text x=\"".(2*$cx)."\" y=\"".(2*$cy)."\" text-anchor=\"middle\" font-size=\"$largeFontSize\">$sentiment{$k}</text>\n";
         $sentiment{$k} =~ s/^\+//;
      }
   }
}
foreach my $k (keys %polygons) { 
   if (not defined $keys{$k}) {
      print OUTFILE <<THEEND;
  <polygon points="$polygons{$k}" style="fill:#CCCCCC;stroke:purple;stroke-width:1;opacity:0.5;"/>
THEEND
      print LARGEOUTFILE <<THEEND;
  <polygon points="$largePolygons{$k}" style="fill:#CCCCCC;stroke:purple;stroke-width:1;opacity:0.5;"/>
THEEND
   }
}

# create legend
my $x = 350;
my $y = 15;
my $dy = 15;
foreach my $k (sort { $sentiment{$b} <=> $sentiment{$a} } keys %sentiment) {
   if ($totalTweets{$k} >= $minTweets) {
      if ($sentiment{$k} > 0) { $sentiment{$k} = "+$sentiment{$k}"; }
      print OUTFILE "  <text x=\"".$x."\" y=\"$y\" text-anchor=\"end\" font-size=\"$fontSize\">$sentiment{$k}</text>\n";
      print LARGEOUTFILE "  <text x=\"".(2*$x)."\" y=\"".(2*$y)."\" text-anchor=\"end\" font-size=\"$largeFontSize\">$sentiment{$k}</text>\n";
      my $name = ($k eq "") ? "Rest van de wereld" : $k;
      my $tweets = $totalTweets{$k} == 1 ? "tweet" : "tweets";
      print OUTFILE "  <text x=\"".($x+5)."\" y=\"$y\" text-anchor=\"start\" font-size=\"$fontSize\">$name ($totalTweets{$k} $tweets)</text>\n";
      print LARGEOUTFILE "  <text x=\"".(2*$x+10)."\" y=\"".(2*$y)."\" text-anchor=\"start\" font-size=\"$largeFontSize\">$name ($totalTweets{$k} $tweets)</text>\n";
      $y += $dy;
   }
}
foreach my $k (sort { $a cmp $b } keys %polygons) {
   if (not defined $totalTweets{$k} or $totalTweets{$k} < $minTweets) {
      my $name = ($k eq "") ? "Rest van de wereld" : $k;
      if (not defined $totalTweets{$k}) { $totalTweets{$k} = 0; }
      my $tweets = $totalTweets{$k} == 1 ? "tweet" : "tweets";
      print OUTFILE "  <text x=\"".($x+5)."\" y=\"$y\" text-anchor=\"start\" font-size=\"$fontSize\">$name ($totalTweets{$k} $tweets)</text>\n";
      print LARGEOUTFILE "  <text x=\"".(2*$x+10)."\" y=\"".(2*$y)."\" text-anchor=\"start\" font-size=\"$largeFontSize\">$name ($totalTweets{$k} $tweets)</text>\n";
      $y += $dy;
   }
}
$query =~ s/^twinl-geo\+?//i;
$date = &simplifyDate($date);
if ($query ne "") { 
   print OUTFILE <<THEEND;
  <text x="5" y="40" text-anchor="start" font-size="$fontSize">Zoekwoord: $query</text>
THEEND
   print LARGEOUTFILE <<THEEND;
  <text x="10" y="80" text-anchor="start" font-size="$largeFontSize">Zoekwoord: $query</text>
THEEND
}
print OUTFILE <<THEEND;
  <text x="5" y="10" text-anchor="start" font-size="$fontSize" font-weight="bold">Sentiment per provincie</text>
  <text x="5" y="25" text-anchor="start" font-size="$fontSize">Datum: $date</text>
  <text x="255" y="270" text-anchor="start" font-size="$fontSize">twiqs.nl</text>
</svg>
THEEND
print LARGEOUTFILE <<THEEND;
  <text x="10" y="20" text-anchor="start" font-size="$largeFontSize" font-weight="bold">Sentiment per provincie</text>
  <text x="10" y="50" text-anchor="start" font-size="$largeFontSize">Datum: $date</text>
  <text x="510" y="540" text-anchor="start" font-size="$largeFontSize">twiqs.nl</text>
</svg>
THEEND
close(OUTFILE);
close(LARGEOUTFILE);

exit(0);

sub sent2color {
   my $sentiment = shift(@_);
   # convert sentiment value to color: -0.5-+0.5 -> -FF-+FF
   my $color = abs(255*$sentiment/(100*0.5));
   if ($color > 255) { $color = 255; }
   $color = 255-$color;
   # avoid colors close to white (invisible)
   $color *= 0.8;
   $color = sprintf "%x",$color;
   if ($color !~ /../) { $color = "0$color"; }
   if ($sentiment > 0) { $color = "FFFF".$color; } # yellow
   elsif ($sentiment == 0) { $color = "FFFFFF"; } # white
   else { $color = "FF".$color.$color; } # red
   return($color);
}

# 20130913 check this function
sub polygonCenter {
   # algorithm from http://en.wikipedia.org/wiki/Centroid#Centroid_of_polygon
   my $coordinatesLine = shift(@_);
   my @coordinates = split(/[ ,]+/,$coordinatesLine);
   my $a = 0;
   for (my $i=0;$i<$#coordinates-1;$i+=2) {
      $a += 0.5*($coordinates[$i]*$coordinates[$i+3]-$coordinates[$i+2]*$coordinates[$i+1]);
   }
   my $cx = 0;
   my $cy = 0;
   for (my $i=0;$i<$#coordinates-1;$i+=2) {
      $cx += ($coordinates[$i]+$coordinates[$i+2])*($coordinates[$i]*$coordinates[$i+3]-$coordinates[$i+2]*$coordinates[$i+1])/(6*$a);
      $cy += ($coordinates[$i+1]+$coordinates[$i+3])*($coordinates[$i]*$coordinates[$i+3]-$coordinates[$i+2]*$coordinates[$i+1])/(6*$a);
   }
   return($cx,$cy);
}

sub simplifyDate {
   my $date = shift(@_);
   my @date = split(//,$date);
   my %months = ("01","januari","02","februari","03","maart","04","april","05","mei","06","juni",
                 "07","juli","08","augustus","09","september","10","oktober","11","november","12","december");
   if (not defined $date[20]) { die "simplifyDate: unexpected date line: $date\n"; }
   my ($year1,$month1,$day1,$hour1) = ($date[0].$date[1].$date[2].$date[3],$date[4].$date[5],$date[6].$date[7],$date[8].$date[9]);
   my ($year2,$month2,$day2,$hour2) = ($date[11].$date[12].$date[13].$date[14],$date[15].$date[16],$date[17].$date[18],$date[19].$date[20]);
   if ($year1 eq $year2 and $month1 eq $month2 and $day1 eq $day2 and $hour1 eq "00" and $hour2 eq "23") { $day1 =~ s/^0//; return("$day1 $months{$month1} $year1"); }
   elsif ($year1 eq $year2 and $month1 eq $month2 and $day1 eq "01" and $day2 eq &maxDays($year2,$month2)  and $hour1 eq "00" and $hour2 eq "23") { return("$months{$month1} $year1"); }
   elsif ($year1 eq $year2 and $month1 eq "01" and $month2 eq "12" and $day1 eq "01" and $day2 eq "31" and $hour1 eq "00" and $hour2 eq "23") { return($year1); }
   elsif ($year1 eq $year2 and $month1 eq $month2 and $day1 eq $day2 and $hour1 eq $hour2) { return("$day1 $months{$month1} $year1 $hour1:00-$hour1:59"); }
   else { return($date); }
}

sub maxDays() {
   my ($year,$month) = @_;

   if (not defined $year or not defined $month or $year eq "" or $month eq "") { return(""); }
   if ($month == 4 or $month == 6 or $month == 9 or $month == 11) { return(30); }
   elsif ($month == 2) {
      if ($year % 4 != 0 or ($year % 100 == 0 and $year % 400 != 0)) { return(28); }
      else { return(29); }
   } else { return(31); }
}

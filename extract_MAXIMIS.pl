#!/usr/bin/perl -w
######################
#
# Script: extract_MAXIMIS.pl
# Purpose: To call stored procedures to extract MAXIMIS Hold table data
#
######################
use strict;
use Time::Local;
use File::Copy;
use File::Basename;
use ActiveState::Config::INI;
use Getopt::Std;
use Net::FTP;
use POSIX qw(strftime);

my $bcp;
my $sqlcmd;
my $database_name;
my $database_server_name;

my $IgnorePrevDate; 
my $proc;
my $cmd;
my $row;
my @vwbuff;

my $scriptname="extract_MAXIMIS.pl";
my $section = "MAXIMIS";

my $extract_MAXIMIS_ftp_server;
my $extract_MAXIMIS_ftp_userid;
my $extract_MAXIMIS_ftp_password;
my $extract_MAXIMIS_ftp_folder;

my $dtm = strftime("%y%m%d.%H:%M:%S",localtime);


######################################################################
# Function : BcpOut
# Return values : 0 success, die for fail
sub BcpOut {
    my ($TABLE) = $_[0];
    my ($lfile_out) = $_[1];
    $cmd=`${bcp} ${database_name}.dbo.${TABLE} out "${lfile_out}" -S $database_server_name -T -q -c -t ,`;
    print $cmd;
#Asmita Changes : Azure comment below line
    #($cmd =~ /Error|Msg/) or die "BCP of Table ${TABLE} failed.";
    return 0

} # function BcpOut

#####################################
# Function: copy_to_final_file
# Purpose : To copy one file to another final file
# Return value: 0 upon success, 'die' on failure
#######################################
sub copy_to_final_file {
    my ($rfmt_file) = $_[0];
    my ($final_file) = $_[1];
    print "rfmt file = ", $rfmt_file;
    print "\nfinal file = ",$final_file, "\n";
    open (RFMTFILE, "<", $rfmt_file) || die "Cannot open $rfmt_file";
    open (FINALFILE, ">", $final_file) || die "Cannot open $final_file! $!";
    while (my $line = <RFMTFILE>) {
        print FINALFILE $line;      # Write records from RFMTFILE to FINALFILE
    }
    close RFMTFILE;
    close FINALFILE;
    return 0
}

###############################
# Process DGD_Environ.ini file
###############################
my $ini=ActiveState::Config::INI->new("DGD_Environ.ini") or die "Can't read DGD_Environ.ini";
print "$scriptname: Starting\n";

$bcp = ${ini}->property('GENERAL', 'BCP');
$sqlcmd = ${ini}->property('GENERAL', 'SQLCMD');
my $workdir = ${ini}->property('MAXIMIS', 'WORKDIR');
print "WorkFolder - ${workdir}\n";
$database_name = ${ini}->property('GENERAL', 'DATABASE_NAME');
print "Database - ${database_name}\n";
$database_server_name = ${ini}->property('GENERAL', 'DATABASE_SERVER_NAME');
print "Database Server - ${database_server_name}\n";

$extract_MAXIMIS_ftp_userid=${ini}->property($section, 'extract_MAXIMIS_ftp_userid');
$extract_MAXIMIS_ftp_password=${ini}->property($section, 'extract_MAXIMIS_ftp_password');
$extract_MAXIMIS_ftp_folder=${ini}->property($section, 'extract_MAXIMIS_ftp_folder');
$extract_MAXIMIS_ftp_server=${ini}->property($section, 'extract_MAXIMIS_ftp_server');
print "Maximis file copy location: ${extract_MAXIMIS_ftp_folder}\n";

# Variables related to TradeHold information
my $TradeHold_TABLE;
my $TradeHold_BCP_OUT;
my $TradeHold_BCP_OUT_RFMT;
my @tradehold_bcp_out_lines;
my $trade_hold_file;

# Variables related to Investment Hold information
my $InvHold_TABLE;
my $InvHold_BCP_OUT;
my $InvHold_BCP_OUT_RFMT;
my @invhold_bcp_out_lines;
my $inv_hold_file;

# Variables related to Event Hold information
my $EventHold_TABLE;
my $EventHold_BCP_OUT;
my $EventHold_BCP_OUT_RFMT;
my @eventhold_bcp_out_lines;
my $event_hold_file;

# Variables related to Rate Affil Hold information
my $RateAffilHold_TABLE;
my $RateAffilHold_BCP_OUT;
my $RateAffilHold_BCP_OUT_RFMT;
my @rateaffilhold_bcp_out_lines;
my $rate_affil_hold_file;

# Variables related to Pay Sched Hdr Hold information
my $PaySchedHdrHold_TABLE;
my $PaySchedHdrHold_BCP_OUT;
my $PaySchedHdrHold_BCP_OUT_RFMT;
my @paysched_hdr_hold_bcp_out_lines;
my $pay_sched_hdr_hold_file;

# Variables related to Pay Sched Det Hold information
my $PaySchedDetHold_TABLE;
my $PaySchedDetHold_BCP_OUT;
my $PaySchedDetHold_BCP_OUT_RFMT;
my @paysched_det_hold_bcp_out_lines;
my $pay_sched_det_hold_file;


###############################
#
# Process input switches
#
###############################
#my $USAGE="Usage: $0 [-f FromDate] {-T ToDate}";

my %Options;
my $ok;
die "Error: $!." if !($ok=getopts('F:T:' , \%Options)) ; # %Options holds all switches and their values

my $opt_F=$Options{F}; # From date - optional parameter - not passed by ESP
my $opt_T=$Options{T}; # To date - optional parameter - not passed by ESP 
# 
my $fdate = "";
my $tdate = "";
$IgnorePrevDate = 'Y';

if ($opt_F) {
    $fdate = $opt_F; # 02/12/2011
    $fdate =~ s/\///g; # 02/12/2011
    $fdate =~ s/\s+$//;
    $fdate =~ s/^\s*//;
    my ($fmonth, $fday , $fyear) = unpack "A2 A2 A4", $fdate;
    
    eval{ # This test will die starting on 01/17/2038. This is the limitation of timelocal function.
        timelocal(0,0,0,$fday, $fmonth-1, $fyear); # dies in case of bad date
    + 
        1;
    } or die "Invalid date: ${opt_F}";
    $fdate=$fyear . $fmonth . $fday; # yymmdd or yyyymmdd
	$IgnorePrevDate = '';
}

if ($opt_T) {
    $tdate = $opt_T; # 02/12/2011
    $tdate =~ s/\///g; # 02/12/2011
    $tdate =~ s/\s+$//;
    $tdate =~ s/^\s*//;
    my ($tmonth, $tday , $tyear) = unpack "A2 A2 A4", $tdate;
    
    eval{ # This test will die starting on 01/17/2038. This is the limitation of timelocal function.
        timelocal(0,0,0,$tday, $tmonth-1, $tyear); # dies in case of bad date
    + 
        1;
    } or die "Invalid date: ${opt_T}";
    $tdate=$tyear . $tmonth . $tday # yymmdd or yyyymmdd

}
#print "opt_T = ", $opt_T, "\n";   # testing check
#print "opt_F = ", $opt_F, "\n";   # testing check

##########
# Call p_5600_i_tw_ext_MAX_hold_keys storedprocedure
##########
$proc = "p_5600_i_tw_ext_MAX_hold_keys";
print "Dealing with p_5600_i_twt_ext_MAXH_hold_keys\n\n";   # testing check
$proc .= $fdate ?  " '${fdate}', '${tdate}'" : " Null,Null";
print "${proc}\n";
$cmd=`${sqlcmd} -S${database_server_name} -E -Q"exec ${database_name}..${proc}"` or die;
(!($cmd =~ /Error|Msg/)) or (!$cmd) or die "Error in ${proc}: ${cmd}\n";
print "${proc}: ${cmd}\n";
##########
# Call p_5605_extract_MAXIMIS storedprocedure
##########
$proc = "p_5605_extract_MAXIMIS"; 
print "Dealing with p_5605_extract_MAXIMIS\n";  # testing check
$cmd=`${sqlcmd} -S${database_server_name} -E -Q"exec ${database_name}..${proc} '${IgnorePrevDate}'"` or die;
(!($cmd =~ /Error|Msg/)) or (!$cmd) or die "Error in ${proc}: ${cmd}\n";
print "${proc}: ${cmd}\n";

# ##########################
# Write the Trade Hold feed
# ##########################
print  "Write TradeHold feed.\n";

$TradeHold_TABLE="tw_MAX_TradeHold";
$TradeHold_BCP_OUT=${workdir}  . "\\" . ${TradeHold_TABLE} . "\.bcp\.out";
$TradeHold_BCP_OUT_RFMT=${workdir} . "\\" . ${TradeHold_TABLE} . "\.rfmt";

&BcpOut ($TradeHold_TABLE,$TradeHold_BCP_OUT);

if ( -z $TradeHold_BCP_OUT ) {
    print  "TradeHold table is empty. Setting date-time for all filenames to System date/time\n";
    #$dtm = strftime("%y%m%d.%H:%M:%S",localtime);
    $dtm = strftime("%Y%m%d.%H%M%S",localtime);
    print  "dtm=$dtm\n";
}
else {
    print  "Setting date-time for all filenames from last field in the first record\n";
    my @tradeholdlines;
    my $record;
    my @firstRecord;
    open(TRADEHOLDFILE,"$TradeHold_BCP_OUT") || die("Cannot Open $TradeHold_BCP_OUT");
    chomp (@tradeholdlines = <TRADEHOLDFILE>);
    close TRADEHOLDFILE;
    @firstRecord = split(/,/,$tradeholdlines[0]);
    $dtm = $firstRecord[$#firstRecord]; # Example 11/30/2010 15:05:07
	my ($dt, $tm) = split( " ", $dtm);
	my ($m, $d, $y) = split('/', $dt);
	$tm =~ s/://g; # remove ":" 
	$dtm = "$y$m$d.$tm";
    chomp($dtm);
    print  "dtm=$dtm\n";
}
 print  "Reformat the raw table data\n";
  
# Read from TRade Hold file and store in array
my $record;
my $footer;
my $filecount = 0;   # number of records in file
open (TRADEHOLDFILE,"$TradeHold_BCP_OUT") || die ("Cannot open $TradeHold_BCP_OUT");
@tradehold_bcp_out_lines = <TRADEHOLDFILE>;
close(TRADEHOLDFILE);

# sort the array
@tradehold_bcp_out_lines = sort @tradehold_bcp_out_lines;

# convert commas to quote comma quote and store in output file
open (TRADEHOLDBCPOUT,">","$TradeHold_BCP_OUT_RFMT");

foreach $record (@tradehold_bcp_out_lines) {
    chomp $record;
    $record =~ s/,/","/g;
    $filecount++;
    print  TRADEHOLDBCPOUT qq("${record}"\n);
}

# Create footer line and write it to file
$footer = qq("\$F99 ${filecount}"\n);
print  TRADEHOLDBCPOUT $footer;
print  "footer = $footer\n";

close TRADEHOLDBCPOUT;
print  "Finished: Write TradeHold BCP Out\n";

$trade_hold_file = ${workdir} . "\\" . "maxtrd.bcf.trade_hold." . "$dtm" . "\.csv";

print  "Copy to the final file $trade_hold_file\n";
&copy_to_final_file ($TradeHold_BCP_OUT_RFMT, $trade_hold_file);


# ################################
#### Write the Investment Hold Feed
# ################################
print  "Write Investment Hold Feed\n";

$InvHold_TABLE="tw_MAX_InvHold";
$InvHold_BCP_OUT=$workdir . "\\" . ${InvHold_TABLE} . "\." . "bcp\.out";

print  "bcp out table data\n";
&BcpOut ($InvHold_TABLE, $InvHold_BCP_OUT);

print  "Reformat the raw table data\n";
$InvHold_BCP_OUT_RFMT = ${workdir} . "\\" . ${InvHold_TABLE} . "\.rfmt";

# Read from Investment Hold file and store in array

open (INVHOLDFILE,"$InvHold_BCP_OUT") || die ("Cannot open $InvHold_BCP_OUT");
@invhold_bcp_out_lines = <INVHOLDFILE>;
close(INVHOLDFILE);

# sort the array
@invhold_bcp_out_lines = sort @invhold_bcp_out_lines;

# convert commas to quote comma quote and store in output file
open (INVHOLDBCPOUT,">","$InvHold_BCP_OUT_RFMT");
$filecount=0;
foreach $record (@invhold_bcp_out_lines) {
    chomp $record;
    $record =~ s/,/","/g;
    $filecount++;
    print  INVHOLDBCPOUT qq("${record}"\n);
}

# Create footer line and write it to Inv Hold BCP out rfmt file
$footer = qq("\$F99 ${filecount}"\n);
print  INVHOLDBCPOUT $footer;
print  "footer = $footer\n";

close INVHOLDBCPOUT;
print  "Finished: Write InvHold BCP Out\n";

$inv_hold_file=${workdir} . "\\" . "maxtrd\.bcf\.investment_hold\." . "$dtm". "\.csv";

print  "Copy to the final file $inv_hold_file\n";
&copy_to_final_file ($InvHold_BCP_OUT_RFMT, $inv_hold_file);


# ################################
#### Write the Event Hold Feed
# ################################
print  "Write Event Hold Feed\n";

$EventHold_TABLE="tw_MAX_EventHold";
$EventHold_BCP_OUT=$workdir . "\\" . ${EventHold_TABLE} . "\." . "bcp\.out";

print  "bcp out table data\n";
&BcpOut ($EventHold_TABLE, $EventHold_BCP_OUT);

print  "Reformat the raw table data\n";
$EventHold_BCP_OUT_RFMT = ${workdir} . "\\" . ${EventHold_TABLE} . "\.rfmt";

# Read from Event Hold file and store in array

open (EVENTHOLDFILE,"$EventHold_BCP_OUT") || die ("Cannot open $EventHold_BCP_OUT");
@eventhold_bcp_out_lines = <EVENTHOLDFILE>;
close(EVENTHOLDFILE);

# sort the array
@eventhold_bcp_out_lines = sort @eventhold_bcp_out_lines;

# convert commas to quote comma quote and store in output file
open (EVENTHOLDBCPOUT,">","$EventHold_BCP_OUT_RFMT");
$filecount=0;
foreach $record (@eventhold_bcp_out_lines) {
    chomp $record;
    $record =~ s/,/","/g;
    $filecount++;
    print  EVENTHOLDBCPOUT qq("${record}"\n);
}

# Create footer line and write it to Event Hold BCP out rfmt file
$footer = qq("\$F99 ${filecount}"\n);
print  EVENTHOLDBCPOUT $footer;
print  "footer = $footer\n";

close EVENTHOLDBCPOUT;
print  "Finished: Write EventHold BCP Out\n";

$event_hold_file = ${workdir} . "\\" . "maxtrd\.bcf\.event_hold\." . $dtm . "\.csv";
print  "Copy to the final file $event_hold_file\n";

&copy_to_final_file ($EventHold_BCP_OUT_RFMT, $event_hold_file);

# ################################
##### Write the Rate Affil Hold feed
# ################################
print  "Write Rate Affil Hold feed\n";

$RateAffilHold_TABLE="tw_MAX_RateAffilHold";
$RateAffilHold_BCP_OUT=${workdir} . "\\" . ${RateAffilHold_TABLE} . "\.bcp\.out";

print  "bcp out table data\n";
&BcpOut ($RateAffilHold_TABLE, $RateAffilHold_BCP_OUT);

print  "Reformat the raw table data\n";

$RateAffilHold_BCP_OUT_RFMT = ${workdir} . "\\" . ${RateAffilHold_TABLE} ."\.rfmt";

# Read from Rate Affil Hold file and store in array

open (RATEAFFILHOLDFILE,"$RateAffilHold_BCP_OUT") || die ("Cannot open $RateAffilHold_BCP_OUT");
@rateaffilhold_bcp_out_lines = <RATEAFFILHOLDFILE>;
close(RATEAFFILHOLDFILE);

# sort the array
@rateaffilhold_bcp_out_lines = sort @rateaffilhold_bcp_out_lines;

# convert commas to quote comma quote and store in output file
open (RATEAFFILHOLDBCPOUT,">","$RateAffilHold_BCP_OUT_RFMT");
$filecount=0;
foreach $record (@rateaffilhold_bcp_out_lines) {
    chomp $record;
    $record =~ s/,/","/g;
    $filecount++;
    print  RATEAFFILHOLDBCPOUT qq("${record}"\n);
}

# Create footer line and write it to Rate Affil Hold BCP out rfmt file
$footer = qq("\$F99 ${filecount}"\n);
print  RATEAFFILHOLDBCPOUT $footer;
print  "footer = $footer\n";

close RATEAFFILHOLDBCPOUT;
print  "Finished: Write RateAffilHold BCP Out\n";

$rate_affil_hold_file = ${workdir} . "\\" . "maxtrd\.bcf\.rate_affil_hold\." . $dtm . "\.csv";
print  "Copy to the final file $rate_affil_hold_file\n";

&copy_to_final_file ($RateAffilHold_BCP_OUT_RFMT, $rate_affil_hold_file);

# ################################
##### Write the Pay Sched Hdr Hold feed
# ################################
print  "Write PaySchedHdrHold feed...\n";

$PaySchedHdrHold_TABLE="tw_MAX_PaySchedHdrHold";
$PaySchedHdrHold_BCP_OUT=${workdir} . "\\" . ${PaySchedHdrHold_TABLE} . "\.bcp\.out";

print  "bcp out table data\n";
&BcpOut ($PaySchedHdrHold_TABLE, $PaySchedHdrHold_BCP_OUT);

print  "Reformat the raw table data\n";
$PaySchedHdrHold_BCP_OUT_RFMT = ${workdir} . "\\" . ${PaySchedHdrHold_TABLE} ."\.rfmt";

# Read from Pay Sched Hdr Hold file and store in array

open (PAYSCHEDHDRHOLDFILE,"$PaySchedHdrHold_BCP_OUT") || die ("Cannot open $PaySchedHdrHold_BCP_OUT");
@paysched_hdr_hold_bcp_out_lines = <PAYSCHEDHDRHOLDFILE>;
close(PAYSCHEDHDRHOLDFILE);

# sort the array
@paysched_hdr_hold_bcp_out_lines = sort @paysched_hdr_hold_bcp_out_lines;

# convert commas to quote comma quote and store in output file
open (PAYSCHEDHDRHOLDBCPOUT,">","$PaySchedHdrHold_BCP_OUT_RFMT");
$filecount=0;
foreach $record (@paysched_hdr_hold_bcp_out_lines) {
    chomp $record;
    $record =~ s/,/","/g;
    $filecount++;
    print  PAYSCHEDHDRHOLDBCPOUT qq("${record}"\n);
}

# Create footer line and write it to Pay Sched Hdr Hold BCP out rfmt file
$footer = qq("\$F99 ${filecount}"\n);
print  PAYSCHEDHDRHOLDBCPOUT $footer;
print  "footer = $footer\n";

close PAYSCHEDHDRHOLDBCPOUT;
print  "Finished: Write PaySchedHdrHold BCP Out\n";

$pay_sched_hdr_hold_file = ${workdir} . "\\" . "maxtrd\.bcf\.pay_sched_hdr_hold\." . $dtm . "\.csv";
print  "Copy to the final file $pay_sched_hdr_hold_file\n";

&copy_to_final_file ($PaySchedHdrHold_BCP_OUT_RFMT, $pay_sched_hdr_hold_file);

# ################################
#### Write the Pay Sched Det Hold feed
# ################################
print  "Write PaySchedDetHold feed...\n";

$PaySchedDetHold_TABLE="tw_MAX_PaySchedDetHold";
$PaySchedDetHold_BCP_OUT=${workdir} . "\\" . ${PaySchedDetHold_TABLE} . "\.bcp\.out";

print  "bcp out table data\n";
&BcpOut ($PaySchedDetHold_TABLE, $PaySchedDetHold_BCP_OUT);

print  "Reformat the raw table data\n";
$PaySchedDetHold_BCP_OUT_RFMT = ${workdir} . "\\" . ${PaySchedDetHold_TABLE} ."\.rfmt";

# Read from Pay Sched Det Hold file and store in array

open (PAYSCHEDDETHOLDFILE,"$PaySchedDetHold_BCP_OUT") || die ("Cannot open $PaySchedDetHold_BCP_OUT");
@paysched_det_hold_bcp_out_lines = <PAYSCHEDDETHOLDFILE>;
close(PAYSCHEDDETHOLDFILE);

# sort the array
@paysched_det_hold_bcp_out_lines = sort @paysched_det_hold_bcp_out_lines;

# convert commas to quote comma quote and store in output file
open (PAYSCHEDDETHOLDBCPOUT,">","$PaySchedDetHold_BCP_OUT_RFMT");
$filecount=0;
foreach $record (@paysched_det_hold_bcp_out_lines) {
    chomp $record;
    $record =~ s/,/","/g;
    $filecount++;
    print  PAYSCHEDDETHOLDBCPOUT qq("${record}"\n);
}

# Create footer line and write it to Pay Sched Det Hold BCP out rfmt file
$footer = qq("\$F99 ${filecount}"\n);
print  PAYSCHEDDETHOLDBCPOUT $footer;
print  "footer = $footer\n";

close PAYSCHEDDETHOLDBCPOUT;
print  "Finished: Write PaySchedDetHold BCP Out\n";

$pay_sched_det_hold_file = ${workdir} . "\\" . "maxtrd\.bcf\.pay_sched_det_hold\." . $dtm . "\.csv";
print  "Copy to the final file $pay_sched_det_hold_file\n";

&copy_to_final_file ($PaySchedDetHold_BCP_OUT_RFMT, $pay_sched_det_hold_file);

# #########################################################
# Call p_5620_MAX_Blotter storedprocedure
# #########################################################

$proc = "p_5620_MAX_Blotter";
print "Dealing with p_5620_MAX_Blotter\n\n";   # testing check
$cmd=`${sqlcmd} -S${database_server_name} -E -Q"exec ${database_name}..${proc}"` or die;
(!($cmd =~ /Error|Msg/)) or (!$cmd) or die "Error in ${proc}: ${cmd}\n";
print "${proc}: ${cmd}\n";

# #########################################################
#  Extract tw_MAX_Blotter
# #########################################################

my $max_table = "tw_MAX_Blotter";
my $Blotter_Filename = "maxblt\.bcf\.trade_hold\." . $dtm . "\.csv";
my $Blotter_File = ${workdir} . "\\" . $Blotter_Filename;

$cmd=`${sqlcmd} -S $database_server_name -E -Q"select * from ${database_name}..tw_MAX_Blotter" -W -s ,` or die;
($cmd =~ /Error|Msg/) and die "Extract of view ${max_table} failed.\n ${cmd}\n";

$cmd =~ s/NULL//g;    #remove NULL
@vwbuff = split("\n",$cmd);
open (RFMT,">",$Blotter_File)|| die "Cannot open $Blotter_File\n";
print RFMT $Blotter_Filename . "\n";	# filename print header
if (@vwbuff > 4)
	{	# have data to process
	print RFMT "Ticket Number,Transaction Type,CUSIP,Security Description,Portfolio,Broker Code,Broker Long Name,Trade Date,Settlement Date,Quantity,Price,Commissions,Fees,Net Amount\n";	# print column headers
	foreach $row (@vwbuff[2 .. $#vwbuff -2])
		{ 
		$row =~ s/,/","/g;    #add in quotes
		print RFMT '"'.$row . '"'."\n";}
	}
	else 
	{ 
	print RFMT "No Trade records to process.\n";	# nothing today
	}

close RFMT;
print "Finished : Write Max Blotter feed to: ${Blotter_File}\n";

# #########################################
# File Copy the extracts to the "staging" folder
# #########################################

# If no From and Through dates were passed (the normal condition)
#   then proceed with the file copy
# WR 208006 - Removed FTP concept and Added File Copy concept
if (!$opt_F && !$opt_T) 
{
 print "\nGet original file for file copy\n";

 my $trade_hold_file1 = basename($trade_hold_file);
 my $inv_hold_file1 = basename($inv_hold_file);
 my $event_hold_file1 = basename($event_hold_file);
 my $rate_affil_hold_file1 = basename($rate_affil_hold_file);
 my $pay_sched_hdr_hold_file1 = basename($pay_sched_hdr_hold_file);
 my $pay_sched_det_hold_file1 = basename($pay_sched_det_hold_file);
 my $Blotter_File1 = basename($Blotter_File);

 print  "\nFile Copy the extracts to ${extract_MAXIMIS_ftp_folder}\n";

 copy("${event_hold_file}","${extract_MAXIMIS_ftp_folder}${event_hold_file1}") or die "Copy failed ${event_hold_file1}";
 copy("${rate_affil_hold_file}","${extract_MAXIMIS_ftp_folder}${rate_affil_hold_file1}") or die "Copy failed ${rate_affil_hold_file1}";
 copy("${pay_sched_hdr_hold_file}","${extract_MAXIMIS_ftp_folder}${pay_sched_hdr_hold_file1}") or die "Copy failed ${pay_sched_hdr_hold_file1}";
 copy("${pay_sched_det_hold_file}","${extract_MAXIMIS_ftp_folder}${pay_sched_det_hold_file1}") or die "Copy failed ${pay_sched_det_hold_file1}";
 copy("${Blotter_File}","${extract_MAXIMIS_ftp_folder}${Blotter_File1}") or die "Copy failed ${Blotter_File1}";
 copy("${trade_hold_file}","${extract_MAXIMIS_ftp_folder}${trade_hold_file1}") or die "Copy failed ${trade_hold_file1}";
 copy("${inv_hold_file}","${extract_MAXIMIS_ftp_folder}${inv_hold_file1}") or die "Copy failed ${inv_hold_file1}";

 print  "File Copy of extract files successful\n";
}    
 
# ###############################################################
# Record the successfully extracted keys in the DealGen database
# ###############################################################

# If no From and Through dates were passed (the normal condition), then
# record the successfully extracted hold_keys and data

if (!$opt_F && !$opt_T) {
    # call the stored procedure, p_5610_record_extract_MAXIMIS
    $proc="p_5610_record_extract_MAXIMIS";
    print  "Executing $proc\n";
    $cmd=`${sqlcmd} -S${database_server_name} -E -Q"exec ${database_name}..${proc}"`;
    (!($cmd =~ /Error|Msg/)) or (!$cmd) or die "Error in ${proc}: ${cmd}\n";
    print  "${proc}: ${cmd}\n";
}

# #############
# Housekeeping
# #############
unlink $TradeHold_BCP_OUT;
unlink $TradeHold_BCP_OUT_RFMT;

unlink $EventHold_BCP_OUT;
unlink $EventHold_BCP_OUT_RFMT;

unlink $InvHold_BCP_OUT;
unlink $InvHold_BCP_OUT_RFMT;

unlink $RateAffilHold_BCP_OUT;
unlink $RateAffilHold_BCP_OUT_RFMT;

unlink $PaySchedHdrHold_BCP_OUT;
unlink $PaySchedHdrHold_BCP_OUT_RFMT;

unlink $PaySchedDetHold_BCP_OUT;
unlink $PaySchedDetHold_BCP_OUT_RFMT;

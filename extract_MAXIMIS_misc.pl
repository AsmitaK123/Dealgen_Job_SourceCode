#!/usr/bin/perl -w
# Object  : extract_MAXIMIS_misc.pl
# Purpose : Run the stored procedures to extract MAXIMIS miscellaneous table data

use strict;
use Time::Local;
use File::Copy;
use File::Basename;
use Net::FTP;
use ActiveState::Config::INI;
use Getopt::Std;
use POSIX qw(strftime);

my $sqlcmd;
my $row;
my @vwbuff;
my $database_name;
my $database_server_name;

my $cmd;
my $proc;
my $scriptname="extract_MAXIMIS_misc.pl";
my $section = "MAXIMIS_MISC";

my $extract_MAXIMIS_misc_ftp_server;
my $extract_MAXIMIS_misc_ftp_userid;
my $extract_MAXIMIS_misc_ftp_password;
my $extract_MAXIMIS_misc_ftp_folder;

my $dtm;
my $record;

my $max_misc_view;   # to hold view name
my $max_misc_bcp_out_rfmt;
my $max_misc_sic_prim_file;

my $max_misc_analyst_file;
my $max_misc_minor_type_file;

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
# Process input switches
###############################
#my $USAGE="Usage: $0 [-f FromDate] {-T ToDate}";
print "$scriptname: Starting.\n";
my %Options;
my $ok;
die "Error: $!." if !($ok=getopts('F:T:' , \%Options)) ; # %Options holds all switches and their values

my $opt_F=$Options{F}; # From date - optional parameter - not passed by ESP
my $opt_T=$Options{T}; # To date - optional parameter - not passed by ESP 

my $fdate = "";
my $tdate = "";
my $IgnorePrevDate = 'Y';

# copied out from extract_MAXIMIS.pl
if ($opt_F) {
    $fdate = $opt_F; # 02/12/2011
    $fdate =~ s/\///g; # 02/12/2011
    $fdate =~ s/\s+$//;
    $fdate =~ s/^\s*//;
    my ($fmonth, $fday , $fyear) = unpack "A2 A2 A4", $fdate;
    $fdate=$fyear . $fmonth . $fday; # yymmdd or yyyymmdd
    print "From Date: ${fdate}\n";
	$IgnorePrevDate = '';
}

if ($opt_T) {
    $tdate = $opt_T; # 02/12/2011
    $tdate =~ s/\///g; # 02/12/2011
    $tdate =~ s/\s+$//;
    $tdate =~ s/^\s*//;
    my ($tmonth, $tday , $tyear) = unpack "A2 A2 A4", $tdate;
    $tdate=$tyear . $tmonth . $tday; # yymmdd or yyyymmdd
    print "To Date: ${tdate}\n";}

###############################
# Process DGD_Environ.ini file
###############################
my $ini=ActiveState::Config::INI->new("DGD_Environ.ini") or die "Can't read DGD_Environ.ini: ";
$sqlcmd = ${ini}->property('GENERAL', 'SQLCMD');
my $workdir = ${ini}->property($section, 'WORKDIR');
print "Workdir: ${workdir}\n";
$database_name = ${ini}->property('GENERAL', 'DATABASE_NAME');
print "Database_name: ${database_name}\n";
$database_server_name = ${ini}->property('GENERAL', 'DATABASE_SERVER_NAME');
print "Database_server_name: ${database_server_name}\n";

$extract_MAXIMIS_misc_ftp_userid=${ini}->property($section, 'extract_MAXIMIS_misc_ftp_userid');
$extract_MAXIMIS_misc_ftp_password=${ini}->property($section, 'extract_MAXIMIS_misc_ftp_password');
$extract_MAXIMIS_misc_ftp_folder=${ini}->property($section, 'extract_MAXIMIS_misc_ftp_folder');
$extract_MAXIMIS_misc_ftp_server=${ini}->property($section, 'extract_MAXIMIS_misc_ftp_server');
print "Maximis file copy location: ${extract_MAXIMIS_misc_ftp_folder}\n";

# ######################
# Call stored procedure p_5700_i_tw_ex_MAX_misc_keys to populate the
# 'hold_keys' table
# ######################

$proc = "p_5700_i_tw_ex_MAX_misc_keys";
print "Dealing with ${proc}\n\n";   # testing check
$proc .= $fdate ?  " '${fdate}', '${tdate}'" : " Null,Null";
$cmd=`${sqlcmd} -S${database_server_name} -E -Q"exec ${database_name}..${proc}"` or die;
(!($cmd =~ /Error|Msg/)) or (!$cmd) or die "Error in ${proc}: ${cmd}\n";
print "${proc}: ${cmd}\n";

# ######################
# Call stored procedure p_5705_extract_MAXIMIS_misc to extract the
# MAXIMIS MAX_misc feed data
# ######################

$proc = "p_5705_extract_MAXIMIS_misc"; 
print "Dealing with ${proc}\n\n";   # testing check
$cmd=`${sqlcmd} -S${database_server_name} -E -Q"exec ${database_name}..${proc} '${IgnorePrevDate}'"` or die;
(!($cmd =~ /Error|Msg/)) or (!$cmd) or die "Error in ${proc}: ${cmd}\n";
print "${proc}: ${cmd}\n";

# In preparation for the extracts creation, get date/time
$dtm = strftime("%Y%m%d",localtime);
print "\$dtm = $dtm\n";

# #########################################################
#  Extract v_MAX_misc_SIC_PRIM View
# #########################################################

$max_misc_view = "v_MAX_misc_SIC_PRIM";
$max_misc_bcp_out_rfmt=$workdir . "\\" . ${max_misc_view} . "\.rfmt";

$cmd=`${sqlcmd} -S $database_server_name -E -Q"select * from ${database_name}..v_MAX_misc_SIC_PRIM" -W -s ,` or die;
print $cmd;
($cmd =~ /Error|Msg/) and die "Extract of view ${max_misc_view} failed.";

@vwbuff = split("\n",$cmd);
open (RFMT,">",$max_misc_bcp_out_rfmt)|| die "Cannot open $max_misc_bcp_out_rfmt";
print RFMT $vwbuff[0]."\n";	# print header
foreach $row (@vwbuff[2 .. $#vwbuff -1])
	{ print RFMT $row . "\n";}

close RFMT;
print "Finished : Write SIC_PRIM feed\n";

# Final file
$max_misc_sic_prim_file=${workdir} . "\\jh_ind_cde" . "\." . $dtm . "\.csv";

print "Copy to the final file $max_misc_sic_prim_file\n";
&copy_to_final_file ($max_misc_bcp_out_rfmt, $max_misc_sic_prim_file);

# Housekeeping
unlink $max_misc_bcp_out_rfmt;

# ###########################################################
#  Extract v_MAX_misc_ANALYST view
# ###########################################################

$max_misc_view = "v_MAX_misc_ANALYST";

$cmd=`${sqlcmd} -S $database_server_name -E -Q"select * from ${database_name}..v_MAX_misc_ANALYST" -W -s ,` or die;
print $cmd;
($cmd =~ /Error|Msg/) and die "Extract of view ${max_misc_view} failed.";

@vwbuff = split("\n",$cmd);
open (RFMT,">",$max_misc_bcp_out_rfmt)|| die "Cannot open $max_misc_bcp_out_rfmt";
print RFMT $vwbuff[0]."\n";	# print header
foreach $row (@vwbuff[2 .. $#vwbuff -1])
	{ print RFMT $row . "\n";}
	
close RFMT;

# final file
$max_misc_analyst_file=${workdir} . "\\analyst\." . $dtm . "\.csv";

print "Copy to the final file $max_misc_analyst_file\n";
&copy_to_final_file ($max_misc_bcp_out_rfmt, $max_misc_analyst_file);

# Housekeeping
unlink $max_misc_bcp_out_rfmt;

# #############################################################
#  Extract v_MAX_misc_MINOR_TYPE view
# #############################################################
$max_misc_view="v_MAX_misc_MINOR_TYPE";

$cmd=`${sqlcmd} -S $database_server_name -E -Q"select * from ${database_name}..v_MAX_misc_MINOR_TYPE" -W -s ,` or die;
print $cmd;
($cmd =~ /Error|Msg/) and die "Extract of view ${max_misc_view} failed.";

@vwbuff = split("\n",$cmd);
open (RFMT,">",$max_misc_bcp_out_rfmt)|| die "Cannot open $max_misc_bcp_out_rfmt";
print RFMT $vwbuff[0]."\n";	# print header
foreach $row (@vwbuff[2 .. $#vwbuff -1])
	{ print RFMT $row . "\n";}
	
close RFMT;

# final file
$max_misc_minor_type_file=${workdir} . "\\minor_type\." . $dtm . "\.csv";

print "Copy to the final file $max_misc_analyst_file\n";
&copy_to_final_file ($max_misc_bcp_out_rfmt, $max_misc_minor_type_file);

# Housekeeping
unlink $max_misc_bcp_out_rfmt;


# File copy the extracts to the "staging" folder
# WR 208006 - Removed FTP concept and Added File Copy concept

if (!$opt_F && !$opt_T) 
{

 print "\nGet original file for file copy\n";

 my $max_misc_analyst_file1 = basename($max_misc_analyst_file);
 my $max_misc_sic_prim_file1 = basename($max_misc_sic_prim_file);
 my $max_misc_minor_type_file1 = basename($max_misc_minor_type_file);

 print "FTP extracts to target folder $extract_MAXIMIS_misc_ftp_folder\n";

 copy("${max_misc_analyst_file}","${extract_MAXIMIS_misc_ftp_folder}${max_misc_analyst_file1}") or die "Copy failed ${max_misc_analyst_file1}";
 copy("${max_misc_sic_prim_file}","${extract_MAXIMIS_misc_ftp_folder}${max_misc_sic_prim_file1}") or die "Copy failed ${max_misc_sic_prim_file1}";
 copy("${max_misc_minor_type_file}","${extract_MAXIMIS_misc_ftp_folder}${max_misc_minor_type_file1}") or die "Copy failed ${max_misc_minor_type_file1}";

 print  "File Copy of extract files successful\n";    

}

# Record the successfully extracted keys in the DealGen database

# If no From and Through dates were passed (the normal condition), then
# record the successfully extracted hold_keys and data

if (!$opt_F && !$opt_T) {
    # call the stored procedure, p_5710_rec_extr_MAX_misc
    $proc="p_5710_rec_extr_MAX_misc";  
    print "Executing $proc\n";
	$cmd=`${sqlcmd} -S $database_server_name -E -Q"exec ${database_name}..$proc"`  or die;
	print $cmd;
	($cmd =~ /Error|Msg/) and die "Running ${proc} failed.";
}

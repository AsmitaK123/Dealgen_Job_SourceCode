#!/usr/bin/perl -w
# Object  : extract_TW_files.pl
# Purpose : Run the stored procedure and extract TW files.

use strict;
no warnings 'uninitialized';
use ActiveState::Config::INI;
use File::Copy;
use POSIX qw(strftime);
use Date::Calc qw( Today Add_Delta_Days);   # date arimethic

my $bcp;
my $sqlcmd;
my $database_name;
my $database_server_name;
my $database_server_instance_name;

my $Proc_AnalyticBase;
my $Proc_cashFlow;
my $Proc_Allocation;
#GWAMBCF-906
my $Proc_ScheduledFuding;

my $cmd;
my ($dtm, $dtm2);

my $scriptname="extract_TW_files.pl";
my $section = "TW_EXTRACT";

my $AnaBase_Table;
my $AnaBase_File;
my $CashFlow_Table;
my $CashFlow_File;
my $Allocation_Table;
my $Allocation_File;
#GWAMBCF-906
my $ScheduledFunding_Table;
my $Scheduledfunding_File;
my $m;
my $d;

my ($sec,$min,$hour) = localtime();

$sec = sprintf ("%02d", $sec);
$min = sprintf ("%02d", $min);
$hour = sprintf ("%02d", $hour);


# find the last date of the previous month
my ($y, $m1, $d1) = Today();
if($m1 <= 9)
{
$m = "0"."${m1}";
}
else
{
$m = $m1;
}
if($d1 <=9)
{
$d="0"."${d1}";
}
else
{
$d=$d1;
}

###############################
# Process DGD_Environ.ini file
###############################
my $ini=ActiveState::Config::INI->new("DGD_Environ.ini") or die "Can't read DGD_Environ: ";
print "$scriptname: Starting\n";
$bcp = ${ini}->property('GENERAL', 'BCP');
my $workdir=${ini}->property($section, 'WORKDIR');
print "Work dir: ${workdir}\n";
$sqlcmd = ${ini}->property('GENERAL', 'SQLCMD');
$database_name = ${ini}->property('GENERAL', 'DATABASE_NAME');
print "Database: ${database_name}\n";
$database_server_name = ${ini}->property('GENERAL', 'DATABASE_SERVER_NAME');
print "Database Server: ${database_server_name}\n";
$dtm = strftime("%Y%m%d.%H%M%S",localtime);
$dtm2 = strftime("%m%d%Y",localtime);

#############################################
#Extracts Analytics base data for the DealGen to TW file.
#
#Run the stored procedure p_9104_i_Analytics_TWfeed_BatchJob
#############################################
$Proc_AnalyticBase=${ini}->property($section, 'PROC5');
$Proc_AnalyticBase="p_9104_i_Analytics_TWfeed_BatchJob";
print "Dealing with ${Proc_AnalyticBase}\n\n";
$cmd=`${sqlcmd} -S ${database_server_name} -E -Q "exec ${database_name}..${Proc_AnalyticBase} "` or die;
(!($cmd =~ /Error|Msg/)) or (!$cmd) or die "Error in ${Proc_AnalyticBase}: ${cmd}\n";
print "${Proc_AnalyticBase}: ${cmd}\n";



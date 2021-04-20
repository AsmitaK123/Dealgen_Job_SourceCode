#!/usr/bin/perl -w
# Object  : extract_TW_files.pl
# Purpose : Run the stored procedure and extract TW files.

use strict;
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
#Run the stored procedure p_9104_i_Analytics_BaseTW
#############################################
#$Proc_AnalyticBase=${ini}->property($section, 'PROC1');
#print "Dealing with ${Proc_AnalyticBase}\n\n";
#$cmd=`${sqlcmd} -S ${database_server_name} -E -Q "exec ${database_name}..${Proc_AnalyticBase} "` or die;
#(!($cmd =~ /Error|Msg/)) or (!$cmd) or die "Error in ${Proc_AnalyticBase}: ${cmd}\n";
#print "${Proc_AnalyticBase}: ${cmd}\n";

###############################
# Extract tw_Analytics_Base table
###############################
print "Write Analytics base data feed\n";
$AnaBase_Table=${ini}->property($section, 'TABLE1');
$AnaBase_File=${workdir}.${ini}->property($section, 'FILE1')."_".$y.$m.$d."_".$hour.$min.$sec. "\.txt";




print "Copy to the final file ${AnaBase_File}\n";

$cmd=`${bcp} ${database_name}.dbo.${AnaBase_Table} out "${AnaBase_File}" -S $database_server_name -T -q -c -t "	"`;
print $cmd;
($cmd =~ /Error|Msg/) or (!$cmd) or die "BCP of Table ${AnaBase_Table} failed.";

print "Finished : Write Analytics base data feed\n";

#############################################
#Extracts cash Flow data for the DealGen to TW file.
#
#Run the stored procedure p_9105_i_Analytics_CashFlowTW
#############################################
#$Proc_cashFlow=${ini}->property($section, 'PROC2');
#print "Dealing with ${Proc_cashFlow}\n\n";
#$cmd=`${sqlcmd} -S ${database_server_name} -E -Q "exec ${database_name}..${Proc_cashFlow} "` or die;
#(!($cmd =~ /Error|Msg/)) or (!$cmd) or die "Error in ${Proc_cashFlow}: ${cmd}\n";
#print "${Proc_cashFlow}: ${cmd}\n";

###############################
# Extract tw_Analytics_CashFlow table
###############################
print "Write Cash Flow data feed\n";
$CashFlow_Table=${ini}->property($section, 'TABLE2');
$CashFlow_File=${workdir}.${ini}->property($section, 'FILE2')."_".$y.$m.$d."_".$hour.$min.$sec. "\.txt";
print "Copy to the final file ${CashFlow_File}\n";

$cmd=`${bcp} ${database_name}.dbo.${CashFlow_Table} out "${CashFlow_File}" -S $database_server_name -T -q -c -t "	"`;
print $cmd;
($cmd =~ /Error|Msg/) or die "BCP of Table ${CashFlow_Table} failed.";
print "Finished : Write Cash Flow data feed\n";

#############################################
#Extracts Allocation base data for the DealGen to TW file.
#
#Run the stored procedure p_9108_i_Analytics_AllocationTW
#############################################
#$Proc_Allocation=${ini}->property($section, 'PROC3');
#print "Dealing with ${Proc_Allocation}\n\n";
#$cmd=`${sqlcmd} -S ${database_server_name} -E -Q "exec ${database_name}..${Proc_Allocation} "` or die;
#(!($cmd =~ /Error|Msg/)) or (!$cmd) or die "Error in ${Proc_Allocation}: ${cmd}\n";
#print "${Proc_Allocation}: ${cmd}\n";

###############################
# Extract tw_Analytics_AllocationTW table
###############################
print "Write Analytics base data feed\n";
$Allocation_Table=${ini}->property($section, 'TABLE3');
$Allocation_File=${workdir}.${ini}->property($section, 'FILE3')."_".$y.$m.$d."_".$hour.$min.$sec. "\.txt";




print "Copy to the final file ${Allocation_File}\n";

$cmd=`${bcp} ${database_name}.dbo.${Allocation_Table} out "${Allocation_File}" -S $database_server_name -T -q -c -t "	"`;
print $cmd;
($cmd =~ /Error|Msg/) or die "BCP of Table ${Allocation_Table} failed.";

print "Finished : Write Allocation base data feed\n";

#GWAMBCF-906 Asmita Changes 
#############################################
#Extracts Scheduled Funding base data for the DealGen to TW file.
#
#Run the stored procedure p_9109_i_ScheduledFunding_TW
#############################################
#$Proc_ScheduledFuding=${ini}->property($section, 'PROC4');
#print "Dealing with ${Proc_ScheduledFuding}\n\n";
#$cmd=`${sqlcmd} -S ${database_server_name} -E -Q "exec ${database_name}..${Proc_ScheduledFuding} "` or die;
#(!($cmd =~ /Error|Msg/)) or (!$cmd) or die "Error in ${Proc_ScheduledFuding}: ${cmd}\n";
#print "${Proc_ScheduledFuding}: ${cmd}\n";

###############################
# Extract tw_Schdeuled table
###############################
print "Write Scheduled Funding data feed\n";
$ScheduledFunding_Table=${ini}->property($section, 'TABLE4');
$Scheduledfunding_File=${workdir}.${ini}->property($section, 'FILE4')."_".$y.$m.$d."_".$hour.$min.$sec. "\.txt";




print "Copy to the final file ${Scheduledfunding_File}\n";

$cmd=`${bcp} ${database_name}.dbo.${ScheduledFunding_Table} out "${Scheduledfunding_File}" -S $database_server_name -T -q -c -t "	"`;
print $cmd;
#($cmd =~ /Error|Msg/) or (!$cmd) or  die "BCP of Table ${ScheduledFunding_Table} failed.";

print "Finished : Write Scheduled Funding data feed\n";



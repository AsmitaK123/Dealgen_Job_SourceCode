#!/usr/bin/perl -w
# Object  : extract_private_report.pl
# Purpose : Run the stored procedure to report the Private 
#  Placements from DealGen over the last two weeks
use strict;
use ActiveState::Config::INI;
use File::Copy;
use POSIX qw(strftime);

my $sqlcmd;
my $database_name;
my $database_server_name;
my $database_server_instance_name;

my $proc;
my $cmd;
my ($dtm, $dtm2);
my @vwbuff;
my $row;

my $scriptname="extract_private_report.pl";
my $section = "PrivPlace";

my $Privates_TABLE;
my $Privates_FILE;


###############################
# Process DGD_Environ.ini file
###############################
my $ini=ActiveState::Config::INI->new("DGD_Environ.ini") or die "Can't read DGD_Environ.ini: ";
print "$scriptname: Starting\n";
my $workdir=${ini}->property($section, 'WORKDIR');
print "Work dir: ${workdir}\n";
$sqlcmd = ${ini}->property('GENERAL', 'SQLCMD');
$database_name = ${ini}->property('GENERAL', 'DATABASE_NAME');
print "Database: ${database_name}\n";
$database_server_name = ${ini}->property('GENERAL', 'DATABASE_SERVER_NAME');
print "Database Server: ${database_server_name}\n";
my $PP_path = ${ini}->property($section, 'PRIV_BUSINESSDIR') . '\\';
$dtm = strftime("%Y%m%d.%H%M%S",localtime);
$dtm2 = strftime("%m%d%Y",localtime);

###############################
# Run the stored procedure to populate the Private Placement table
###############################

$proc = "p_5650_r_ext_PrivatePlacement";
print "Dealing with ${proc}\n\n";
$cmd=`${sqlcmd} -S ${database_server_name} -E -Q "exec ${database_name}..${proc}"` or die;
(!($cmd =~ /Error|Msg/)) or (!$cmd) or die "Error in ${proc}: ${cmd}\n";
print "${proc}: ${cmd}\n";

# #########################################################
#  Extract  tw_PrivatePlacement table
# #########################################################
print "Write Private Placement feed\n";
$Privates_TABLE="tw_PrivatePlacement";
$Privates_FILE=${workdir} . "\\PrivatePlacement\." . $dtm . "\.csv";
print "Copy to the final file ${Privates_FILE}\n";

$cmd=`${sqlcmd} -S ${database_server_name} -E -Q "select CUSIP, ACLevel1Id, ACLevel2Id, DealId, TransactionId, InvestmentId, InvestmentTypeId, InvestmentTypeName, InvestmentDescription, InvestmentBreakdownId, FundingActualId, AcctId, AcctName, SubAcctId, SubAcctCd, AccrualMethodId, InstrumentCode, ParAmt, convert(varchar, SettlementDt, 109) SettlementDt, StatusId, StatusName, convert(varchar, TradeDt, 109) TradeDt,IssueCurrency,USDEqParAmt from ${database_name}..${Privates_TABLE} order by CUSIP,AcctId" -W -s ,` or die;
($cmd =~ /Error|Msg/) and die "Extract of ${Privates_TABLE} failed:\n${cmd}";

$cmd =~ s/NULL//g;    #remove NULL

@vwbuff = split("\n",$cmd);
my $filecount=0;
open (RFMT,">",$Privates_FILE)|| die "Cannot open $Privates_FILE";
print RFMT $vwbuff[0]."\n";	# print header
foreach $row (@vwbuff[2 .. $#vwbuff -2])    # output rows
	{$filecount++; 
	print RFMT $row . "\n";}

print "Rowcount = ${filecount}\n";

close RFMT;
print "Finished : Write Private Placement feed\n";


# Copy the extract to the target folder
my $FINAL_Privates_FILE="PrivatePlacement_${dtm2}\.csv";

print "Copy extract to the target folder: ${PP_path}\n";

# Copy $Privates_File to BCF SecAttrib Folder \DealGen\Data on another drive
my $retcode = copy (${Privates_FILE},${PP_path} . ${FINAL_Privates_FILE}) or die "Copy to $${PP_path} failed!";
print "$Privates_FILE successfully copied to ${PP_path}${FINAL_Privates_FILE}\n";


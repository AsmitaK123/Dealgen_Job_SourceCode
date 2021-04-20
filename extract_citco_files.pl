#!/usr/bin/perl -w
# Object  : extract_citco_files.pl
# Purpose : Run the stored procedure and extract CITCO files.

use strict;
use ActiveState::Config::INI;
use File::Copy;
use POSIX qw(strftime);

my $bcp;
my $sqlcmd;
my $database_name;
my $database_server_name;
my $database_server_instance_name;

my $Proc_SecMaster;
my $Proc_InvHold;
my $Proc_Options;
my $Proc_Trans;
my $Proc_SchFunds;
my $Proc_SinkDunds;
my $cmd;
my ($dtm, $dtm2);
my @vwbuff;
my $row;
my $msg1;

my $scriptname="extract_citco_files.pl";
my $section = "CITCO_EXTRACT";

my $SecMaster_Table;
my $SecMaster_File;
my $SecMaster_File1;
my $Options_Table;
my $Options_File;
my $Trades_Table;
my $Trades_File;
my $TradesFS_Table;
my $TradesFS_File;
my $ActFund_Table;
my $ActFund_File;

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
#Extracts the master keys for the DealGen to PAM Staging file.
#
#Run the stored procedure p_0600_i_tw_ext_CITCO_Security_Master
#############################################
$Proc_SecMaster=${ini}->property($section, 'PROC1');
print "Dealing with ${Proc_SecMaster}\n\n";
$cmd=`${sqlcmd} -S ${database_server_name} -E -Q "exec ${database_name}..${Proc_SecMaster} 'N',' ', ' ' "` or die;
(!($cmd =~ /Error|Msg/)) or (!$cmd) or die "Error in ${Proc_SecMaster}: ${cmd}\n";
print "${Proc_SecMaster}: ${cmd}\n";

#############################################
# Extracts the Security feed for the DealGen to PAM Staging file
# The file file will be processed in the PAM staging area and ultimately
# sent to CITCO as the GIL.
#
# Run the stored procedure p_0675_i_tw_CITCO_InvHold
#############################################
$Proc_InvHold=${ini}->property($section, 'PROC2');
print "Dealing with ${Proc_InvHold}\n\n";
$cmd=`${sqlcmd} -S ${database_server_name} -E -Q "exec ${database_name}..${Proc_InvHold} 'Y' "` or die;
(!($cmd =~ /Error|Msg/)) or (!$cmd) or die "Error in ${Proc_InvHold}: ${cmd}\n";
print "${Proc_InvHold}: ${cmd}\n";

#############################################
# Extracts the Options feeds for the DealGen to PAM Staging file
# The file file will be processed in the PAM staging area and ultimately
# sent to CITCO  
#
# Run the stored procedure p_0677_i_tw_CITCO_Options
#############################################
$Proc_Options=${ini}->property($section, 'PROC3');
print "Dealing with ${Proc_Options}\n\n";
$cmd=`${sqlcmd} -S ${database_server_name} -E -Q "exec ${database_name}..${Proc_Options} 'Y' "` or die;
(!($cmd =~ /Error|Msg/)) or (!$cmd) or die "Error in ${Proc_Options}: ${cmd}\n";
print "${Proc_Options}: ${cmd}\n";

#############################################
# Extracts the Transactions and scheduled funding feeds 
# for the DealGen to PAM Staging file.
# The file file will be processed in the PAM staging area and ultimately
# sent to CITCO as the GTL  
#
# Run the stored procedure p_0680_i_tw_CITCO_Transactions
#############################################
$Proc_Trans=${ini}->property($section, 'PROC4');
print "Dealing with ${Proc_Trans}\n\n";
$cmd=`${sqlcmd} -S ${database_server_name} -E -Q "exec ${database_name}..${Proc_Trans} 'Y' "` or die;
(!($cmd =~ /Error|Msg/)) or (!$cmd) or die "Error in ${Proc_Trans}: ${cmd}\n";
print "${Proc_Trans}: ${cmd}\n";

#############################################
# Run the stored procedure p_0682_i_tw_CITCO_ScheduledFunds
#############################################
$Proc_SchFunds=${ini}->property($section, 'PROC5');
print "Dealing with ${Proc_SchFunds}\n\n";
$cmd=`${sqlcmd} -S ${database_server_name} -E -Q "exec ${database_name}..${Proc_SchFunds} 'Y' "` or die;
(!($cmd =~ /Error|Msg/)) or (!$cmd) or die "Error in ${Proc_SchFunds}: ${cmd}\n";
print "${Proc_SchFunds}: ${cmd}\n";

###############################
#  Extract  tw_CITCO_InvHold table
###############################
print "Write CITCO Security Master feed\n";
$SecMaster_Table=${ini}->property($section, 'TABLE1');
$SecMaster_File=${workdir} . "\\".${ini}->property($section, 'FILE1')."_temp\.csv";
$SecMaster_File1=${workdir} . "\\".${ini}->property($section, 'FILE1')."\.csv";
print "Copy to the final file ${SecMaster_File}\n";

$cmd=`${bcp} ${database_name}.dbo.${SecMaster_Table} out "${SecMaster_File}" -S $database_server_name -T -q -c -t ,`;
print $cmd;
($cmd =~ /Error|Msg/) and die "BCP of Table ${SecMaster_Table} failed.";

print "Finished : Write CITCO Security Master feed\n";

# reformat bcp file
$msg1 = reform( "${SecMaster_File}", "${SecMaster_File1}");
($msg1) and die $msg1 = "Error in reformat: ${msg1}\n";

# delete Temp File
unlink "${SecMaster_File}" or die "Error delete file ${SecMaster_File}\n";

###############################
#  Extract  tw_CITCO_Options table
###############################
print "Write CITCO Options feed\n";
$Options_Table=${ini}->property($section, 'TABLE2');
$Options_File=${workdir} . "\\".${ini}->property($section, 'FILE2')."\.csv";
print "Copy to the final file ${Options_File}\n";

$cmd=`${bcp} ${database_name}.dbo.${Options_Table} out "${Options_File}" -S $database_server_name -T -q -c -t ,`;
print $cmd;
($cmd =~ /Error|Msg/) and die "BCP of Table ${Options_Table} failed.";

print "Finished : Write CITCO Options feed\n";

###############################
#  Extract  tw_CITCO_Transactions table
###############################
print "Write CITCO Trades feed\n";
$Trades_Table=${ini}->property($section, 'TABLE3');
$Trades_File=${workdir} . "\\".${ini}->property($section, 'FILE3')."\.csv";
print "Copy to the final file ${Trades_File}\n";

$cmd=`${bcp} ${database_name}.dbo.${Trades_Table} out "${Trades_File}" -S $database_server_name -T -q -c -t ,`;
print $cmd;
($cmd =~ /Error|Msg/) and die "BCP of Table ${Trades_Table} failed.";

print "Finished : Write CITCO Trades feed\n";

###############################
#  Extract  tw_CITCO_ScheduledFunds table
###############################
print "Write CITCO Scheduled Funds feed\n";
$TradesFS_Table=${ini}->property($section, 'TABLE4');
$TradesFS_File=${workdir} . "\\".${ini}->property($section, 'FILE4')."\.csv";
print "Copy to the final file ${TradesFS_File}\n";

$cmd=`${bcp} ${database_name}.dbo.${TradesFS_Table} out "${TradesFS_File}" -S $database_server_name -T -q -c -t ,`;
print $cmd;
($cmd =~ /Error|Msg/) and die "BCP of Table ${TradesFS_Table} failed.";

print "Finished : Write CITCO Scheduled Funds feed\n";

###############################
#  Extract  tw_CITCO_Actual_Funding table
###############################
print "Write CITCO Actual Funding feed\n";
$ActFund_Table=${ini}->property($section, 'TABLE5');
$ActFund_File=${workdir} . "\\".${ini}->property($section, 'FILE5')."\.csv";
print "Copy to the final file ${ActFund_File}\n";

$cmd=`${bcp} ${database_name}.dbo.${ActFund_Table} out "${ActFund_File}" -S $database_server_name -T -q -c -t ,`;
print $cmd;
($cmd =~ /Error|Msg/) and die "BCP of Table ${ActFund_Table} failed.";

print "Finished : Write CITCO Actual Funding feed\n";


###############################
#  Remove unwanted Space
###############################

sub reform 
{
# reformat bcp file
my ($strIn, $strOut) = @_;      # raw file, reformated file
my $msg = "";   #error message
my @flds;		# array of field names
my $src = ", ,";
my $src1 = ', ,';
my $des = ",,";
my $des1 = ',,';
eval{
	(-e $strIn) or die $msg= "File ${strIn} not found";

	# open files
	open strINPUT, "<${strIn}" or die $msg= "Can't open source file ${strIn}";
	open strOUTPUT, ">${strOut}" or die $msg= "Can't open target file ${strOut}";

	# reformat file
	while (<strINPUT>) 
	{
    		my $ln = $_;
		$ln =~ s/$src/$des/g;
		$ln =~ s/$src/$des/g;
		$ln =~ s/$src/$des/g;
		$ln =~ s/$src/$des/g;
		chomp;  # clean trailing
		print strOUTPUT "${ln}";   # output
	}

	close strINPUT;
	close strOUTPUT;
    } || {$msg = $@};		#we got an error
return ($msg);
}

# Object  : extract_placement_activity.pl
# Purpose : Run the stored procedure to report the Private 
# Placements from DealGen over the last two weeks
# job runs evey fifteen minutes so errors in database or file copy are trapped and sent as warnings rather then failures

use strict;
use ActiveState::Config::INI;
use File::Copy;
use Mail::Sender;
use POSIX qw(strftime);

my $proc;
my $cmd;
my @vwbuff;
my $row;
my @col;
my $dtm= strftime("%Y%m%d.%H%M%S",localtime);

my $scriptname="extract_private_placement_activity.pl";
my $section = "PrivPlace";
my $msg = "";

###############################
## Process DGD_Environ.ini
###############################
my $ini=ActiveState::Config::INI->new("DGD_Environ.ini") or die "Can't read DGD_Environ.ini: ";
print "$scriptname: Starting\n";
my $workdir=${ini}->property($section, 'WORKDIR') . '\\';
print "Work dir: ${workdir}\n";

my $sqlcmd = ${ini}->property('GENERAL', 'SQLCMD');
my $database_name = ${ini}->property('GENERAL', 'DATABASE_NAME');
print "Database: ${database_name}\n";
my $database_server_name = ${ini}->property('GENERAL', 'DATABASE_SERVER_NAME');
print "Database Server: ${database_server_name}\n";

my $PP_path = ${ini}->property($section, 'PRIVACT_BUSINESSDIR') . '\\';
my $msrvr = ${ini}->property('GENERAL', 'MSRVR');		# smtp server
my $maddr = ${ini}->property('GENERAL', 'MADDR');		# support email

# wrap database and copy in eval to trap errors
eval {
	###############################
	# Run the stored procedure to populate the Private Placement table
	###############################
	$proc = "p_5651_r_ext_PrivPlaceActivity";
	print "Dealing with ${proc}\n\n";
	$cmd=`${sqlcmd} -S ${database_server_name} -E -Q "exec ${database_name}..${proc}"` or die;
	(!($cmd =~ /Error|Msg/)) or (!$cmd) or die "Error in ${proc}: ${cmd}\n";
	print "${proc}: ${cmd}\n";

	# #########################################################
	#  Extract  tw_PrivPlacementActivity table
	# #########################################################
	print "Write Private Placement Activity feed\n";
	my $Privates_TABLE="tw_PrivPlacementActivity";
	my $Privates_FILE="PrivatePlacementActivity." . $dtm . "\.csv";
	my $Privates_FILE_EMPTY ="PrivatePlacementActivity." . $dtm . "_empty\.csv";
	print "Copy to the final file ${Privates_FILE}\n";

	$cmd=`${sqlcmd} -S ${database_server_name} -E -Q "select * from ${database_name}..${Privates_TABLE}" -W -s ,` or die;
	($cmd =~ /Msg/) and die "Extract of ${Privates_TABLE} failed:\n${cmd}";

	$cmd =~ s/NULL//g;    #remove NULL

	@vwbuff = split("\n",$cmd);
	my $filecount=0;
  my $colcount = ($vwbuff[0] =~ tr/,//) +1;
	open (RFMT,">",${workdir} . $Privates_FILE)|| die "Cannot open ${workdir}\\$Privates_FILE";
	print RFMT $vwbuff[0]."\n";	# print header
	foreach $row (@vwbuff[2 .. $#vwbuff -2])    # output rows
    {
     $filecount++;

     # wr111705 - 7/2012 - jkl
     #   Change format of SettlementDt and Amount columns.
     #   Row has 25 columns including ones with null value at the end
     #     so we need to specify the column count so as not to lose the nulls.
     @col = split(",",$row,$colcount);

     # Change format of SettlementDt column from mm/dd/yy to dd-mon-yy
     my ($month,$day,$year) = split("/",@col[0]);
     @col[0] = strftime('%d-%b-%y', 0,0,0,$day,$month -1 ,$year);

     # Round Amount column to 2 decimal places
     @col[9] = sprintf("%.2f", @col[9]);

     # put the columns back together into one row and write it to the output file
     $row = join(",",@col);
     print RFMT $row . "\n";
    }

	print "Rowcount = ${filecount}\n";

	close RFMT;
	print "Finished : Write Private Placement Activity feed ${Privates_FILE}\n";

	# #########################################################
	# Copy the extract to the target folder
	# #########################################################

	print "Copy extract to the target folder: ${PP_path}\n";

	if($filecount == 0)
	{
	my $retcode = copy (${workdir} . ${Privates_FILE},${PP_path} . ${Privates_FILE_EMPTY}) or die "Copy to ${PP_path} failed!";
	rename(${workdir} . ${Privates_FILE}, ${workdir} . ${Privates_FILE_EMPTY}) or die "Copy to ${workdir} failed!";
	print "${Privates_FILE_EMPTY} successfully copied to ${PP_path}${Privates_FILE_EMPTY}\n";
	}
	else
	{
	my $retcode = copy (${workdir} . ${Privates_FILE},${PP_path} . ${Privates_FILE}) or die "Copy to ${PP_path} failed!";
	print "${Privates_FILE} successfully copied to ${PP_path}${Privates_FILE}\n";
	}
} || {$msg = $@};		#we got an error

if ($msg) {
	print "\nProcess Failed:\n$msg"; 
	my $sender = new Mail::Sender {smtp => $msrvr};
	if (ref ($sender->MailMsg({to => $maddr,
			from => $maddr, 
			subject => "${scriptname} failed",
			msg => $msg}))) {} 
	else { print "$Mail::Sender::Error\n";}
	}
else {
  print "\nLoad Completed";
  }
exit;

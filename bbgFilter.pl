# Object  : bbgFilter.pl
# Purpose : Filter the BBG xml files 
# 
# job runs with the BBGSocket every Wait Interval until the BBGSocket creates endbbgmove file.

use strict;
use ActiveState::Config::INI;
use File::Copy;
use Mail::Sender;
use POSIX qw(strftime);

my $proc;
my $cmd;
my $row;

my $DEBUG = 0;
my $bbgS = 0;
my $bbgT = 0;
my $bbgTDTM;
my $bbgTM = 0;
my $bbgTMDTM = 0;
my @inArray;
my $line;
my $pos1;
my $pos2;
my $rectyp;
my @rectyps;
my $trader;
my @traders;
my $tranno;
my @trannos;
my $USD;
my $xmlfile;
my @xmlfiles;
my $bbgnew;

my $sec;
my $min;
my $hr;
my $day;
my $mon;
my $yr;

my $scriptname="bbgFilter.pl";
my $section = "BBGFILTER";
my $msg = "";

###############################
## Process DGD_Environ.ini
###############################
my $ini=ActiveState::Config::INI->new("DGD_Environ.ini") or die "Can't read DGD_Environ.ini: ";
print "\n\n$scriptname: Starting\n";
my $xmldir=${ini}->property($section, 'XMLDIR');
print "xml dir: ${xmldir}\n";
my $archivedir = ${ini}->property($section, 'ARCHIVEDIR') . '\\';
print "archive dir: ${archivedir}\n";
my $jobdir = ${ini}->property($section, 'BBGFILTER_JOBDIR') . '\\';
print "job dir: ${jobdir}\n";
my $waitinterval = ${ini}->property($section, 'WAITINTERVAL');
print "wait interval ${waitinterval}\n";
my $sqlcmd = ${ini}->property('GENERAL', 'SQLCMD');
my $database_name = ${ini}->property('GENERAL', 'DATABASE_NAME');
print "Database: ${database_name}\n";
my $database_server_name = ${ini}->property('GENERAL', 'DATABASE_SERVER_NAME');
print "Database Server: ${database_server_name}\n";
my $msrvr = ${ini}->property('GENERAL', 'MSRVR');    # smtp server
my $maddr = ${ini}->property('GENERAL', 'MADDR');    # support email
my $endbbgmove = $jobdir . "endbbgmove";
my $bbgmovelock = $jobdir . "bbgmove.lock";

# values could be in DGD_environ.ini but the definition would be lost.
# Also the transactions processed from Bloomberg have not changed in years.
# "42"|"2")    TRANTYPE=New
# "142"|"102") TRANTYPE=SameDayCancel
# "342"|"302") TRANTYPE=NextDayCancel
# "43"|"6")    TRANTYPE=SameDayCorrect
# "242"|"202") TRANTYPE=NextDayCorrect
@rectyps = qw(2 6 42 43 102 142 202 242 302 342);


###############################
# Function SearchXML
# This searches a line for the given XML tag
# returning the value between the XML tags
###############################
sub SearchXML {
    my ($search) = $_[0];
    my $val;

    if (index($line, "<$search>") > 0) {
        $pos1 = index($line, ">");
        $pos2 = index($line, "</$search>");
        $val = substr($line, ($pos1 + 1), ($pos2 - $pos1 - 1));
        $line =~ s/^\s+//; #remove leading spaces
        print "Search $search, Value $val, Line $line\n" if($DEBUG != 0);
        return $val;
    } #fi found search string
} #end SearchXML function

############################# DateTm #######################################

sub DateTm {

    ($sec, $min, $hr, $day, $mon, $yr) = localtime(time);
    $mon = $mon + 1;
    $yr  = $yr  + 1900;
    $hr  = "0$hr"   if ($hr  < 10);
    $min = "0$min"  if ($min < 10);
    $sec = "0$sec"  if ($sec < 10);
    $day = "0$day"  if ($day < 10);
    $mon = "0$mon"  if ($mon < 10);

} # end of DateTm

############################# MAIN #######################################

############################### Read the Archived Master Trade Tickets
# process BBGTMTrade*.mlv
# populate the array of processed trade masters
#  This way if there are next days changes then we will process them
############################### Master Trade Ticket

print "Change Directory to $archivedir and get the old Master Trades\n";
chdir($archivedir);

@xmlfiles = glob "BBGTMTrade*.mlv";
foreach $xmlfile (@xmlfiles) {
    $bbgTM=0;
    $bbgTMDTM = substr($xmlfile, 10, 17);
    open FILE, "< $xmlfile" or die;
    while (<FILE>) {
         $line = $_;
         chomp ($line);

         if (index($line, "TransactionNbr") > -1) {
             $tranno = &SearchXML("TransactionNbr") ;
             push (@trannos, $tranno);
         } #fi Store Transaction Number in a array...

    } # end of XML file
    close FILE;
    print "Processed Archived $xmlfile\n";

} # end foreach XML Trade Master File.

print "Change Directory to $xmldir\n";
chdir($xmldir);

# wrap loop in eval to trap errors

eval {

    ###############################
    # Get the traders from SQL
    ###############################
    &DateTm;
    print "Get Traders from tc_Trader table at $hr:$min:$sec\n";
    my $Traders_TABLE="tc_Trader";

    $cmd=`${sqlcmd} -S ${database_server_name} -E -Q "select BBGLastLogin from ${database_name}..${Traders_TABLE}" -h-1 -W -s ,` or die;
    ($cmd =~ /Msg/) and die "Extract of ${Traders_TABLE} failed:\n${cmd}";

    $cmd =~ s/NULL//g;    #remove NULL

    @traders = split("\n",$cmd);
    &DateTm;
    print "Finished : getting traders from SQL\n\n\t\tStarting loop at $hr:$min:$sec\n\n";

    ###############################
    # While the file endbbgmove file does not exist
    ###############################
    while (1 == 1) {
        if ( -e $endbbgmove ) {
            &DateTm;
            print "\n\n\nbbgFilter found the endbbgmove file so it is time to exit at $hr:$min:$sec\n\n";
            # Housekeeping remove the exit stage left file...
            unlink $endbbgmove;
            exit;
        } # fi endbbgmove exists

        ###############################
        # Programmer Note: The existence of a file named bbgmove.lock, brought 
        #   into existence temporarily while BBGSocket is writing XML files, 
        #   causes this script to sleep for $LOCK_INTERVAL seconds
        ###############################
        if ( -e $bbgmovelock ) {
            print "BBGSocket processing - Wait for it...\t";
        } else {

            ###############################
            # Initalize variables for the next pass
            ###############################
            $bbgS = 0;
            $bbgTM = 0;
            $bbgT = 0;
            $trader = 0;
            $rectyp = 0;

            ############################### Security Master
            # process BBGSecurity*.mlv
            # Security master XML files are filtered
            #   on CurrencyCd and only process if the value is 'USD'
            ############################### Security Master
            @xmlfiles = glob "BBGSecurity*.mlv";
            foreach $xmlfile (@xmlfiles) {
                open FILE, "< $xmlfile" or die;
                while (<FILE>) {
                     $line = $_;
                     chomp ($line);

                     if (index($line, "CurrencyCd") > -1) {
                         $USD = &SearchXML("CurrencyCd");
                     } #fi Settlement Currency ISO

                } # end of XML file
                close FILE;

                print "$USD, should be USD...\n" if($DEBUG != 0);
                &DateTm;
                if ($USD eq "USD") {
                    $bbgS++;
                    print "Processing\t:\t$xmlfile at $hr:$min:$sec\n";
                    system ("copy", $xmlfile, "*.xml");
                    system ("move", $xmlfile, "..\\bbg_archive\ ");
                } else {
                    print "Discarding\t:\t$xmlfile, non USD currency $USD at $hr:$min:$sec\n";
                    system ("move", $xmlfile, "..\\bbg_discard\ ");
                } #fi USD currency

            } # end for each XML Security file

            # If we have securities wait so BBGFeed can process before the trade files.
            # NEED DIFF COUNTER!!!!
            sleep $waitinterval if ($bbgS++ > 0);

            ############################### Master Trade Ticket
            # process BBGTMTrade*.mlv
            # Filtering takes place on Master Ticket and compares
            #   the pattern for XML attribute LastLogin to a list of  more
            #   authorized JHF userids
            ############################### Master Trade Ticket
            @xmlfiles = glob "BBGTMTrade*.mlv";
            foreach $xmlfile (@xmlfiles) {
                $bbgTM=0;
                $bbgTMDTM = substr($xmlfile, 10, 17);
                open FILE, "< $xmlfile" or die;
                while (<FILE>) {
                     $line = $_;
                     chomp ($line);

                     if (index($line, "SettleCurrencyISOCd") > -1) {
                         $USD = &SearchXML("SettleCurrencyISOCd");
                         $bbgTM++ if ($USD eq "USD");
                     } #fi Settlement Currency ISO

                     if (index($line, "TransactionNbr") > -1) {
                         $tranno = &SearchXML("TransactionNbr") ;
                         push (@trannos, $tranno);
                     } #fi Store Transaction Number in a array...

                     if (index($line, "RecordType") > -1) {
                         $rectyp = &SearchXML("RecordType");
                         @inArray = grep $_ eq $rectyp, @rectyps;
                         $bbgTM++ if ($#inArray > -1);
                     } #fi Record Type

                     if (index($line, "LastLogin") > -1) {
                         $trader = &SearchXML("LastLogin");
                         @inArray = grep $_ eq $trader, @traders;
                         $bbgTM++ if ($#inArray > -1);
                     } #fi Last Login or Trader

                } # end of XML file
                close FILE;

                print "Currency $USD, Record Type $rectyp, Trader $trader, Valid TM count $bbgTM, Tran # $tranno\nTran # array @trannos\n" if($DEBUG != 0);
                &DateTm;
                if (($USD eq "USD") and ($bbgTM == 3)) {
                    print "Processing\t:\t$xmlfile at $hr:$min:$sec\n";
                    system ("copy", $xmlfile, "*.xml");
                    system ("move", $xmlfile, "..\\bbg_archive\ ");
                } else {
                    print "Discarding\t:\t$xmlfile\n\t\tCurrency $USD, Transaction # $tranno, Record Type $rectyp, Trader $trader at $hr:$min:$sec\n";
                    system ("move", $xmlfile, "..\\bbg_discard\ ");
                } #fi USD currency and (Record Type and Trader) are valid

            } # end for each XML Trade Master file
            # If we have Trade Master wait so BBGFeed can process before the trade ticket.
            # NEED DIFF COUNTER!!!!
            sleep $waitinterval if ($bbgTM > 0);

            ############################### Trade Ticket
            # process BBGTrade*.mlv
            # Filtering takes place on Trade Ticket and compares
            #   the pattern for XML attribute LastLogin to a list of 
            #   authorized JHF userids
            ############################### Trade Ticket
            @xmlfiles = glob "BBGTrade*.mlv";
            foreach $xmlfile (@xmlfiles) {
                $bbgT=0;
				$bbgnew= 0;
                $bbgTDTM = substr($xmlfile, 8, 17);
                open FILE, "< $xmlfile" or die;
                while (<FILE>) {
                     $line = $_;
                     chomp ($line);

                     if (index($line, "SettleCurrencyISOCd") > -1) {
                         $USD = &SearchXML("SettleCurrencyISOCd");
                         $bbgT++ if ($USD eq "USD");
                     } #fi Settlement Currency ISO

                     if (index($line, "MasterTicketNbr") > -1) {
                         $tranno = &SearchXML("MasterTicketNbr") ;
                         @inArray = grep $_ eq $tranno, @trannos;
                         if ($#inArray > -1) {
                             $bbgT++;
                         } else {
                             &DateTm;
                             #print "SKIPPING\tTrade Ticket with date time $bbgTDTM it is too new\n" if ($bbgTDTM > $bbgTMDTM);
                             print "SKIPPING\tWe do not have a Master Transaction number equal to $tranno at $hr:$min:$sec\n";
							 $bbgnew = 1;
                             last;
                         } #fi hey the trade ticket is newer than the last trade master
                     } #fi compare transaction number with the list of Master transaction numbers.

                     if (index($line, "RecordType") > -1) {
                         $rectyp = &SearchXML("RecordType");
                         @inArray = grep $_ eq $rectyp, @rectyps;
                         $bbgT++ if ($#inArray > -1);
                     } #fi check for valid Record Type

                     if (index($line, "LastLogin") > -1) {
                         $trader = &SearchXML("LastLogin");
                         @inArray = grep $_ eq $trader, @traders;
                         $bbgT++ if ($#inArray > -1);
                     } #fi Last Login or Trader

                } # end of XML file
                close FILE;
				if  ($bbgnew == 0)  {
					print "Currency $USD, Record Type $rectyp, Trader $trader, Valid TM count $bbgTM, Tran # $tranno\nTran # array @trannos\n" if($DEBUG != 0);
					&DateTm;
					if (($USD eq "USD") and ($bbgT == 4)) {
						print "Processing\t:\t$xmlfile at $hr:$min:$sec\n";
						system ("copy", $xmlfile, "*.xml");
						system ("move", $xmlfile, "..\\bbg_archive\ ");
					} else {
						print "Discarding\t:\t$xmlfile\n\t\tCurrency $USD, Transaction # $tranno, Record Type $rectyp, Trader $trader at $hr:$min:$sec\n";
						system ("move", $xmlfile, "..\\bbg_discard\ ");
					} #fi USD currency and (Master Ticket Number and Record Type and Trader) are valid
				}
            } # end for each XML Trade Ticket file

        } #fi bbgmovelock

        &DateTm;
        print "Waiting for XML(MLV) files from Bloomberg at $hr:$min:$sec\n";
        sleep $waitinterval;

    } #fi endbbgmove
} || {$msg = $@};                #we got an error

if ($msg) {
        print "\nProcess Failed:\n$msg"; 
        my $sender = new Mail::Sender {smtp => $msrvr};
        if (ref ($sender->MailMsg({to => $maddr,
                        from => $maddr, 
                        subject => "${scriptname} failed",
                        msg => $msg}))) {} 
        else { print "$Mail::Sender::Error at $hr:$min:$sec\n";}
        }
else {
  print "\nBBG Filter Complete";
  }
exit;

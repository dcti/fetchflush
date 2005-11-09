#!/usr/bin/perl -T

# Distributed.net e-mail block fetcher
#    Jeff Lawson <jlawson@bovine.net>
#
# The following Perl CPAN packages need to be installed:
#    IO-stringy-1.203
#    MIME-Base64 (2.04 or higher)
#    MIME-tools-4.121
#    MailTools (1.11 or higher)
#    

use strict;
require MIME::Parser;
require MIME::Entity;
require MIME::Base64;          # only indirectly needed
require MIME::QuotedPrint;     # only indirectly needed
require MIME::Body;            # only indirectly needed
require Mail::Send;            # only indirectly needed
require IO::Stringy;           # only indirectly needed
use IPC::SysV qw(IPC_R IPC_W IPC_CREAT ftok);


# explicitly set our path to untaint it
$ENV{'PATH'} = '/bin:/usr/bin';
my $sendmail = '/usr/sbin/sendmail';
umask 002;

# Set our own "from" email address.
my $serveraddress = 'blocks-bounces@distributed.net';

# Constants to control the behavior of fetching.
my $maxfetch = 20000;		# client upper limit 
my $maxinstances = 6;          # maximum number of fetch instances
my $basedir = '/home/blocks/fetchflush';
my $tmpdir = '/tmp/blocks';


# Set the keyserver to flush to.
my $keyserver = 'us.v29.distributed.net';

# Set the default fetch values (modified by the request message).
my $fetchcount = 0;
my $fetchcontest = "rc5-72";
my $suffix = "r72";
my $projectpriority = "OGR=0,OGR-P2=0,RC5-72";
my $fetchblocksize = 31;     	# blocksize (28-33) for rc5-64
my $dnetcbin = "$basedir/dnetc29";


# Redirect our stderr
my ($sec,$min,$hour,$mday,$mon,$year,$wday,$yday,$isdst) = gmtime();
my $year4 = $year + 1900;        # yes this is y2k safe.
my $month = sprintf("%02d", $mon + 1);
my $logfile = "$basedir/logs/fetch-$year4-$month.log";
open( STDERR, ">>$logfile" );


# Our standard message
my $greeting = <<EOM;

This message has been sent to you because you sent mail to
fetch\@distributed.net.  The attached is the output of "dnetc -fetch".

Include "numblocks=yyyy" anywhere in the body of your message. Note
that the client may impose an upper-limit of the number of workunits
you can request at a time.

To request OGR-P2 blocks compatible with the new v2.9008 clients,
include "contest=OGR" anywhere in the body of your message.  The
default is to request RC5-72 blocks (which are only usable by v2.9
clients).

Other than these flags, the contents of any messages sent to
fetch\@distributed.net are ignored.

Three "-fetch" attempts are made, in an attempt to overcome any
network errors.

EOM


#The attached buffers contain approximately the number of workunits you
#requested by the keyword "numblocks" Note that now, numblocks does not
#indicate number of blocks of packets, but workunits. This can mean you
#actually get less packets/blocks, since blocks can contain multiple
#workunits. This makes the behavior of the keyword "blocksize" a little
#different, since this this keyword doesn't influence the number of
#workunits you get.



# Parses an incoming message to decide the best address to send the
# reply message to.
sub FindSender ($)
{
    my $head = shift || return undef;
    my $sender = $head->get('Reply-to', 0) || 
	$head->get('From', 0) || "";
    if( $sender =~ m/(\S+@\S+)/ ) {
	$sender = $1;
	$sender =~ s/^[^<]*<//;
	$sender =~ s/>.*$//;
    } else { 
	undef $sender;
    }
    return $sender;
}



sub ProcessCommands ($)
{
    my $text = shift || "";
#    if ( $text =~ m|blocksize\s*=\s*(\d+)|is ) {
#	$fetchblocksize = int($1);
#	if ($fetchblocksize < 28) { $fetchblocksize = 28; }
#	if ($fetchblocksize > 33) { $fetchblocksize = 33; }
#    }
    if ( $text =~ m|numblocks\s*=\s*(\d+)|is ) {
	$fetchcount = int($1);
	if ($fetchcount < 1) { $fetchcount = 1; }
	if ($fetchcount > $maxfetch) { $fetchcount = $maxfetch; }
    }
    if ( $text =~ m/(?:contest|project)\s*=\s*([\w\-]+)/is ) {
	my $contest = lc $1;
	if ( $contest eq "rc5" || $contest eq "rc5-72" || $contest eq "rc572" ) { 
	    $fetchcontest = "rc5-72";
	    $suffix="r72"; 
	    $keyserver = "us.v29.distributed.net";
	    $dnetcbin = "$basedir/dnetc29";
	    $projectpriority = "OGR=0,OGR-P2=0,RC5-72";
	}
	elsif ( $contest eq "ogr" || $contest eq "ogrp2" || $contest eq "ogr-p2" ) { 
	    $fetchcontest = "ogr_p2";
	    $suffix="ogf";
	    $keyserver = "us.v29.distributed.net";
	    $dnetcbin = "$basedir/dnetc29";
	    $projectpriority = "OGR=0,OGR-P2,RC5-72=0";
	}
    }
}


# Sends an email to a specified address.         
sub SendMessage ($$$)
{
    my $addressee = shift;
    my $subject = shift;
    my $body = shift;
    
    my $top = build MIME::Entity
	Type => "text/plain",
	From => $serveraddress,
	To => $addressee,
	Subject => $subject,
	Data => $body;

    if (!open(MAIL, "| $sendmail -t -i"))
    {
        print STDERR "$$: Unable to launch sendmail.\n";
	exit 0;
    }
    $top->print(\*MAIL);
    close MAIL;
    print STDERR "$$: Sent mail to $addressee\n";
}


# Sends an email (and an optional attachment) to a specified address.
sub SendMessageAttachment ($$$;$$)
{
    my $addressee = shift || "";
    my $subject = shift || "";
    my $body = shift || "";
    my $attachfile = shift || undef;
    my $attachname = shift || undef;

    print STDERR "$$: Starting attachment message\n";

    my $top = build MIME::Entity
	Type => "multipart/mixed",
	From => $serveraddress,
	To => $addressee,
	Subject => $subject;

    attach $top
	Type => "text/plain",
	Data => $body;

    if (defined $attachfile && defined $attachname)
    {
	attach $top 
	    Path => $attachfile,
	    Filename => $attachname,
	    Type => "application/binary-stream",
	    Encoding => "base64";
	close(ATTACH);
    }

    #print STDERR "$$: Launching sendmail\n";
    if (!open(MAIL, "| $sendmail -t -i"))
    {
        print STDERR "$$: Unable to launch sendmail.\n";
	exit 0;
    }
    #print STDERR "$$: Composing message\n";
    eval {
	local $SIG{ALRM} = sub { die "alarm\n" };
	alarm 30;
	$top->print(\*MAIL);
	alarm 0;
    };
    if ($@) {
	print STDERR "$$: body generation timed out\n";
    }
    close(MAIL);
    print STDERR "$$: Sent mail to $addressee\n";
}


# Simultaneous instance checker and limiter.
sub LimitInstances ($$)
{
    my $maxcopies = shift || 4;      # number of instances to check.
    my $ipcid = shift || die;        # arbitrary unique 8-bit integer

    my $shmkey = ftok($0, $ipcid) || die "unable to get key";
    my $shmid = shmget($shmkey, 4 * $maxcopies, IPC_R | IPC_W | IPC_CREAT);
    die "unable to get memory" if (!defined $shmid);

    for (my $i = 0; $i < $maxcopies; $i++) {
        my $memraw;
        shmread($shmid, $memraw, $i * 4, 4) || die "unable to read memory";
        my $mempid = int(unpack('N',$memraw));
	if ($mempid =~ m/(\d+)/) { $mempid = $1; } else { $mempid = 0; }
        if (!$mempid || kill(0, $mempid) <= 0) {
            # found an empty pid slot
            $memraw = pack('N', $$);
            shmwrite($shmid, $memraw, $i * 4, 4) || die "unable to write memory";
            return 1;    # success
        }
    }
    die "no slots available";
}



# Construct our parser object
my $parser = new MIME::Parser;
$parser->parse_nested_messages('REPLACE');
$parser->output_dir($tmpdir);
#$parser->output_prefix("fetch");
$parser->output_to_core('ALL');



# Parse the input stream
my $entity = $parser->read(\*STDIN);
if (!$entity ) {
    my $sender = FindSender($parser->last_head);
    if (! $sender) { 
        print STDERR "$$: Could not parse or find sender.\n";
	print STDERR "$$: Exiting\n";
	exit 0;
    }

    my $bodymsg = <<EOF;
We could not parse your message.  Perhaps it wasn't a MIME encapsulated message?

INSTRUCTIONS FOLLOW:
$greeting
EOF

    SendMessage($sender, "Distributed.Net Block Flusher Failure", $bodymsg);
    print STDERR "$$: Couldn't parse MIME stream from $sender.\n";
    print STDERR "$$: Exiting\n";
    exit 0;
}
$entity->make_multipart;


#
# Determine the sender
my $sender = FindSender($entity->head);
if (! $sender) { 
    print STDERR "$$: Could not find sender.\n";
    print STDERR "$$: Exiting\n";
    exit 0;
}
my $nowstring = gmtime;
print STDERR "$$: Processing message from $sender at $nowstring GMT\n";


#
# Check for process limits
eval {
    LimitInstances($maxinstances, 43);
};
if ($@) {
    print STDERR "$$: Too many instances running ($@).  Sending back error.\n";

    my $bodymsg = <<EOF;
The block fetcher is currently undergoing an abnormal amount of
activity and is unable to process your request at this time. 
Please resend your request in 15 minutes or more and we may be
able to handle your request then.
EOF

    SendMessage($sender, "Distributed.Net Block Fetcher Failure", $bodymsg);
    print STDERR "$$: Exiting\n";
    exit 0;
}


# Determine the subject
my $subject = $entity->head->get('Subject', 0) || "";
chomp $subject;
ProcessCommands($subject);


#
# Iterate through all of the parts and parse commands.
my $num_parts = $entity->parts;
for (my $part = 0; $part < $num_parts; $part++)
{
    # Get the body, as a MIME::Body;
    my $body = $entity->parts($part) || next;
    my $mime_type     = $body->head->mime_type;
    my $mime_encoding = $body->head->mime_encoding;

    # See if we should try to interpret commands from this part
    if ( $mime_type =~ m|^text/|i ) 
    {
	my $IO = $body->open("r")      || die "open body: $!";
	while (defined($_ = $IO->getline)) {
	    ProcessCommands($_);
	}
	$IO->close                  || die "close I/O handle: $!";    
    }

    # Delete the files for any external (on-disk) data:
    if ($body->bodyhandle) {
	$body->bodyhandle->purge;    
    }
}

#
# Ensure that a block request was actually specified
if ( $fetchcount < 1 )
{
    print STDERR "$$: No request found.  Sending help back.\n";

    my $bodymsg = <<EOF;
A complete request was not found.  At the very minimum, your 
request should specify the number of blocks that you would like, 
via the 'numblocks' keyword.

INSTRUCTIONS FOLLOW:
$greeting
EOF

    SendMessage($sender, "Distributed.Net Block Fetcher Failure", $bodymsg);
    print STDERR "$$: Exiting\n";
    exit 0;
}


#
# Generate the temporary filename
my $filename = "$tmpdir/fetch-$$.$suffix";
if ( !open(TOUCH,">$filename") ) 
{
    print STDERR "$$: Could not open temporary file ($filename).\n";

    my $bodymsg = <<EOF;
There was a problem generating a temporary filename for the 
processing of your request.  If the problem persists, please 
contact us so that we can look into resolving the issue.
EOF

    SendMessage($sender, "Distributed.Net Block Fetcher Failure", $bodymsg);
    print STDERR "$$: Exiting\n";
    exit 0;
}
close(TOUCH);
chmod 0666, $filename;          # sigh

my $filebasename = $filename;
$filebasename =~ s/\.$suffix$//;

# $suffix contains "rc5" or the like
# $fetchcontest contains "rc5.ini" or the like

#
# Create an ini file for the client.
#
my $inifilename = "$tmpdir/blocks$$.ini";
my $clientlog = "$tmpdir/blocks$$.log";
if (!open(INI, ">$inifilename")) {
    print STDERR "$$: Fetch unable to write ini file.\n";
    SendMessage($sender, "Distributed.Net Block Fetcher Failure",
        "A fetch was attempted, but no output was produced.  If the ".
	"problem persists, please contact us so that we can look into ".
	"resolving the issue.\n");
    exit 0;
}
print INI <<EOF;
[networking]
autofindkeyserver=0
keyserver=$keyserver

[misc]
project-priority=$projectpriority

[logging]
log-file=$clientlog
log-file-type="no limit"

[buffers]
buffer-file-basename="$filebasename"

[$fetchcontest]
preferred-blocksize=$fetchblocksize
fetch-workunit-threshold=$fetchcount
EOF
close(INI);


#
# Execute the actual fetch sequence
my $results;
print STDERR "$$: Starting request (count=$fetchcount, project=$fetchcontest, blocksize=$fetchblocksize)\n";
chdir $tmpdir;
my $fetchcmd = "$dnetcbin -ini $inifilename -fetch";

if (open(SUB, "$fetchcmd |")) {
    local $/ = undef;
    $results = <SUB>;
    close SUB;
}

if (open (LOG, $clientlog)) {
    local $/ = undef;
    $results = <LOG>;  # this overwrites previous $results, which is OK.
    close LOG;
}

unlink $clientlog;
unlink $inifilename;


#
# Filter out the warning messages and sensitive information.
#
$results =~ s#Truncating buffer file '$tmpdir/fetch-#Fetch ID is '#g;
$results =~ s#to zero packets. \(bad header\)##g;


#
# Mail back the results
#
if ( $results !~ m|\S+| )
{
    print STDERR "$$: Fetch completed with no output.\n";
    SendMessage($sender, "Distributed.Net Block Fetcher Failure",
        "A fetch was attempted, but no output was produced.  If the ".
	"problem persists, please contact us so that we can look into ".
	"resolving the issue.\n");
}
else
{
    my $gotcount = 0;
    if ( $results =~ m|Retrieved (\d+) \w+ packets? \((\d+) work |is ||
	$results =~ m|Retrieved (\d+) packets? \(([\d\.]+) stat|is ) {
        print STDERR "$$: Retrieved $1 packets ($2 work units) from server\n";
        $gotcount = 1;
    }
    elsif ( $results =~ m|Retrieved stats unit (\d+) of|is) {
	print STDERR "$$: Retrieved $1 work units from server\n";
	$gotcount = 1;
    }
    if ( $gotcount < 1 ) {
        print STDERR "$$: Block fetching operations of unknown blocks complete.\n";
    }

    my $bodymsg = <<EOF;
The block fetcher has completed your fetch of $fetchcount requested blocks.

The output of the fetch is shown below:

RESULTS FOLLOW:
$results
EOF

    SendMessageAttachment($sender, "Distributed.Net Block Fetching Results", $bodymsg, $filename, "buff-in.$suffix");
}
unlink $filename;
print STDERR "$$: Exiting\n";
exit 0;



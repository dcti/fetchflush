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
require Mail::Send;            # only indirectly needed
require IO::Stringy;           # only indirectly needed


# explicitly set our path to untaint it
$ENV{'PATH'} = '/bin:/usr/bin';
my $sendmail = '/usr/sbin/sendmail';
umask 002;

# Set our own address
my $serveraddress = 'help';


# Set the default fetch values
my $rc5server = '209.98.32.14'; #nodezero
#my $rc5server = 'us.v27.distributed.net';

my $fetchcount = 0;
my $fetchcontest = "rc5.ini";
my $suffix = "rc5";
my $fetchblocksize = 31;     	# blocksize (28-33)
my $maxfetch = 20000;		# client upper limit 

# Redirect our stderr
my $basedir = '/home/blocks/fetchflush';
my ($sec,$min,$hour,$mday,$mon,$year,$wday,$yday,$isdst) = gmtime();
my $year4 = $year + 1900;        # yes this is y2k safe.
my $month = sprintf("%02d", $mon + 1);
my $logfile = "$basedir/logs/fetch-$year4-$month.log";
open( STDERR, ">>$logfile" );


# Our standard message
my $greeting = <<EOM;
This message has been sent to you because you sent mail
to fetch\@distributed.net.  The attached is the output of
"dnetc -fetch".

Include "numblocks=yyyy" anywhere in the body of
your message. Note that the client may impose an
upperlimit of the number of workunits you can
request in 1 fetch.

To specify a preferred blocksize, include 
"blocksize=xx" anywhere in the body of your mesasage,
"xx" being between 28 and 33.

To request OGR keys, include "contest=OGR" anywhere in 
the body of your message. Default is RC5.

Other than these flags, the contents of any messages sent
to fetch\@distributed.net are ignored.

The attached buffers contain approximately the number of
workunits you requested by the keyword "numblocks"
Note that now, numblocks does not indicate number of blocks
of packets, but workunits. This can mean you actually get
less packets/blocks, since blocks can contain multiple
workunits. This makes the behavior of the keyword 
"blocksize" a little different, since this this keyword
doesn't influence the number of workunits you get.

Three "-fetch" attempts are made, in an attempt to
overcome any network errors.

EOM


# Construct our parser object
my $parser = new MIME::Parser;
$parser->parse_nested_messages('REPLACE');
$parser->output_dir("/tmp/blocks");
$parser->output_prefix("fetch");
$parser->output_to_core('ALL');



# Parse the input stream
my $entity = $parser->read(\*STDIN);
if (!$entity ) {
    my $sender = &FindSender($parser->last_head);
    if (! $sender) { 
        print STDERR "$$: Could not parse or find sender.\n";
	print STDERR "$$: Exiting\n";
	exit 0;
    }
    SendMessage($sender, "Distributed.Net Block Flusher Failure",
		"We could not parse your message.  Perhaps it wasn't ".
                "a MIME encapsulated message?\n\n".
		"INSTRUCTIONS FOLLOW:\n$greeting\nEOF.");
    print STDERR "$$: Couldn't parse MIME stream from $sender.\n";
    print STDERR "$$: Exiting\n";
    exit 0;
}
$entity->make_multipart;


# Determine the subject
my $subject = $entity->head->get('Subject', 0) || "";
chomp $subject;
&ProcessCommands($subject);


#
# Determine the sender
my $sender = &FindSender($entity->head);
if (! $sender) { 
    print STDERR "$$: Could not find sender.\n";
    print STDERR "$$: Exiting\n";
    exit 0;
}
my $nowstring = gmtime;
print STDERR "$$: Processing message from $sender at $nowstring GMT\n";


#
# Iterate through all of the parts
my $results;
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
	    &ProcessCommands($_);
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
    SendMessage($sender, "Distributed.Net Block Fetcher Failure",
    	"A complete request was not found.  At the very minimum, your ".
	"request should specify the number of blocks that you would like, ".
	"via the 'numblocks' keyword.\n\n".
	"INSTRUCTIONS FOLLOW:\n$greeting\nEOF.");
    print STDERR "$$: Exiting\n";
    exit 0;
}


#
# Generate the temporary filename
my $filename = "/tmp/blocks/fetch-".$$.".$suffix";
if ( !open(TOUCH,">$filename") ) 
{
    print STDERR "$$: Could not open temporary file ($filename).\n";
    SendMessage($sender, "Distributed.Net Block Fetcher Failure",
    	"There was a problem generating a temporary filename for the ".
	"processing of your request.  If the problem persists, please ".
	"contact us so that we can look into resolving the issue.");
    print STDERR "$$: Exiting\n";
    exit 0;
}
close(TOUCH);
chmod 0666, $filename;          # sigh

my $filebasename = $filename;
$filebasename =~ s/.$suffix//;

# $suffix contains "rc5" or the like
# $fetchcontest contains "rc5.ini" or the like

#
# Execute the actual fetch sequence
print STDERR "$$: Starting request (count=$fetchcount, contest=$fetchcontest, blocksize=$fetchblocksize)\n";
chdir $basedir;
my $clientlog = '/tmp/blocks-log'.$$;
open(SUB, "$basedir/dnetc -ini $fetchcontest -inbase $filebasename -b $suffix $fetchcount -blsize $suffix $fetchblocksize -a $rc5server -l $clientlog -fetch |");
$/ = undef;
$results = <SUB>;
close SUB;

open (LOG, $clientlog);
$/ = undef;
$results = <LOG>;  # this overwrites previous $results, which is OK.
close LOG;

unlink $clientlog;

#
# Filter out the warning message.
$results =~ s#Truncating buffer file '/tmp/blocks/fetch-#Fetch ID is '#g;
$results =~ s#to zero packets. \(bad header\)##g;


#
# Mail back the results
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
    if ( $results =~ m|Retrieved (\d+) packets \((\S+) work |is ) {
        print STDERR "$$: Retrieved $1 blocks ($2 work units) from server\n";
        $gotcount = 1;
    }
    if ( $results =~ m|Retrieved (\d+) packet \((\S+) work |is ) {
        print STDERR "$$: Retrieved $1 blocks ($2 work units) from server\n";
        $gotcount = 1;
    }
    if ( $gotcount < 1 ) {
        print STDERR "$$: Block fetching operations of unknown blocks complete.\n";
    }
    SendMessageAttachment($sender, "Distributed.Net Block Fetching Results", 
        "The block fetcher has completed your fetch of $fetchcount ".
	"requested blocks.\n\n".
	"** NOTE: THE REQUESTED WORK IS NOW ALWAYS WORKUNITS, NOT\n".
	"** PACKETS, REGARDLESS YOUR PREFERRED BLOCKSIZE\n\n".
	"The output of the fetch is shown below:\n\n".
	"RESULTS FOLLOW:\n$results\nEOF.\n", $filename, "buff-in.$suffix");
}
unlink $filename;
print STDERR "$$: Exiting\n";
exit 0;



sub FindSender
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



sub ProcessCommands
{
    my $text = shift || "";
    if ( $text =~ m|blocksize\s*=\s*(\d+)|is )
    {
	$fetchblocksize = int($1);
	if ($fetchblocksize < 28) { $fetchblocksize = 28; }
	if ($fetchblocksize > 33) { $fetchblocksize = 33; }
    }
    if ( $text =~ m|numblocks\s*=\s*(\d+)|is )
    {
	$fetchcount = int($1);
	if ($fetchcount < 1) { $fetchcount = 1; }
	if ($fetchcount > $maxfetch) { $fetchcount = $maxfetch; }
    }
    if ( $text =~ m|contest\s*=\s*(\w+)|is )
    {
	my $contest = lc $1;
	if    ( $contest eq "rc5" ) { $fetchcontest = "rc5.ini"; $suffix="rc5"; }
	elsif ( $contest eq "des" ) { $fetchcontest = "des.ini"; $suffix="des"; }
	elsif ( $contest eq "csc" ) { $fetchcontest = "csc.ini"; $suffix="csc"; }
	elsif ( $contest eq "ogr" ) { $fetchcontest = "ogr.ini"; $suffix="ogr"; }
    }
}
         
sub SendMessage
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

sub SendMessageAttachment
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
#	    Encoding => "quoted-printable";
	    Encoding => "base64";
	close(ATTACH);
    }

    print STDERR "$$: Launching sendmail\n";
    if (!open(MAIL, "| $sendmail -t -i"))
    {
        print STDERR "$$: Unable to launch sendmail.\n";
	exit 0;
    }
    print STDERR "$$: Composing message\n";
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


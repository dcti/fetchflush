#!/usr/bin/perl -T

# Distributed.net e-mail block flusher
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
use IPC::SysV qw(IPC_R IPC_W IPC_CREAT ftok);


#require MIME::Decoder::UU;
#$decoder = new MIME::Decoder 'x-uuencode' or die "unsupported";
#$decoder->decode(\*STDIN, \*STDOUT);
#/^begin\s*(\d*)\s*(\S*)/; 

# explicitly set our path to untaint it
$ENV{'PATH'} = '/bin:/usr/bin';
my $sendmail = '/usr/sbin/sendmail';
umask 002;

# Set our own address
my $serveraddress = 'help';

# Default options
#my $rc5server = '209.98.32.14';   # nodezero
#my $rc5server = '130.161.38.8';
my $rc5server = 'us.v27.distributed.net';
my $maxinstances = 6;       # maximum number of instances


# Redirect our stderr
my $basedir = '/home/blocks/fetchflush';
my ($sec,$min,$hour,$mday,$mon,$year,$wday,$yday,$isdst) = gmtime();
my $year4 = $year + 1900;        # yes this is y2k safe.
my $month = sprintf("%02d", $mon + 1);
my $logfile = "$basedir/logs/flush-$year4-$month.log";
open( STDERR, ">>$logfile" );

# Our standard message
my $greeting = <<EOM;
This message has been sent to you because you sent mail to
flush\@distributed.net.  The attached is the output of "dnetc -flush"
using your buffer files.  Three attempts are made, in an attempt to
overcome any network errors.

Buffer files must be attached to your message using MIME Base64
encoding.  They must be called "buff-out.xxx", according to the contest
you are flushing.

The email address specified in your client's configuration will be
used when giving credit to flushed blocks (not to the email address
that you are emailing this message from).

However, don't send blocks from version 2.6401 clients here if you
want credit.  Due to a bug in that single client version, the email
address is not stored within the blocks.  Upgrade your clients!

Other than the attachments, the contents of any messages sent to
flush\@distributed.net are ignored.  If you encounter problems with
this service, please send email to help\@distributed.net
EOM



# Construct our parser object
my $parser = new MIME::Parser;
$parser->parse_nested_messages('REPLACE');
$parser->output_dir("/tmp/blocks");
$parser->output_prefix("flush");
$parser->output_to_core('ALL');


# Parse the input stream
my $entity = $parser->read(\*STDIN);
if (!$entity ) {
    my $sender = &FindSender($parser->last_head);
    if (! $sender) {
        print STDERR "$$: Couldn't parse or find sender's address\n";
	print STDERR "$$: Exiting\n";
	exit 0;
    }
    SendMessage($sender, "Distributed.Net Block Flusher Failure",
		"We could not parse your message.  Perhaps it wasn't ".
		"a MIME encapsulated message?\n\n".
                "INSTRUCTIONS FOLLOW:\n$greeting\nEOF.");
    print STDERR "$$: Couldn't parse MIME stream from $sender\n";
    print STDERR "$$: Exiting\n";
    exit 0;
}
$entity->make_multipart;



# Determine the sender
my $sender = &FindSender($entity->head);
if (! $sender) { 
    print STDERR "$$: Could not find sender's email address.\n";
    print STDERR "$$: Exiting\n";
    exit 0;
}
my $nowstring = gmtime;
print STDERR "$$: Processing message from $sender at $nowstring GMT\n";


#
# Check for process limits
eval {
    &LimitInstances($maxinstances, 44);
};
if ($@) {
    print STDERR "$$: Too many instances running.  Sending back error.\n";
    SendMessage($sender, "Distributed.Net Block Flusher Failure",
        "The block flusher is currently undergoing an abnormal amount of ".
        "activity and is unable to process your request at this time.  ".
        "Please resend your request in 15 minutes or more and we may be ".
		"able to handle your request then.\n\nEOF.");
    print STDERR "$$: Exiting\n";
    exit 0;
}


# Determine the subject
my $subject = $entity->head->get('Subject', 0) || "";
chomp $subject;


# Iterate through all of the parts
my $results;
my $num_parts = $entity->parts;
for (my $part = 0; $part < $num_parts; $part++)
{
    # Get the body, as a MIME::Body;
    my $body = $entity->parts($part);
    my $mime_type     = $body->head->mime_type;
    my $mime_encoding = $body->head->mime_encoding;

    # See if we should try to flush this
    if ( $mime_type =~ m|^multipart/|i )
    {
	print STDERR "$$: ignoring nested multipart section\n";
    }
    elsif ( $mime_type !~ m|^text/|i )
    {
	my $IO = $body->open("r");
	if ($IO)
	{
	    my $bodypath = "/tmp/blocks/flush-" . $$ . "-" . $part;
	    my $bodyfullpath = $bodypath . ".rc5";
	    my $clientlog = "/tmp/blocks/log-".$$;

	    if (open(OUTBUFF, ">$bodyfullpath")) {
		my $buffer;
		$IO->read($buffer,5000000);
		syswrite OUTBUFF,$buffer,5000000;
		close(OUTBUFF);
	    }
	    undef $IO;

	    chdir $basedir;
	    chmod 0666, $bodyfullpath;    # sigh...

	    # execute the client and capture its console output.
	    open(SUB, "$basedir/dnetc -outbase $bodypath -flush -a $rc5server -l $clientlog |");
	    local $/ = undef;
	    my $subresults = <SUB>;
	    close SUB;

	    # read in the entire logfile output.
	    open(LOG, $clientlog);
            local $/ = undef;
            my $logresults = <LOG>;
            close LOG;
	    unlink $clientlog;

	    # try first to send back the logfile output, but only if
	    # it appears to contain useful content.  otherwise send
	    # back the entire capture of the console output.
	    if ($logresults =~ m/\S+/ && 
		length($logresults) > 80) {
		$results .= $logresults;
	    } else {
		$results .= $subresults;
	    }

	    # delete the temporarily saved buffer file.
	    unlink $bodyfullpath;
	    #unlink $bodypath.".rc5";
	    #unlink $bodypath.".des";
	    #unlink $bodypath.".ogr";
	    #unlink $bodypath.".csc";
	}
	else
	{
	    print STDERR "$$: warning failed to open $mime_type ($mime_encoding) body\n";
	}
    }

    # Delete the files for any external (on-disk) data:
    if ($body->bodyhandle) {
	$body->bodyhandle->purge;
    }
}


# Mail back the results
if ( !$results || $results !~ m|\S+| )
{
    print STDERR "$$: Flush completed with no output.\n";

    SendMessage($sender, "Distributed.Net Block Flusher Failure",
		"Flush completed with no output.  Perhaps we could not ".
		"find any MIME-attached files that looked like a buffer ".
		"file?\n\nINSTRUCTIONS FOLLOW:\n$greeting\nEOF.");
}
else
{
    my $gotcount = 0;
    while ( $results =~ m|Sent (\d+) packets? \((\S+) work|gis ) {
        print STDERR "$$: Block flushing of $1 blocks ($2 workunits) complete.\n";
        $gotcount = 1;
    }
    if ( ! $gotcount ) {
        print STDERR "$$: Block flushing operations complete (unknown results).\n";
    }
    SendMessage($sender, "Distributed.Net Block Flusher Results",
		"Flush completed with output.  The results are shown ".
		"at the bottom of this message.\n\n" .
		"RESULTS FOLLOW:\n$results\nEOF.");
}
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
    }
    $top->print(\*MAIL);
    close MAIL;
    print STDERR "$$: Sent mail to $addressee\n";
}

sub LimitInstances
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

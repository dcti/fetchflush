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
use MIME::Parser;
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

# Set our own 'From' e-mail address.
my $serveraddress = 'blocks-bounces@distributed.net';

# Default options
my $keyserver = 'us.v27.distributed.net';
my $maxinstances = 6;       # maximum number of simultaneous instances
my $tmpdir = '/tmp/blocks';
my $basedir = '/home/blocks/fetchflush';


# Redirect our stderr to a log file.
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

Buffer files must be attached to your message using either MIME Base64
or UU encoding.  Buffers formats from both client versions v2.8 and
v2.9 are supported.

The email address specified in your client's configuration will be
used when giving credit to flushed blocks (not to the email address
that you are emailing this message from).

Other than the attachments, the contents of any messages sent to
flush\@distributed.net are ignored.  If you encounter problems with
this service, please send email to help\@distributed.net
EOM



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
         
sub SendMessage ($$$)
{
    my ($addressee, $subject, $body) = @_;

    my $top = build MIME::Entity
        Type => "text/plain",
        From => $serveraddress,
        To => $addressee,
        Subject => $subject,
        Data => $body;

    if (!open(MAIL, "| $sendmail -t -i")) {
        print STDERR "$$: Unable to launch sendmail.\n";
    }
    $top->print(\*MAIL);
    close MAIL;
    print STDERR "$$: Sent mail to $addressee\n";
}

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
#$parser->output_prefix("flush");
$parser->output_to_core('ALL');
$parser->extract_uuencode(1);

# Parse the input stream
my $entity = $parser->read(\*STDIN);
if (!$entity ) {
    my $sender = FindSender($parser->last_head);
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
my $sender = FindSender($entity->head);
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
    LimitInstances($maxinstances, 44);
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
    elsif ( $mime_type !~ m|^text/|i )        # any non-text section.
    {
	#$body->binmode(1);     # this doesn't seem to work.
	my $IO = $body->open("r");
	if ($IO)
	{
	    my $basebodypath = "$tmpdir/flush-$$-$part";    # base buffer filename (without extension)
	    my $bodyfullpath = $basebodypath . ".ogr";    # buffer filename (with extension).  exact extension doesn't need to match contents, but needs to be an extension checked by the client.

	    my $clientlog = "$tmpdir/log-$$";      # log filename

	    my $is_v29 = 0;
	    my $bufferfilesize = 0;
	    if (open(OUTBUFF, ">$bodyfullpath")) {
		binmode OUTBUFF;
		my $buffer;
		my $first = 1;
		while ($IO->read($buffer,10240)) {
		    if ($first) {
			# determine if this is a Win32 executable (virus/worm)
			if ($buffer =~ m/^MZ/) {
			    print STDERR "$$: ignoring Win32 executable.\n";
			    exit 0;
			}

			# determine if the buffer was created by a v2.9 client.
			$is_v29 = ($buffer =~ m/^\x83\xB6\x34\x1A/s);

			$first = 0;
		    }
		    $bufferfilesize += syswrite OUTBUFF,$buffer;
		    undef $buffer;
		}
		close(OUTBUFF);
	    }
	    undef $IO;

	    chdir $basedir;
	    chmod 0666, $bodyfullpath;    # sigh...

	    # decide the command-line to execute.
	    my $flushcmd;
	    if ($is_v29) {
		$keyserver = "us.v29.distributed.net";
		$flushcmd = "$basedir/dnetc29 -outbase $basebodypath -flush -a $keyserver -l $clientlog";
		print STDERR "$$: Found $bufferfilesize byte v2.9 client buffer\n";
	    } else {
		$flushcmd = "$basedir/dnetc28 -outbase $basebodypath -flush -a $keyserver -l $clientlog";
		print STDERR "$$: Found $bufferfilesize byte client buffer\n";
	    }

	    # execute the client and capture its console output.
	    my $subresults;
	    if (open(SUB, "$flushcmd |")) {
		local $/ = undef;
		$subresults = <SUB>;
		close SUB;
	    }

	    # read in the entire logfile output.
	    my $logresults;
	    if (open(LOG, $clientlog)) {
		local $/ = undef;
		$logresults = <LOG>;
		close LOG;
	    }
	    unlink $clientlog;

	    # try first to send back the logfile output, but only if
	    # it appears to contain useful content.  otherwise send
	    # back the entire capture of the console output.
	    if ($logresults =~ m/\S+/ && length($logresults) > 80) {
		$results .= $logresults;
	    } else {
		$results .= $subresults;
	    }

	    # delete the temporarily saved buffer file.
	    unlink $bodyfullpath;
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
    while ( $results =~ m/Sent (\d+) packets? \((\S+) (work|stat)/gis ) {
        print STDERR "$$: Block flushing of $1 packets ($2 stats units) complete.\n";
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

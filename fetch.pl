#!/usr/bin/perl5 -T

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
use lib '/usr/local/lib/perl5/site_perl/5.005';
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
my $serveraddress = 'rc5help\@distributed.net';


# Set the default fetch values
my $rc5server = '205.149.163.211';   # rc5.best.net
my $fetchcount = 0;
my $fetchcontest = 1;        # 1=rc5, 2=des
my $fetchblocksize = 30;     # blocksize (28-31)


# Redirect our stderr
my $basedir = '/home/bovine/fetchflush';
my $logfile = "$basedir/fetch.log";
open( STDERR, ">>$logfile" );


# Our standard message
my $greeting = <<EOM;
This message has been sent to you because you sent mail
to fetch\@distributed.net.  The attached is the output of
"rc5des -fetch".  The attached buffers default to 100
blocks of 2^28 keys (or keypairs for DES). Three "-fetch"
attempts are made, in an attempt to overcome any network
errors.

To request blocks of a different size, include
"blocksize=xx" anywhere in your message (subject or body).
To request a buffer file with more or fewer than the default
(100 blocks), include "numblocks=yyyy" anywhere in your
message.  'xx' is any number from 28 to 31.  'yyyy' may be
any number from 1 to 1000.

By default, RC5 blocks are retrieved.  To get DES blocks,
include "CONTEST=DES" in your message (subject or body).

Other than these flags, the contents of any messages sent
to fetch\@distributed.net are ignored.
EOM


# Construct our parser object
my $parser = new MIME::Parser;
$parser->parse_nested_messages('REPLACE');
$parser->output_dir("/tmp");
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


# Determine the sender
my $sender = &FindSender($entity->head);
if (! $sender) { 
    print STDERR "$$: Could not find sender.\n";
    print STDERR "$$: Exiting\n";
    exit 0;
}
my $nowstring = gmtime;
print STDERR "$$: Processing message from $sender at $nowstring GMT\n";


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
my $filename = "/tmp/fetch-".$$.".in";
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



#
# Execute the actual fetch sequence
print STDERR "$$: Starting request (count=$fetchcount, contest=$fetchcontest, blocksize=$fetchblocksize)\n";
chdir $basedir;
open(SUB, "$basedir/rc5des -in $filename -percentoff -b $fetchcount -blsize $fetchblocksize -processdes 0 -p $rc5server -fetch |");
$/ = undef;
$results = <SUB>;
close SUB;


#
# Mail back the results
if ( $results !~ m|\S+| )
{
    print STDERR "$$: Flush completed with no output.\n";
    SendMessage($sender, "Distributed.Net Block Fetcher Failure",
        "A fetch was attempted, but no output was produced.  If the ".
	"problem persists, please contact us so that we can look into ".
	"resolving the issue.\n");
}
else
{
    my $gotcount = 0;
    if ( $results =~ m|Retrieved (\d+) (\S+) blocks from server|is ) {
        print STDERR "$$: Retrieved $1 $2 blocks from server\n";
        $gotcount = 1;
    }
    if ( $gotcount < 1 ) {
        print STDERR "$$: Block fetching operations of unknown blocks complete.\n";
    }
    SendMessageAttachment($sender, "Distributed.Net Block Fetching Results", 
        "The block fetcher has completed your fetch of $fetchcount ".
	"requested blocks.  The output of the fetch is shown below:\n\n".
	"RESULTS FOLLOW:\n$results\nEOF.\n", $filename, "buff-in.rc5");
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
	if ($fetchblocksize > 31) { $fetchblocksize = 31; }
    }
    if ( $text =~ m|numblocks\s*=\s*(\d+)|is )
    {
	$fetchcount = int($1);
	if ($fetchcount < 1) { $fetchcount = 1; }
	if ($fetchcount > 1000) { $fetchcount = 1000; }
    }
    if ( $text =~ m|contest\s*=\s*(\w+)|is )
    {
	my $contest = lc $1;
	if ( $contest eq "rc5" ) { $fetchcontest = 1; }
	elsif ( $contest eq "des" ) { $fetchcontest = 2; }
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
	    Encoding => "quoted-printable";
#	    Encoding => "base64";
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


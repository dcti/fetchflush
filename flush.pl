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

#require MIME::Decoder::UU;
#$decoder = new MIME::Decoder 'x-uuencode' or die "unsupported";
#$decoder->decode(\*STDIN, \*STDOUT);
#/^begin\s*(\d*)\s*(\S*)/; 




# explicitly set our path to untaint it
$ENV{'PATH'} = '/bin:/usr/bin';
my $sendmail = '/usr/sbin/sendmail';
umask 002;

# Set our own address
my $serveraddress = 'rc5help';


# Default options
my $rc5server = 'nodezero.distributed.net';


# Redirect our stderr
my $basedir = '/home/bovine/fetchflush';
my ($sec,$min,$hour,$mday,$mon,$year,$wday,$yday,$isdst) = gmtime();
my $year4 = $year + 1900;        # yes this is y2k safe.
my $month = sprintf("%02d", $mon + 1);
my $logfile = "$basedir/logs/flush-$year4-$month.log";
open( STDERR, ">>$logfile" );


# Our standard message
my $greeting = <<EOM;
This message has been sent to you because you sent mail to
flush\@distributed.net.  The attached is the output of "rc5des -flush"
using your buffer files.  Three attempts are made, in an attempt to
overcome any network errors.

Buffer files must be attached to your message using MIME Base64
encoding.  They must be called "buff-out.rc5" and/or "buff-out.des".

The email address specified in your client's configuration will be
used when giving credit to flushed blocks (not to the email address
that you are emailing this message from).

However, don't send blocks from version 2.6401 clients here if you
want credit.  Due to a bug in that single client version, the email
address is not stored within the blocks.  Upgrade your clients!

Other than the attachments, the contents of any messages sent to
flush\@distributed.net are ignored.  If you encounter problems with
this service, please send email to rc5help\@distributed.net
EOM



# Construct our parser object
my $parser = new MIME::Parser;
$parser->parse_nested_messages('REPLACE');
$parser->output_dir("/tmp");
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


# Determine the subject
my $subject = $entity->head->get('Subject', 0) || "";
chomp $subject;


# Determine the sender
my $sender = &FindSender($entity->head);
if (! $sender) { 
    print STDERR "$$: Could not find sender's email address.\n";
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
	    my $bodypath = "/tmp/flush".$$."-".$part;
	    if (open(OUTBUFF, ">$bodypath"))
	    {
		my $buffer;
		$IO->read($buffer,500000);
		syswrite OUTBUFF,$buffer,500000;
	    }
	    undef $IO;
	    close(OUTBUFF);

	    chdir $basedir;
	    chmod 0666, $bodypath;    # sigh...
	    open(SUB, "$basedir/rc5des -out $bodypath -percentoff -processdes 0 -flush -a $rc5server |");
	    $/ = undef;
	    $results .= <SUB>;
	    close SUB;
	    unlink $bodypath;
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
    if ( $results =~ m|Sent (\d+) (\S+) blocks to server|is ) {
        print STDERR "$$: Block flushing of $1 $2 blocks complete.\n";
        $gotcount = 1;
    }
    if ( ! $gotcount ) {
        print STDERR "$$: Block flushing operations complete (unknown results).\n";
    }
    SendMessage($sender, "Distributed.Net Block Flusher Results",
		"Flush completed with output.  The results are shown ".
		"at the bottom of this message.\n\n" .
                "INSTRUCTIONS FOLLOW:\n$greeting\n\n".
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


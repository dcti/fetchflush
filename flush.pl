#!/usr/local/bin/perl5 -T

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

# explicitly set our path to untaint it
$ENV{'PATH'} = '/bin:/usr/bin';
umask 002;

# Set our own address
my $serveraddress = 'rc5help\@distributed.net';


# Default options
my $rc5server = '205.149.163.211';   # rc5.best.net


# Redirect our stderr
my $basedir = '/home/bovine/fetchflush';
my $logfile = "$basedir/flush.log";
open( STDERR, ">>$logfile" );


# Our standard message
my $greeting = <<EOM;
This message has been sent to you because you sent mail to
flush\@distributed.net.  The attached is the output of "rc5des -flush"
using your buffer files.  Three attempts are made, in an attempt to
overcome any network errors.

Buffer files must be attached to your message using MIME Base64
encoding.  They must be called "buff-out.rc5" and/or "buff-out.des".

Note that blocks saved to buffer files with version 6401 clients will
not be associated with your email, but will be associated with the
email of the client they are flushed from!  Upgrade your clients!

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
	exit 1;
    }
    SendMessage($sender, "Distributed.Net Block Flusher Failure",
		"We could not parse your message.  Perhaps it wasn't ".
		"a MIME encapsulated message?\n\n".
                "INSTRUCTIONS FOLLOW:\n$greeting\nEOF.");
    print STDERR "$$: Couldn't parse MIME stream from $sender\n";
    exit 1;
}
$entity->make_multipart;


# Determine the subject
my $subject = $entity->head->get('Subject', 0) || "";
chomp $subject;


# Determine the sender
my $sender = &FindSender($entity->head);
if (! $sender) { 
    print STDERR "$$: Could not find sender's email address.\n";
    exit 1;
}
my $nowstring = gmtime;
print STDERR "$$: Processing message from $sender at $nowstring\n";


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
    if ( $mime_type !~ m|^text/|i )
    {
	my $IO = $body->open("r")      || die "open body failed";
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
	open(SUB, "$basedir/rc5des -out $bodypath -percentoff -processdes 0 -flush -p $rc5server |");
	$/ = undef;
	$results .= <SUB>;
	close SUB;
	unlink $bodypath;
    }

    # Delete the files for any external (on-disk) data:
    $body->bodyhandle->purge;    
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

    if (!open(MAIL, "| /usr/sbin/sendmail -t -i"))
    {
        print STDERR "Unable to launch sendmail.\n";
    }
    $top->print(\*MAIL);
    close MAIL;
}


package Fluent::Agent::IOUtil;

use 5.014;
use warnings;
use English;
use Log::Minimal;

use POSIX;
use Time::Piece;

use UV;

use constant DEFAULT_WRITE_TIMEOUT => 5;

# callback args: bool (success to write data into stream, or fail)
sub write {
    my ($this, $called, $stream, $msg, $piped_checker, $callback) = @_;
    my ($package, $filename, $line) = caller;
    my $label = $package . ", line " . $line;

    my $written = 0;
    my $piped = 0;
    my $timeout = 0;

    my $timer = UV::timer_init();
    my $timeout_callback = sub {
        return if $written or $piped; # successfully sent, or failed by SIGPIPE already.
        warnf "%s Failed to write message, timeout, %s, error:%s", $label, $called, UV::strerror(UV::last_error());
        $timeout = 1;
        if ($stream) { # UV::write() not failed yet
            $callback->(0);
        }
    };
    my $write_callback = sub {
        my ($status) = @_;
        if ($status != 0) { # failed to write
            return if $timeout or $piped; # already failed in this fluent-agent by write timeout or SIGPIPE

            warnf "%s Failed to write message, %s, error: %s", $label, $called, UV::strerror(UV::last_error());
            $callback->(0);
        }
        if ($timeout) {
            #TODO: mmm.... actually sended?
            warnf "%s Timeout detected for %s", $label, $called;
            return;
        }
        # successfully written
        debugf "Successfully written into stream";
        $written = 1;
        $callback->(1);
    };
    UV::write($stream, $msg, $write_callback);
    if ($piped_checker->()) {
        warnf "%s Pipe reset by peer, %s", $label, $called;
        $piped = 1;
        return $callback->(0);
    }
    #TODO needs one-time map to terminate on shutdown ?
    UV::timer_start($timer, (DEFAULT_WRITE_TIMEOUT * 1000), 0, $timeout_callback);
}

1;

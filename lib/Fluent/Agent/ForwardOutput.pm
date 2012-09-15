package Fluent::Agent::ForwardOutput;

use 5.014;
use English;
use Log::Minimal;

use base 'Fluent::Agent::BaseOutput';

use Try::Tiny;

use Data::MessagePack;

use constant DEFAULT_CONNECT_TIMEOUT => 5; # 5sec

use constant DEFAULT_WRITE_TIMEOUT => 5; # 5sec

use constant CONNECTION_CHECK_INTERVAL => 5; # 5sec
use constant DEFAULT_CONNECTION_KEEPALIVE => 1800; # 30min
use constant CONNECTION_KEEPALIVE_MARGIN_MAX => 30; # 30sec

# use constant RECONNECT_WAIT_INCR_RATE => 1.5;
# use constant RECONNECT_WAIT_MAX => 1800;

#TODO: TCP connection keepalive management
#TODO: Retry wait for bufferes

sub configure {
    my ($self, %args) = @_;

    #TODO: set configurable parameters

    $self->{servers} = +{
        primary => $args{primary}, # arrayref of [host, port]
        secondary => $args{secondary}, # arrayref of [host, port] or blank arrayref
    };
    $self->{mode} = +{ name => 'primary', timeout => 0 };
    # timeout: with secondary mode, timeout unix_time for mode change into primary

    # server means arrayref as [$host, $port]

    # server status
    # server => +{ tcp => $socket, host => $host, port => $port, state => $bool(alive/dead), start => unix_time, timeout => unix_time }
    $self->{status} = +{
        (map { ($_ => { host => $_->[0], port => $_->[1], state => 1 }) } @{$self->primary_servers}, @{self->secondary_servers})
    };

    # flag map now trying to connect (waiting established or timeout)
    $self->{connecting} = +{}; # server => bool

    # connection queue
    $self->{queue} = []; # cyclic queue of [$tcp, $server] used by send_data

    $self->{timers} = +{};

    # TODO Is this needed?
    # $self->{onetime_timers} = []; # onetime timers (non permanent, deleted immediately after timeout watching, and so on)
    $self;
}

sub primary_servers { (shift)->{servers}->{primary}; }
sub secondary_servers { (shift)->{servers}->{secondary} };

sub broken_connection {
    my ($self, $tcp, $server) = @_;

    my $status = $self->{status}->{$server};
    delete $status->{tcp};
    delete $status->{start};
    delete $status->{timeout};
    $status->{state} = 0;

    try { UV::close($tcp) } catch { $_; }; # close socket and ignore all errors
}

sub maintain_connections {
    my ($self) = @_;
    # mode check and mode timeout check
    

    # expired keepalive connection check
}

sub connect {
    my ($self, $server) = @_;
    my ($host, $port) = @$server;

    my $tcp = UV::tcp_init();

    # connect timeout watcher
    my $timer = UV::timer_init();
    my $timeout_callback = sub {
        return if $self->{status}->{$server}->{state};

        # not connected yet (and timeout)
        warnf "failed to connect host %s, port %s", $host, $port;
        delete $self->{connecting}->{$server};
        try { UV::close($tcp); } catch { }; # ignore all errors
        $self->{state}->{$server}->{state} = 0;
    };

    my $established = sub {
        debugf "ESTABLISHED arguments %s", \@_;

        delete $self->{connecting}->{$server};
        my $start = scalar(time());
        # timeout: min(DEFAULT_KEEPALIVE - MARGIN_MAX) <- -> max(DEFAULT_KEEPALIVE + MARGIN_MAX)
        my $timeout = $start + DEFAULT_CONNECTION_KEEPALIVE + int(CONNECTION_KEEPALIVE_MARGIN_MAX * ( 2 * rand(1) - 1 ));

        my $status = $self->{status}->{$server};
        $status->{tcp} = $tcp;
        $status->{state} = 1;
        $status->{start} = $start;
        $status->{timeout} = $timeout;
        push $self->{queue}, [$tcp, $server];
    };
    push $self->{connecting}->{$server} = 1;
    UV::tcp_connect($tcp, $host, $port, $established);
    UV::timer_start($timer, (DEFAULT_CONNECT_TIMEOUT * 1000), 0, $timeout_callback);
}

sub send_data {
    my ($self, $msg, $callback) = @_;

    my $pair = shift $self->{queue};
    return $callback->(0) unless $pair; # no one connection established (yet?), or all connections are in busy

    my ($tcp, $server) = @$pair;

    my $written = 0;
    my $timeout = 0;

    my $timer = UV::timer_init();
    my $timeout_callback = sub {
        return if $written; # successfully sent already.
        warnf "Failed to send message, timeout, to host %s, port %s", @$server, UV::strerror(UV::last_error());
        $timeout = 1;
        if ($tcp) { # UV::write() not failed yet
            $self->broken_connection($tcp, $server);
            $callback->(0);
        }
    }
    my $write_callback = sub {
        my ($status) = @_;
        if ($status) { # failed to write
            return 0 if $timeout; # already failed in this fluent-agent by write timeout

            warnf "Failed to send message to host %s, port %s, message: %s", @$server, UV::strerror(UV::last_error());
            $self->broken_connection($tcp, $server);
            $callback->(0);
        }
        if ($timeout) {
            #TODO mmm.... actually sended?
            warnf "Timeout detected for host %s, port %s", @$server;
            return 0;
        }

        # successfully sent
        push $self->connection_queue, $tcp;
        $callback->(1);
    };
    UV::write($tcp, $msg, $write_callback)
    UV::timer_start($timer, DEFAULT_WRITE_TIMEOUT, 0, $timeout_callback)
}

sub start {
    my ($self) = @_;

    my $timer = UV::timer_init();
    UV::timer_start($timer, CONNECTION_CHECK_INTERVAL, CONNECTION_CHECK_INTERVAL, sub { $self->maintain_connections; });
    $self->{timers}->{connection_maintainer} = $timer;
}

sub output {
    my ($self, $buffer, $callback) = @_;
    if (scalar(@{$self->connection_queue}) < 1) { # no one connection established (yet?), or all connections are in busy
        return $callback->(0);
    }
    my $msg = Data::MessagePack->pack($buffer->data);
    $self->send_data($msg, $callback);
}

sub shutdown {
    my ($self) = @_;
    foreach my $key (keys %{$self->{timers}}) {
        UV::timer_stop($self->{timers}->{$key});
    }
}

1;

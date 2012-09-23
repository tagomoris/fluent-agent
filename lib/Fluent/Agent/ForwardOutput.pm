package Fluent::Agent::ForwardOutput;

use 5.014;
use warnings;
use English;
use Log::Minimal;

use List::MoreUtils;
use Try::Tiny;

use Data::MessagePack;

use UV;

use base 'Fluent::Agent::BaseOutput';
use Fluent::Agent::IOUtil;

use constant DEFAULT_CONNECT_TIMEOUT => 5; # 5sec

use constant DEFAULT_WRITE_TIMEOUT => 5; # 5sec

use constant CONNECTION_CHECK_INTERVAL => 15; # 15sec

use constant DEFAULT_CONNECTION_KEEPALIVE => 1800; # 30min
use constant CONNECTION_KEEPALIVE_MARGIN_MAX => 30; # 30sec

#TODO: Retry wait for bufferes

sub configure {
    my ($self, %args) = @_;

    #TODO: set configurable parameters

    $self->{piped} = $args{piped_checker};

    $self->{servers} = +{
        primary => $args{primary}, # arrayref of [host, port]
        secondary => $args{secondary}, # arrayref of [host, port] or blank arrayref
    };
    $self->{mode} = 'normal';
    # mode: 'normal' or 'broken', 'normal' is status that all primary nodes are alive. 'broken' is else.
    # In 'normal', only primary nodes are used.
    # In 'broken', all of nodes both of primary and secondary used.
    # timeout: with broken mode, timeout is unix_time try to change mode into normal

    # server means arrayref as [$host, $port]

    # server status
    # server => +{ tcp => $socket, host => $host, port => $port, state => $bool(alive/dead), start => unix_time, timeout => unix_time }
    $self->{status} = +{
        (map { ($_ => { host => $_->[0], port => $_->[1], state => 1, standby => 0 }) } $self->primary_servers),
        (map { ($_ => { host => $_->[0], port => $_->[1], state => 1, standby => 1 }) } $self->secondary_servers)
    };

    # flag map now trying to connect (waiting established or timeout)
    $self->{connecting} = +{}; # server => bool

    # connection queue
    $self->{connection_queue} = []; # cyclic queue of [$tcp, $server] used by send_data

    $self->{timers} = +{};

    $self;
}

sub current_mode {
    my ($self) = @_;
    my $time = time();
    my $is_normal = $self->{mode} eq 'normal';
    if ($is_normal) {
        if ( List::MoreUtils::all { $_->{state} == 1 } grep { $_->{standby} == 0 } values(%{$self->{status}}) ) {
            ## 1 or more primary servers are alive
            return 'normal';
        }
        # else; mode is now primary, but all of primary nodes are dead
        warnf "One or more servers are down, mode changed into 'broken'";
        $self->{mode} = 'broken';
        return 'broken';
    }
    # now broken
    # if all primary nodes are backed to alive, turned into 'normal'
    if ( List::MoreUtils::all { $_->{state} == 1 } grep { $_->{standby} == 0 } values(%{$self->{status}}) ) {
        warnf "All of primary nodes are alive back, mode changed into 'normal'";
        $self->{mode} = 'normal';
        return 'normal';
    }

    return 'broken';
}

sub primary_servers { @{(shift)->{servers}->{primary}}; }
sub secondary_servers { @{(shift)->{servers}->{secondary}}; };
sub all_servers { my $self = shift; return ($self->primary_servers, $self->secondary_servers); }

sub close_connection {
    my ($self, $tcp, $server) = @_;

    my $status = $self->{status}->{$server};
    delete $status->{tcp};
    delete $status->{start};
    delete $status->{timeout};

    debugf "TCP socket status, is_writable %s, is_active %s", UV::is_writable($tcp), UV::is_active($tcp);
    try { UV::close($tcp) } catch { $_; }; # close socket and ignore all errors
}

sub broken_connection {
    my ($self, $tcp, $server) = @_;

    $self->close_connection($tcp, $server);
    $self->{status}->{$server}->{state} = 0;
}

sub maintain_connections {
    my ($self) = @_;
    my @target_nodes;
    if ($self->current_mode eq 'normal') {
        @target_nodes = $self->primary_servers;
    } else {
        @target_nodes = $self->all_servers;
    }

    my $now = time();

    # check and close expired keepalive sockets
    foreach my $node (keys %{$self->{status}}) {
        my $status = $self->{status}->{$node};
        next unless $status->{tcp} and $status->{timeout} and $now > $status->{timeout};

        my $tcp = $status->{tcp};
        my $index = List::MoreUtils::first_index { $_->[0] == $tcp } @{$self->{connection_queue}}; # queue is arrayref of [tcp,server]
        if ($index >= 0) {
            splice($self->{connection_queue}, $index, 1);
            infof "Connection keepalive expired, %s:%s", $status->{host}, $status->{port};
            $self->close_connection($tcp, $node);
        }
    }

    # reconnect for broken connection
    foreach my $node (@target_nodes) {
        unless ($self->{status}->{$node}->{tcp}) {
            $self->connect($node) unless $self->{connecting}->{$node};
            next;
        }
    }
}

sub connect {
    my ($self, $server) = @_;
    my ($host, $port) = @$server;
    my $callback = sub {
        my ($status, $results) = @_;
        if ($status != 0) {
            warnf "Cannot resolv host name %s: %s", $host, UV::strerror(UV::last_error);
            return;
        }
        $self->connect_actual($server, $host, $port, $results->[0]);
    };
    UV::getaddrinfo($host, $port, $callback, 4); # 'hint == 4' means AF_INET only (without AF_INET6)
}

sub connect_actual {
    my ($self, $server, $host, $port, $address) = @_;

    my $tcp = UV::tcp_init();

    # connect timeout watcher
    my $timer = UV::timer_init();
    my $timeout_callback = sub {
        return if $self->{status}->{$server}->{state};

        # not connected yet (and timeout)
        warnf "failed to connect host %s (%s), port %s", $host, $address, $port;
        delete $self->{connecting}->{$server};
        try { UV::close($tcp); } catch { }; # ignore all errors
        $self->{state}->{$server}->{state} = 0;
    };
    my $established = sub {
        my ($result) = @_;
        debugf "ForwardOutput connect callback argument (result): %s", $result;

        delete $self->{connecting}->{$server};
        if ($result != 0) {
            warnf "Failed to connect host %s (%s), port %s: %s", $host, $address, $port, UV::strerror(UV::last_error);
            $self->{status}->{$server}->{state} = 0;
            UV::timer_stop($timer);
            return;
        }

        my $start = scalar(time());
        # timeout: min(DEFAULT_KEEPALIVE - MARGIN_MAX) <- -> max(DEFAULT_KEEPALIVE + MARGIN_MAX)
        my $timeout = $start + DEFAULT_CONNECTION_KEEPALIVE + int(CONNECTION_KEEPALIVE_MARGIN_MAX * ( 2 * rand(1) - 1 ));

        my $status = $self->{status}->{$server};
        $status->{tcp} = $tcp;
        $status->{state} = 1;
        $status->{start} = $start;
        $status->{timeout} = $timeout;
        push $self->{connection_queue}, [$tcp, $server];
        infof "Successfully connected to %s(%s):%s, keepalive timeout: %s", $host, $address, $port, scalar(localtime($timeout));
    };
    debugf "Connecting server %s (%s), port %s", $host, $address, $port;
    $self->{connecting}->{$server} = 1;
    UV::tcp_connect($tcp, $address, $port, $established);
    UV::timer_start($timer, (DEFAULT_CONNECT_TIMEOUT * 1000), 0, $timeout_callback);
}

sub send_data {
    my ($self, $msg, $callback) = @_;

    debugf "ForwardOutput connection queue: %s", $self->{connection_queue};
    my $pair = shift $self->{connection_queue};
    return $callback->(0) unless $pair; # no one connection established (yet?), or all connections are in busy

    my ($tcp, $host, $port) = ($pair->[0], $pair->[1]->[0], $pair->[1]->[1]);
    my $target = "host $host, port $port";

    my $cb = sub {
        my ($result) = @_;
        if ($result) {
            push $self->{connection_queue}, $pair;
            return $callback->(1);
        }
        # fail
        $self->broken_connection(@$pair);
        $callback->(0);
    };
    Fluent::Agent::IOUtil->write($target, $tcp, $msg, $self->{piped}, $cb);
}

sub start {
    my ($self) = @_;

    my $timer = UV::timer_init();
    # connect to servers after startup, as early as possible
    UV::timer_start($timer, 1, (CONNECTION_CHECK_INTERVAL * 1000), sub { $self->maintain_connections; });
    $self->{timers}->{connection_maintainer} = $timer;
}

sub output {
    my ($self, $buffer, $callback) = @_;
    if (scalar(@{$self->{connection_queue}}) < 1) { # no one connection established (yet?), or all connections are in busy
        return $callback->(0);
    }
    debugf "ForwardOutput to write data %s", $buffer->data;
    my $msg = '';
    foreach my $data (@{$buffer->data}) {
        $msg .= Data::MessagePack->pack($data);
    }
    $self->send_data($msg, $callback);
}

sub shutdown {
    my ($self) = @_;
    foreach my $key (keys %{$self->{timers}}) {
        UV::timer_stop($self->{timers}->{$key});
        delete $self->{timers}->{$key};
    }
}

1;

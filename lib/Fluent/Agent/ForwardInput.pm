package Fluent::Agent::ForwardInput;

use 5.014;
use English;
use Log::Minimal;

use Try::Tiny;
use Time::Piece;
use Data::MessagePack;

use base 'Fluent::Agent::BaseInput';

use constant DEFAULT_BACKLOG => 100;

sub configure {
    my ($self, %args) = @_;
    $self->{port} = $args{port};
    $self->{open_sockets} = +{};
    my $mp = Data::MessagePack->new();
    $mp->prefer_integer(1);
    $self->{mp} = $mp;
    $self;
}

sub start {
    my ($self) = @_;

    my $udp = UV::udp_init();
    UV::udp_bind($udp, '0.0.0.0', $self->{port})
          && croakf 'bind error(UDP): %s', UV::strerror(UV::last_error());
    UV::udp_recv_start($udp, sub {
        my ($nread, $buf, $sockaddr, $flags) = @_;
        warnf "udp_recv callback arguments: %s", {buf => $buf, sockaddr => $sockaddr, flags => $flags};
        ####TODO write...
        # UV::udp_send($udp, "", 'ip', port, sub{});
    })
        && croakf 'udp recv_start error, port %s: %s', $self->{port}, UV::strerror(UV::last_error());
    $self->{udp_stream} = $udp;

    my $tcp = UV::tcp_init();
    UV::tcp_bind($tcp, '0.0.0.0', $self->{port})
          && croakf 'bind error(TCP): %s', UV::strerror(UV::last_error());
    UV::listen($tcp, DEFAULT_BACKLOG, sub {
        my $client = UV::tcp_init();
        my $r = UV::accept($tcp, $client);
        if ($r) {
            warnf 'accept failed: %s', UV::strerror(UV::last_error());
            return;
        }
        $self->{open_sockets}->{"$client"} = +{ client => $client, buf => '' };
        UV::read_start($client, sub { my ($nread, $buf) = @_; $self->read($client, $nread, $buf); });
    })
        && croakf 'listen error, port %s: %s', $self->{port}, UV::strerror(UV::last_error());
    $self->{tcp_stream} = $tcp;
    $self;
}

sub read {
    my ($self, $client, $nread, $buf) = @_;
    my $client_key = "$client";
    if ($nread < 0) {
        my $err = UV::last_error();
        warnf "client tcp stream read error: %s", UV::strerror($err) if $err != UV::EOF;
        delete $self->{open_sockets}->{$client_key};
        UV::close($client);
        return;
    }
    elsif ($nread == 0) {
        # nothing to read
        return;
    }

    # message Entry {
    #   1: long time
    #   2: object record
    # }
    #
    # message Forward {
    #   1: string tag
    #   2: list<Entry> entries
    # }
    #
    # message PackedForward {
    #   1: string tag
    #   2: raw entries  # msgpack stream of Entry
    # }
    #
    # message Message {
    #   1: string tag
    #   2: long? time
    #   3: object record
    # }
    my $msg;
    my $fullbuf = $self->{open_sockets}->{$client_key}->{buf} . $buf;
    try {
        $msg = $self->{mp}->unpack($fullbuf);
        $self->{open_sockets}->{$client_key}->{buf} = '';
    } catch {
        $msg = undef;
        $self->{open_sockets}->{$client_key} = $fullbuf;
    };
    return unless $msg;

    my $tag = $msg->[0];
    my $entries = $msg->[1];

    if (scalar(@$msg) == 3) { # Message
        $self->emit($tag, @$entries);
    }
    elsif (ref($entries) eq 'Array') { # Forward
        $self->emits_entries($tag, $entries);
    }
    else { # PackedForward
        $self->emits_entries($tag, $entries);
    }
    $self;
}

sub shutdown {
    my ($self) = @_;
    UV::udp_recv_stop($self->{udp_stream});
    foreach my $client_key (keys %{$self->{open_sockets}}) {
        UV::close($self->{open_sockets}->{$client_key}->{client});
    }
    UV::close($self->{tcp_stream});
}

1;

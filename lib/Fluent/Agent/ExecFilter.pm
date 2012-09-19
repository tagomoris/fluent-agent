package Fluent::Agent::ExecFilter;

use 5.014;
use warnings;
use English;
use Log::Minimal;

use Try::Tiny;

use Encode qw( encode_utf8 decode_utf8 );
use POSIX;
use Time::Piece;
use JSON::XS;
use Data::MessagePack;

use UV;
use IO::Socket::UNIX;

use Fluent::Agent::Buffer;

use constant DEFAULT_BACKLOG => 100;

sub new {
    my ($this, %args) = @_;
    my $self = +{};
    bless $self, $this;

    $self->configure(%args) if $self->can('configure');

    return $self;
}

sub configure {
    my ($self, %args) = @_;

    $self->{conf} = +{
        command => $args{command},
        children => $args{children},
        respawn => $args{respawn},
    };

    # $self->{serializer}
    my $in = $args{in};
    my ($in_tag_field, $in_rprefix, $in_aprefix) = (map { $in->{tag}->{$_} } qw(field remove_prefix add_prefix));
    my ($in_time_field, $in_time_format) = ($in->{time}->{field}, $in->{time}->{format});
    my $formatter;
    if ($in->{format}{type} eq 'json' or $in->{format}{type} eq 'msgpack') {
        my $serializer = ($in->{format}{type} eq 'json' ? JSON::XS->new->utf8 : Data::MessagePack->new->canonical->utf8);
        $formatter = sub { my $copy = shift; $serializer->encode($copy); };
    } elsif ($in->{format}{type} eq 'tsv') {
        my $in_fields = $in->{format}->{fields};
        $formatter = sub { my $copy = shift; encode_utf8(join("\t", map { $copy->{$_} || 'NULL' } @$in_fields)) ; };
    } else {
        croakf "Unkown format for serializer: %s", $in->{format}->{type};
    }
    $self->{serializer} = sub {
        my ($tag, $time, $record) = @_;
        my $rtag = ($in_aprefix ? ($in_aprefix . '.') : '') . ($tag =~ s/^$in_rprefix\.//r);
        my $copy = {
            %$record,
            $in_tag_field => $rtag,
            $in_time_field => POSIX::strftime($in_time_format, localtime($time)),
        };
        return $formatter->($copy);
    };

    # self->{deserializer}
    my $out = $args{out};
    my ($out_tag_field, $out_rprefix, $out_aprefix, $out_tstring) = (map { $out->{tag}->{$_} } qw(field remove_prefix add_prefix string));
    my ($out_time_field, $out_time_format) = ($out->{time}->{field}, $out->{time}->{format});
    my $parser;
    if ($out->{format}{type} eq 'json' or $out->{format}{type} eq 'msgpack') {
        my $deserializer = ($out->{format}{type} eq 'json' ? JSON::XS->new->utf8 : Data::MessagePack->new->canonical->utf8);
        $parser = sub {
            my $line = shift;
            try { $deserializer->decode($line); } catch {
                warnf "Failed to deserialize: broken bytes as %s: %s", ref($deserializer), $line;
                undef;
            };
        };
    } elsif ($out->{format}{type} eq 'tsv') {
        my $findex = 0;
        my @out_fields = map { [$_, $findex++] } @{$out->{format}->{fields}};
        $parser = sub {
            my @values = split(/\t/, (shift));
            +{ map { ($_->[0] => ($values[$_->[1]] || 'NULL')) } @out_fields };
        };
    } else {
        croakf "Unkown format for deserializer: %s", $in->{format}->{type};
    }
    $self->{deserializer} = sub {
        my ($line) = @_;
        my $record = $parser->($line);
        return unless $record;

        my $rawtag = delete($record->{$out_tag_field}) || $out_tstring;
        my $tag = ($out_aprefix ? ($out_aprefix . '.') : '') . ($rawtag =~ s/^$out_rprefix\.//r);

        my ($rawtime, $time) = (delete($record->{$out_time_field}), undef);
        if ($out_time_format eq '%s' and $rawtime) {
            $time = int($rawtime);
        } elsif ($rawtime) {
            try { $time = Time::Piece->strptime($rawtime, $out_time_format)->epoch; } catch {
                warnf("Failed to parse time: field %s, format %s, value %s (using current time)",
                      $out_time_field, $out_time_format, $rawtime);
            }
        }
        unless ($time) {
            $time = time();
        }
        return ($tag, $time, $record);
    };

    $self;
}

sub init {
    my ($self, $read_queue, $write_queue) = @_;

    infof "Starting Input plugin: %s", ref($self);
    $self->start() if $self->can('start');
}

sub start {
    my ($self) = @_;
    my $children = {};
    my $queue = [];
    my $i;
    for( $i = 0 ; $i < $self->{conf}->{children} ; $i++ ) {
        my $cb = sub {
            my ($pid, $connected_sock) = @_;
            $children->{$pid} = +{ pid => $pid, sock => $connected_sock };
            push $queue, $connected_sock;
        };
        $self->start_pair($i, $cb);
    }
    $self->{sock_seq} = $i;
    $self->{children} = $children;
    $self->{queue} = $queue;

    #TODO read buffer watcher (and to write data into $sockets)
    #TODO child process/sockets number watcher to respawn (starts after timeout to wait child process' connect)

    $self;
}

sub start_pair {
    my ($self, $seq, $callback) = @_;

    #TODO: windows...
    my $socket_name = "/tmp/fluent-agent.filter.$PID.$seq.sock";
    debugf "Using Unix Domain Socket: %s", $socket_name;

    my $pid = fork();
    croakf "Failed to fork child process for ExecFilter: $!" unless defined $pid;

    if ($pid == 0) {
        $self->start_child($socket_name); # in this method, runs exec();
    }

    # in parent
    $self->start_parent($socket_name, $pid, $callback);
}

sub start_child {
    my ($self, $socket_name) = @_;

    sleep 1; # wait to socket created in parent....

    debugf "Connecting Socket from child ($PID): %s", $socket_name;

    my $sock = IO::Socket::UNIX->new( Peer => $socket_name );

    debugf "Reopening STDIN/STDOUT.... and exec";
    open(STDIN, '<&=', fileno($sock));
    open(STDOUT, '>&=', fileno($sock));

    exec($self->command()) #TODO
        or croakf "Failed to exec: $!";
}

sub start_parent {
    my ($self, $socket_name, $pid, $callback) = @_;
    #     in parent, UV::listen and accept and read_start

    my $pipe = UV::pipe_init(0); # ipc(bool) 0: flag that specify not to pass filehandles over pipe
    UV::pipe_bind($pipe, $socket_name)
          and croakf "Failed to bind pipe %s, %s:", $socket_name, UV::strerror(UV::last_error);

    my $client_sock = UV::pipe_init(0); # ipc(bool) 0: flag that specify not to pass filehandles over pipe
    my $listen_callback = sub {
        my $r = UV::accept($pipe, $client_sock);
        if ($r) {
            warnf "Failed to accept about %s: %s", $socket_name, UV::strerror(UV::last_error);
            return $callback->($pid, undef);
        }
        debugf "Successfully accepted %s", $socket_name;
        my $read_callback = sub {
            my ($nread, $buf) = @_;

            return if $nread == 0; # nothing to read

            if ($nread < 0) { # I/O error
                my $err = UV::last_error();
                if ($err == UV::EOF) {
                    warnf "Connection reset by peer: maybe child process %s crashed.", $pid;
                } else {
                    warnf "Read I/O Error from pid(%s) %s: %s", $pid, $socket_name, $err;
                }
                $self->cleanup_child($pid, $client_sock);
                return;
            }

            $self->filter_output($nread, $buf);
        };
        UV::read_start($client_sock, $read_callback);

        $callback->($pid, $client_sock);
    };
}

sub cleanup_child {
    my ($self, $pid, $sock) = @_;
    #TODO kill child, close socket
}

sub filter_input { # from read buffer
    my ($self, $buffer, $callback) = @_;
    if (scalar(@{$self->{connection_queue}}) < 1) { # no one connection established (yet?), or all connections are in busy
        return $callback->(0);
    }
    debugf "ForwardOutput to write data %s", $buffer->data;
    my $msg = '';
    foreach my $data (@{$buffer->data}) {
        $msg .= Data::MessagePack->pack($data);
    }
    $self->send_data($msg, $callback); # write to exec child's input
}

# buffer for write queue
sub buffer {
    my ($self) = @_;
    my $queue = $self->{queue};
    my $buffer;
    foreach my $b (@$queue) {
        next if $b->marked();
        if ($b->check()) {
            $b->mark();
            next;
        }
        $buffer = $b;
        last;
    }
    unless ($buffer) {
        $buffer = Fluent::Agent::Buffer->new();
        push $queue, $buffer;
    }
    $buffer;
}

sub filter_output { # read callback from exec child's output, and emit buffer into write queue
}

sub shutdown {
    my ($self) = @_;
    # close sockets
    # kill children
    # delete all socket files
    UV::udp_recv_stop($self->{udp_stream});
    foreach my $client_key (keys %{$self->{open_sockets}}) {
        UV::close($self->{open_sockets}->{$client_key}->{client});
    }
    UV::close($self->{tcp_stream});
}

1;

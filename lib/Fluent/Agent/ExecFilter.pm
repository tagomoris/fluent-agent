package Fluent::Agent::ExecFilter;

use 5.014;
use warnings;
use English;
use Log::Minimal;

use Try::Tiny;
use List::MoreUtils;

use Encode qw( encode_utf8 decode_utf8 );
use POSIX;
use Time::Piece;
use JSON::XS;
use Data::MessagePack;

use UV;
use IO::Handle;
use IO::Socket::UNIX;

use base 'Fluent::Agent::BaseFilter';
use Fluent::Agent::IOUtil;

use constant DEFAULT_CHILDREN_WATCHDOG => 15;

use constant DEFAULT_BACKLOG => 100;

sub configure {
    my ($self, %args) = @_;

    $self->{conf} = +{
        command => $args{command},
        children => $args{children},
        respawn => $args{respawn},
        piped => $args{piped_checker},
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

sub command {
    my ($self) = @_;
    $self->{conf}->{command};
}

sub child_pid {
    my ($self, $sock) = @_;
    ($self->{proc}->{children}->{$sock} || {})->{pid};
}

sub start {
    my ($self) = @_;

    my $children = {};
    my $queue = [];
    my $cb = sub {
        my ($pid, $connected_sock, $socket_name) = @_;
        unless (defined $connected_sock and defined $socket_name) {
            if (defined $connected_sock) { try { UV::close($connected_sock); } catch { }; } # ignore error
            if (defined $socket_name) { unlink($socket_name); } # ignore error
            return;
        }
        $children->{$connected_sock} = +{
            pid => $pid,
            sock => $connected_sock,
            sock_name => $socket_name
        };
        push $queue, $connected_sock;
    };
    my $i;
    for( $i = 0 ; $i < $self->{conf}->{children} ; $i++ ) {
        $self->start_pair($i, $cb);
    }
    $self->{proc} = +{
        sock_seq => $i,
        children => $children, # $sock => +{ pid => NUM, sock => $sock, sock_name => $sock_path }
        queue => $queue, # arrayref of $sock
    };

    if ($self->{conf}->{respawn}) {
        my $timer = UV::timer_init();
        $self->{proc}->{watcher} = $timer;
        UV::timer_start($timer, DEFAULT_CHILDREN_WATCHDOG * 1000, DEFAULT_CHILDREN_WATCHDOG * 1000, sub { $self->repair_child($cb); });
    }

    $self;
}

sub repair_child {
    my ($self, $cb) = @_;
    my $children_num = scalar(values %{$self->{proc}->{children}});
    return if $children_num == $self->{conf}->{children};

    my $repairs = $self->{conf}->{children} - $children_num;
    for ( my $i = 0 ; $i < $repairs ; $i++ ) {
        $self->start_pair($self->{proc}->{sock_seq}++, $cb);
    }
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

    debugf "start_child argument: %s", $socket_name;

    sleep 1; # wait to socket created in parent....

    debugf "Connecting Socket from child ($PID): %s", $socket_name;

    my $sock = IO::Socket::UNIX->new( Peer => $socket_name );
    unless ($sock) {
        croakf "Failed to connect unix domain socket to %s: %s", $socket_name, $!;
    }
    $sock->autoflush(1);

    debugf "Reopening STDIN/STDOUT.... and exec";
    open(STDIN, '<&=', fileno($sock));
    open(STDOUT, '>&=', fileno($sock));

    exec($self->command())
        or croakf "Failed to exec: $!";
}

sub start_parent {
    my ($self, $socket_name, $pid, $callback) = @_;
    # in parent, UV::listen and accept and read_start

    debugf "Now binding unix domain socket %s", $socket_name;

    my $pipe = UV::pipe_init(0); # ipc(bool) 0: flag that specify not to pass filehandles over pipe
    UV::pipe_bind($pipe, $socket_name)
          and croakf "Failed to bind pipe %s, %s:", $socket_name, UV::strerror(UV::last_error);
    debugf "Success to bind pipe %s", $socket_name;

    my $client_sock = UV::pipe_init(0); # ipc(bool) 0: flag that specify not to pass filehandles over pipe
    my $listen_callback = sub {
        my $r = UV::accept($pipe, $client_sock);
        if ($r) {
            warnf "Failed to accept about %s: %s", $socket_name, UV::strerror(UV::last_error);
            return $callback->($pid, undef, undef);
        }
        debugf "Successfully accepted %s", $socket_name;
        my $read_callback = sub {
            my ($nread, $buf) = @_;

            debugf "Reading from child process %s, length: %s", $pid, $nread;

            return if $nread == 0; # nothing to read

            if ($nread < 0) { # I/O error
                my $err = UV::last_error();
                if ($err == UV::EOF) {
                    infof "ExecFilter Connection reset by peer: maybe child process %s exited.", $pid;
                } else {
                    warnf "ExecFilter Read I/O Error from pid(%s) %s: %s", $pid, $socket_name, $err;
                }
                $self->cleanup_child($client_sock);
                return;
            }

            $self->filter_output($nread, $buf);
        };
        UV::read_start($client_sock, $read_callback);

        $callback->($pid, $client_sock, $socket_name);
    };
    UV::listen($pipe, DEFAULT_BACKLOG, $listen_callback);
}

sub cleanup_child {
    my ($self, $sock_key) = @_;

    my $child = delete $self->{proc}->{children}->{$sock_key};
    return unless $child;

    infof "To terminate child process of ExecFilter, pid %s", $child->{pid};

    my $sock = $child->{sock};
    my $index = List::MoreUtils::first_index { $_ == $sock } @{$self->{proc}->{queue}}; # queue is arrayref of [tcp,server]
    if ($index >= 0) {
        splice($self->{proc}->{queue}, $index, 1);
    }

    debugf "closing socket to child, pid %s", $child->{pid};
    try { UV::close($child->{sock}); } catch { }; # ignore all errors

    debugf "sending SIGTERM to child, pid %s", $child->{pid};
    try {
        kill(TERM => $child->{pid});
        debugf "Waiting child process to exit (pid %s)", $child->{pid};
        wait;
        debugf "Done wait: %s", $?;
    } catch {
        warnf "Sending SIGTERM to pid %s, error:%s", $child->{pid}, $_;
    };

    #TODO without UNIX domain socket?
    debugf "deleting closed socket socket file %s", $child->{sock_name};
    unlink($child->{sock_name})
        or warnf "Failed to unlink socket path %s: %s", $child->{sock_name}, $!;

    debugf "Success to cleanup for child process %s", $child->{pid};

    $self;
}

sub write_data {
    my ($self, $msg, $callback) = @_;

    my $sock = shift $self->{proc}->{queue};
    return callback->(0) unless $sock; # no one child process running (yet?), or all are in busy

    my $child_pid = $self->child_pid($sock);
    debugf "ExecFilter to write data to child (pid %s), msg: %s", $child_pid, $msg;

    my $cb = sub {
        my ($result) = @_;
        if ($result) {
            push $self->{proc}->{queue}, $sock;
            return $callback->(1);
        }
        # fail
        $self->cleanup_child($sock);
        $callback->(0);
    };
    Fluent::Agent::IOUtil->write($child_pid, $sock, $msg, $self->{conf}->{piped}, $cb);
}

sub output { # from read buffer
    my ($self, $buffer, $callback) = @_;

    debugf "reading data from buffer";
    return $callback->(0) if scalar(@{$self->{proc}->{queue}}) < 1;

    my $msg = '';
    my $r;
    while ($r = $buffer->next_record) {
        debugf "serializing record: %s", $r;
        my $str = $self->{serializer}->(@$r);
        debugf "serialized record: %s", $str;
        $msg .= $str;
        $msg .= "\n";
    }
    $self->write_data($msg, $callback);
}

sub filter_output { # read callback from exec child's output, and emit buffer into write queue
    my ($self, $nread, $buf) = @_;
    my @records;

    debugf "reading data from child process, length: %s, data: %s", $nread, $buf;

    my $pos = 0;
    my $term;
    while($pos <= $nread and ($term = index($buf, "\n", $pos)) >= 0) {
        my $line = substr($buf, $pos, ($term - $pos));

        push @records, [$self->{deserializer}->($line)];
        $pos = $term + 1;
    }
    debugf "deserialized data: %s", \@records;
    $self->emits(@records);
}

sub shutdown {
    my ($self) = @_;

    infof "Stopping all ExecFilter children...";
    UV::timer_stop($self->{proc}->{watcher}) if $self->{proc}->{watcher};

    foreach my $child (values %{$self->{proc}->{children}}) {
        debugf "Stop target: %s", $child->{pid};
        $self->cleanup_child($child->{sock}) if $child->{sock};
    }
    infof "Stopped all ExecFilter children...";

    $self;
}

1;

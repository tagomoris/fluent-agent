package Fluent::Agent::FileInput;

use 5.014;
use English;
use Log::Minimal;

use UV;

use base 'Fluent::Agent::BaseInput';

use Time::Piece;

sub configure {
    my ($self, %args) = @_;
    $self->{path} = $args{path};
    $self->{pattern} = $args{pattern};
    if ( $args{tag_string} ) {
        $self->{tag} = +{ type => 'string', tag => $args{tag_string} };
    } else {
        $self->{tag} = +{ type => 'field', tag => $args{tag_field} };
    }
    if ( $args{time_format} ) {
        $self->{time} = +{ type => 'field', field => $args{time_field}, format => $args{time_format} };
    } else {
        $self->{time} = +{ type => 'now' };
    }
}

sub start {
    my ($self) = @_;
    ###### We neeeeeeeeeed uv_fs_open !
    open(my $fh, "<:encoding(UTF-8)", $self->{path})
        or croakf "failed to open:%s", $self->{path};
    debugf "opening fd";
    $self->{fh} = $fh;
    # seek to last
    debugf "read_start";
    my $uv = UV::read_start($fh, sub { my ($nread, $buf) = @_; $self->read($nread, $buf); });
    debugf "ok";
    $self->{uv} = $uv;
}

sub read {
    my ($self, $nread, $buf) = @_;
    if ($nread < 0) {
        my $err = UV::last_error();
        if ($err == UV::EOF) {
            infof "closed filehandle of path: %s", $self->{path};
        } else {
            warnf 'file read error: %s', UV::strerror($err);
        }
        return;
    } elsif ($nread == 0) {
        return; # nothing to do
    }

    my @records = ();
    my $reading = 0;
    while ( (my $pos = index($buf, "\n", $reading)) > -1 ) {
        my $line = substr($buf, $reading, ($pos - $reading)); # before newline
        $reading = $pos + 1; # after newline
        my $data = {%+};
        my $tag;
        if ($self->{tag}->{type} eq 'string') {
            $tag = $self->{tag}->{tag};
        } else {
            $tag = $data->{$self->{tag}} || 'NOT_FOUND';
        }
        my $time;
        if ($self->{time}->{type} eq 'field') {
            $time = Time::Piece->strptime($data->{$self->{time}->{field}}, $self->{time}->{format});
        } else {
            $time = time();
        }
        push @records, [$tag, $time, $data];
    }
    $self->emits(@records);
}

sub shutdown {
    my ($self) = @_;
    debugf "closing filehandle...";
    UV::close($self->{fh});
}

1;

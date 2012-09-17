package Fluent::Agent::Buffer;

use 5.014;
use warnings;
use English;
use Log::Minimal;
use Time::HiRes qw//;

use Data::MessagePack::Stream;

sub new {
    my ($this, %args) = @_;
    my $flush_interval = $args{flush_interval} || 3; # 3sec
    my ($sec, $usec) = Time::HiRes::gettimeofday();
    my $self = +{type => 'list', data => [], mark => 0, flush_time => [$sec + $flush_interval, $usec]};

    if ($args{tag} and $args{data}) {
        my $tag = $args{tag};
        my $data = $args{data};
        if (ref($data) eq 'ARRAY') {
            $self->{type} = 'forward';
            $self->{data} = [[$tag, $data]];
        }
        else {
            $self->{type} = 'msgpack';
            $self->{data} = [[$tag, $data]];
        }
        $self->{mark} = 1;
    }

    bless $self, $this;
}

sub marked { (shift)->{mark}; }

sub mark {
    my $self = shift;
    $self->{mark} = 1;
}

sub check {
    my $self = shift;
    my ($sec, $usec) = Time::HiRes::gettimeofday();
    return 0 if $sec < $self->{flush_time}->[0];
    return $usec > $self->{flush_time}->[1] if $sec == $self->{flush_time}->[0];
    return 1;
}

# data:
# 1. arrayref of record (type 'list')
#    record: [tag, time, hashref-of-data] # Message
# 2. [tag, arrayref-of-[time, hashref-of-data]] # Forward (type 'forward')
# 3. [tag, msgpack-of-arrayref-of-[time, hashref-of-data]] ? # PackedForward (type 'msgpack')
sub data { (shift)->{data}; }

sub records {
    my $self = shift;
    my $data = $self->{data};
    if ($self->{type} eq 'list') {
        return @{$self->{data}};
    } elsif ($self->{type} eq 'forward') {
        my ($t, $arrayref) = @{$self->{data}};
        return (map { [ $t, $_->[0], $_->[1] ] } @$arrayref);
    } else { # msgpack
        my ($tag, $msgpack) = @{$self->{data}};
        my $entries = Data::MessagePack->unpack($msgpack);
        return (map { [ $tag, $_->[0], $_->[1] ] } @$entries);
    }
}

sub next_record {
    my $self = shift;
    croakf "failed to iterate about non-marked buffer" unless $self->{mark};

    $self->{position} //= 0;
    if ($self->{type} eq 'list') {
        return $self->{data}->[ $self->{position}++ ];
    }
    elsif ($self->{type} eq 'forward') {
        return undef unless $self->{data}->[1]->[ $self->{position} ];
        return [$self->{data}->[0], @{$self->{data}->[1]->[ $self->{position}++ ]}];
    }
    else { #msgpack
        unless ($self->{unpacker}) {
            $self->{unpacker} = Data::MessagePack::Stream->new;
            $self->{unpacker}->feed($self->{data}->[1]);
        }
        return undef unless $self->{unpacker}->next;
        return [$self->{data}->[0], @{$self->{unpacker}->data}];
    }
}

1;

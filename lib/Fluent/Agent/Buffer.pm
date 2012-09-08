package Fluent::Agent::Buffer;

use 5.014;
use Log::Minimal;
use Time::HiRes qw//;

sub new {
    my ($this, $flush_interval) = @_;
    $flush_interval //= 3; # 3sec
    my ($sec, $usec) = Time::HiRes::gettimeofday();
    my $self = +{data => [], mark => 0, flush_time => [$sec + $flush_interval, $usec]};
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

# data: arrayref of record
# record: [tag, time, hashref-of-data]
sub data { (shift)->{data}; }

1;

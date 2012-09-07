package Fluent::Agent::FileOutput;

use 5.014;
use English;
use Log::Minimal;

use Time::Piece;
use Time::HiRes;

use UV;

use Data::MessagePack;
use JSON::XS;

sub new {
    my ($this, %args) = @_;
    bless +{}, $this;
}

sub init {
    my ($self, $queue) = @_; # reading queue (arrayref)
}

sub stop {
    my ($self) = @_;
}

1;

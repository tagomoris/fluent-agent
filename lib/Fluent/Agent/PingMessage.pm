package Fluent::Agent::PingMessage;

use 5.014;
use English;
use Log::Minimal;

use Time::Piece;
use Time::HiRes;

use UV;

sub new {
    my ($this, %args) = @_;
    bless +{}, $this;
}

sub init {
    my ($self, $queue) = @_;
}

sub stop {
    my ($self) = @_;
}

1;

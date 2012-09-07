package Fluent::Agent::ForwardInput;

use 5.014;
use English;
use Log::Minimal;

use Time::Piece;
use Time::HiRes;

use UV;

use IO::Socket::INET;
use Data::MessagePack;
use JSON::XS;

use constant DEFAULT_SOCKET_TIMEOUT => 5; # 5sec

sub new {
    my ($this, %args) = @_;
    bless +{}, $this;
}

sub init {
    my ($self, $queue) = @_; # writing queue (arrayref)
}

sub stop {
    my ($self) = @_;
}

1;

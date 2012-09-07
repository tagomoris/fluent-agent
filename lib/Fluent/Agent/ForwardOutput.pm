package Fluent::Agent::ForwardOutput;

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

use constant DEFAULT_CONNECTION_KEEPALIVE => 1800; # 30min
use constant CONNECTION_KEEPALIVE_MARGIN_MAX => 30; # 30sec
use constant DEFAULT_RECONNECT_WAIT_MIN => 0.5;
use constant DEFAULT_RECONNECT_WAIT_MAX => 3600; # 60min

use constant RECONNECT_WAIT_INCR_RATE => 1.5;

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

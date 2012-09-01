package Fluent::Agent v0.0.1;

use 5.14.0;
use Carp;

use Time::Piece;
use Time::HiRes;
use Log::Minimal;

# use UV; mmm...

use IO::Socket::INET;
use Data::MessagePack;
use JSON::XS;

sub new {
    # input, output, filter, ping, buffers
    my ($this, %args) = @_;
}

sub execute {
    # checker (term, reload)
    my ($self, %args) = @_;
}

1;

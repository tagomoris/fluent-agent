package Fluent::Agent::ForwardInput v0.0.1;

use 5.14.0;
use Carp;

use Time::Piece;
use Time::HiRes;
use Log::Minimal;

# use UV; mmm...

use IO::Socket::INET;
use Data::MessagePack;
use JSON::XS;

use constant DEFAULT_SOCKET_TIMEOUT => 5; # 5sec

sub new {
    my ($this, %args) = @_;
    
}

1;

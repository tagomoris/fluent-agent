package Fluent::Agent::FileInput;

use 5.014;
use English;
use Log::Minimal;

use Time::Piece;
use Time::HiRes;

use UV;

use Data::MessagePack;
use JSON::XS;

use Fluent::Agent::Buffer;

sub new {
    my ($this, %args) = @_;
    bless +{}, $this;
}

sub init {
    my ($self, $queue) = @_; # writing queue (arrayref)
}

sub stop {
}

1;

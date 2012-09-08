package Fluent::Agent::FileOutput;

use 5.014;
use English;
use Log::Minimal;

use base 'Fluent::Agent::BaseOutput';

use JSON::XS;

sub start {
}

sub output {
}

sub shutdown {
    my ($self) = @_;
}

1;

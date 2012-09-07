package Fluent::Agent::Input;

use 5.014;
use English;
use Log::Minimal;

use Fluent::Agent::FileInput;
use Fluent::Agent::ForwardInput;

sub new {
    my ($this, %args) = @_;
    if ($args{type} eq 'file') {
        return Fluent::Agent::FileInput->new(%args);
    } elsif ($args{type} eq 'forward') {
        return Fluent::Agent::ForwardInput->new(%args);
    } else {
        croakf "unknown input type %s", $args{type};
    }
}

1;

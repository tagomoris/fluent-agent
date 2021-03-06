package Fluent::Agent::Output;

use 5.014;
use warnings;
use English;
use Log::Minimal;

use Fluent::Agent::FileOutput;
use Fluent::Agent::ForwardOutput;

use Fluent::Agent::StderrOutput;

sub new {
    my ($this, %args) = @_;
    if ($args{type} eq 'file') {
        return Fluent::Agent::FileOutput->new(%args);
    } elsif ($args{type} eq 'forward') {
        return Fluent::Agent::ForwardOutput->new(%args);
    } elsif ($args{type} eq 'stderr') {
        return Fluent::Agent::StderrOutput->new(%args);
    } else {
        croakf "unknown input type %s", $args{type};
    }
}

1;

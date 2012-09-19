package Fluent::Agent::Filter;

use 5.014;
use warnings;
use English;
use Log::Minimal;

use Fluent::Agent::ExecFilter;

sub new {
    my ($this, %args) = @_;
    if ($args{type} eq 'exec') {
        return Fluent::Agent::ExecFilter->new(%args);
    } else {
        croakf "unknown filter type %s", $args{type};
    }
}

1;

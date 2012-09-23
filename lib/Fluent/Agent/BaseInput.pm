package Fluent::Agent::BaseInput;

use 5.014;
use warnings;
use English;

use base 'Fluent::Agent::BasePlugin';

sub new {
    my ($this, %args) = @_;
    my $self = +{};
    bless $self, $this;

    $self->setup(input => 1);

    $self->configure(%args) if $self->can('configure');

    return $self;
}

1;

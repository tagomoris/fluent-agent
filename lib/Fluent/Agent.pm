package Fluent::Agent v0.0.1;

use 5.14.0;
use Log::Minimal;

use Try::Tiny;

use Time::Piece;
use Time::HiRes;

# use UV; mmm...

use Fluent::Agent::Input;
use Fluent::Agent::Output;
use Fluent::Agent::PingMessage;

#TODO use Fluent::Agent::Filter;

sub new {
    # input, output, filter, ping, buffers
    my ($this, %args) = @_;
    my $self = +{
        input => Fluent::Agent::Input->new(%{$args{input}}),
        output => Fluent::Agent::Output->new(%{$args{input}}),
    };
    if (defined $args{ping}) {
        $self->{ping} = Fluent::Agent::PingMessage->new(%{$args{ping}});
    }
    if (defined $args{filter}) {
        $self->{filter} = Fluent::Agent::Filter->new(%{$args{filter}});
    }

    return bless $self, $this;
}

sub init {
    my $self = shift;
    debugf "Initializing Fluent::Agent";
    #TODO catch exceptions
    $self->{input}->init();
    $self->{ping} and $self->{ping}->init();
    $self->{filter} and $self->{filter}->init();
    $self->{output}->init();
    debugf "Initializing complete";
}

sub stop {
    my $self = shift;
    debug "Stopping Fluent::Agent";
    #TODO catch exceptions
    $self->{input}->stop();
    $self->{ping} and $self->{ping}->stop();
    $self->{filter} and $self->{filter}->stop();
    $self->{output}->stop();
    debugf "Stopping complete";
}

sub execute {
    # checker (term, reload)
    my ($self, %args) = @_;
    my $check_terminated = $args{checker}{term};
    my $check_reload = $args{checker}{reload};

    while(not $check_terminated->()) {
        $self->init(); # first initialization, or got reload
        $self->run($check_reload);
    }
    warnf "Process terminated";
}

1;

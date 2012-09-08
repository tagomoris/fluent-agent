package Fluent::Agent::BaseOutput;

use 5.014;
use English;
use Log::Minimal;

use UV;

use constant DEFAULT_OUTPUT_EVENT_INTERVAL => 100; # ms

sub new {
    my ($this, %args) = @_;
    my $self = +{};
    bless $self, $this;

    $self->configure(%args) if $self->can('configure');

    return $self;
}

# sub configure {
#     my ($self, %args) = @_;
# }

sub init {
    my ($self, $queue) = @_; # reading queue (arrayref)

    infof "Starting Output plugin: %s", ref($self);

    $self->{queue} = $queue;

    $self->start() if $self->can('start');

    my $output_callback = sub {
        return if scalar(@{$self->{queue}}) < 1;
        my $buffer = shift $self->{queue};
        $self->output($buffer);
    };

    my $timer = UV::timer_init();
    $self->{timer} = $timer;
    UV::timer_start($timer, $self->{interval}, $self->{interval}, $output_callback);
}

# sub start {
#     my ($self) = @_;
# }

# sub output {
#     my ($self, $buffer) = @_;
# }

sub stop {
    my ($self) = @_;

    infof "Stopping Output plugin: %s", ref($self);

    UV::timer_stop($self->{timer});
    $self->shutdown() if $self->can('shutdown');
}

# sub shutdown {
#     my ($self) = @_;
# }

1;

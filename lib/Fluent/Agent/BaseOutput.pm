package Fluent::Agent::BaseOutput;

use 5.014;
use warnings;
use English;
use Log::Minimal;

use UV;

use constant DEFAULT_OUTPUT_EVENT_INTERVAL => 100; # ms

sub new {
    my ($this, %args) = @_;
    debugf "Initializing Output: %s", {this => $this, args => \%args};
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

    $self->{__queue} = $queue;
    $self->{__interval} = $self->{watch_interval} || DEFAULT_OUTPUT_EVENT_INTERVAL;

    $self->start() if $self->can('start');

    my $output_timer_event = sub {
        # debugf "output queue size %s, to output", scalar(@{$self->{queue}});
        my $buffer = shift $self->{__queue};
        return unless defined $buffer;
        # debugf "output plugin %s", ref($self);

        my $callback = sub {
            my $result = shift;
            push $self->{__queue}, $buffer unless $result;
            $result;
        };
        $self->output($buffer, $callback);
    };

    my $timer = UV::timer_init();
    $self->{__timer} = $timer;
    UV::timer_start($timer, $self->{__interval}, $self->{__interval}, $output_timer_event);
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

    UV::timer_stop($self->{__timer});
    $self->shutdown() if $self->can('shutdown');
}

# sub shutdown {
#     my ($self) = @_;
# }

1;

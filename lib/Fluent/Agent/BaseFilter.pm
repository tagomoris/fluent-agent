package Fluent::Agent::BaseFilter;

use 5.014;
use warnings;
use English;
use Log::Minimal;

use Try::Tiny;

use UV;

use Fluent::Agent::Buffer;

use constant DEFAULT_FILTER_INPUT_EVENT_INTERVAL => 100; # ms

sub new {
    my ($this, %args) = @_;
    my $self = +{};
    bless $self, $this;

    $self->configure(%args) if $self->can('configure');

    return $self;
}

sub init {
    my ($self, $read_queue, $write_queue) = @_;

    infof "Starting Filter plugin: %s", ref($self);
    $self->{__queues} = +{
        read => $read_queue,
        write => $read_queue,
    };

    $self->start() if $self->can('start');

    my $watcher_callback = sub {
        my $buf = shift $self->{__queues}->{read};
        return unless defined $buf;
        my $callback = sub {
            my $result = shift;
            push $self->{__queues}->{read}, $buf unless $result;
            $result;
        };
        $self->filter_input($buf, $callback);
    };
    my $watcher_timer = UV::timer_init();
    $self->{__timer} = $watcher_timer;
    UV::timer_start($watcher_timer, DEFAULT_FILTER_INPUT_EVENT_INTERVAL, DEFAULT_FILTER_INPUT_EVENT_INTERVAL, $watcher_callback);
}

# sub start {
#     my ($self) = @_;
# }

# sub filter_input {
#     my ($self, $buffer) = @_;
# }

# buffer for write queue
sub buffer {
    my ($self) = @_;
    my $queue = $self->{__queues}->{write};
    my $buffer;
    foreach my $b (@$queue) {
        next if $b->marked();
        if ($b->check()) {
            $b->mark();
            next;
        }
        $buffer = $b;
        last;
    }
    unless ($buffer) {
        $buffer = Fluent::Agent::Buffer->new();
        push $queue, $buffer;
    }
    $buffer;
}

sub emit {
    my ($self, $tag, $time, $record) = @_;
    my $buffer = $self->buffer;
    push $buffer->data, [$tag, $time, $record];

    $buffer->mark() if $buffer->check();
}

# record: [tag, time, hashref-of-data]
sub emits {
    my ($self, @records) = @_;
    my $buffer = $self->buffer;
    push $buffer->data, @records;

    $buffer->mark() if $buffer->check();
}

# (tag, arrayref-of-[time, record]) OR (tag, msgpack-of-arrayref-of-[time, record])
sub emits_entries {
    my ($self, $tag, $data) = @_;
    push $self->{__queues}->{write}, Fluent::Agent::Buffer->new(tag => $tag, data => $data);
}

sub stop {
    my ($self) = @_;

    infof "Stopping Filter plugin: %s", ref($self);

    UV::timer_stop($self->{__timer});
    $self->shutdown() if $self->can('shutdown');
}

# sub shutdown {
#     my ($self) = @_;
# }

1;

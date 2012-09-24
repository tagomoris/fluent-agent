package Fluent::Agent::BasePlugin;

use 5.014;
use warnings;
use English;
use Log::Minimal;

use Try::Tiny;

use UV;

use Fluent::Agent::Buffer;

use constant DEFAULT_WRITEQUEUE_MARK_INTERVAL => 250; # ms
use constant DEFAULT_READQUEUE_FIND_INTERVAL => 100; # ms

sub plugin_type {
    my ($self) = shift;
    return 'filter' if $self->{__type}->{input} and $self->{__type}->{output};
    return 'input' if $self->{__type}->{input};
    return 'output' if $self->{__type}->{output};
    croakf "invalid type name of plugin: input %s, output %s", $self->{__type}->{input}, $self->{__type}->{output};
}

sub setup {
    my ($self, %args) = @_;

    if ($args{input}) {
        $self->{__type}->{input} = 1;
    }
    if ($args{output}) {
        $self->{__type}->{output} = 1;
    }
    if ($args{filter}) {
        $self->{__type}->{input} = 1;
        $self->{__type}->{output} = 1;
    }

    $self->{__queues} = +{};
    $self->{__timers} = +{};

    $self;
}

sub init {
    my ($self, @queues) = @_;

    infof "Starting %s plugin: %s", $self->plugin_type, ref($self);

    if ($self->plugin_type eq 'input') {
        $self->{__queues}->{write} = $queues[0];
    } elsif ($self->plugin_type eq 'output') {
        $self->{__queues}->{read} = $queues[0];
    } else { # filter
        $self->{__queues} = +{
            read => $queues[0],
            write => $queues[1],
        };
    }

    $self->start() if $self->can('start');

    #TODO: $self->{__interval} = $self->{watch_interval} || DEFAULT_OUTPUT_EVENT_INTERVAL;

    if ($self->{__queues}->{read}) {
        my $watcher_callback = sub {
            my $buf = shift $self->{__queues}->{read};
            return unless defined $buf;
            my $callback = sub {
                my $result = shift;
                push $self->{__queues}->{read}, $buf unless $result;
                $result;
            };
            $self->output($buf, $callback);
        };
        my $watcher_timer = UV::timer_init();
        $self->{__timers}->{read_watcher} = $watcher_timer;
        UV::timer_start($watcher_timer, DEFAULT_READQUEUE_FIND_INTERVAL, DEFAULT_READQUEUE_FIND_INTERVAL, $watcher_callback);
    }
    if ($self->{__queues}->{write}) {
        my $marker_callback = sub {
            foreach my $buf (@{$self->{__queues}->{write}}) {
                $buf->mark() if $buf->check();
            }
        };
        my $marker_timer = UV::timer_init();
        $self->{__timers}->{write_marker} = $marker_timer;
        UV::timer_start($marker_timer, DEFAULT_WRITEQUEUE_MARK_INTERVAL, DEFAULT_WRITEQUEUE_MARK_INTERVAL, $marker_callback);
    }
}

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

sub try_flush {
    my ($self) = shift;
    my $buffer = $self->buffer;
    $buffer->mark() if scalar(@{$buffer->data}) > 0;
    $self;
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

    infof "Stopping %s plugin: %s", $self->plugin_type, ref($self);

    foreach my $timer (values %{$self->{__timers}}) {
        UV::timer_stop($timer);
    }

    $self->shutdown() if $self->can('shutdown');
}

1;

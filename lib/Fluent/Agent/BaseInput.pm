package Fluent::Agent::BaseInput;

use 5.014;
use English;
use Log::Minimal;

use Fluent::Agent::Buffer;

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
    my ($self, $queue) = @_; # writing queue (arrayref)

    infof "Starting Input plugin: %s", ref($self);

    $self->{queue} = $queue;

    $self->start() if $self->can('start');
}

# sub start {
#     my ($self) = @_;
# }

sub buffer {
    my ($self) = @_;
    my $queue = $self->{queue};
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

sub stop {
    my ($self) = @_;

    infof "Stopping Input plugin: %s", ref($self);

    $self->shutdown() if $self->can('shutdown');
}

# sub shutdown {
#     my ($self) = @_;
# }

1;

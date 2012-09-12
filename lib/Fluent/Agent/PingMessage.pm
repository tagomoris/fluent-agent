package Fluent::Agent::PingMessage;

use 5.014;
use English;
use Log::Minimal;

use UV;

sub new {
    my ($this, %args) = @_;
    bless +{ tag => $args{tag}, data => $args{data}, interval => $args{interval} * 1000 }, $this;
}

sub init {
    my ($self, $queue) = @_;
    $self->{queue} = $queue;

    infof "Start pinging: %s", ref($self);

    my $timer = UV::timer_init();
    $self->{timer} = $timer;
    UV::timer_start($timer, $self->{interval}, $self->{interval}, sub{ $self->ping(); });
}

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

    $buffer->mark(); # always flush to send ping immediately
}

sub ping {
    my ($self) = @_;
    my $t = time();
    debugf "Pinging at %s(%s)", scalar(localtime($t)), $t;
    $self->emit($self->{tag}, scalar(time()), +{data => $self->{data}});
}

sub stop {
    my ($self) = @_;

    infof "Stop pinging: %s", ref($self);

    UV::timer_stop($self->{timer});
}

1;

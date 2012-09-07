package Fluent::Agent v0.0.1;

use 5.014;
use Log::Minimal;

use Time::Piece;
use Time::HiRes;

use UV;

use Fluent::Agent::Buffer;

use Fluent::Agent::Input;
use Fluent::Agent::Output;
use Fluent::Agent::PingMessage;

#TODO use Fluent::Agent::Filter;

our $QUEUE_FLUSHER_INTERVAL = 100; # ms

sub new {
    # input, output, filter, ping, buffers
    my ($this, %args) = @_;
    my $self = +{
        filtered => 0,
        input => Fluent::Agent::Input->new(%{$args{input}}),
        output => Fluent::Agent::Output->new(%{$args{input}}),
    };
    if (defined $args{ping}) {
        $self->{ping} = Fluent::Agent::PingMessage->new(%{$args{ping}});
    }
    if (defined $args{filter}) {
        $self->{filter} = Fluent::Agent::Filter->new(%{$args{filter}});
        $self->{filtered} = 1;
    }

    $self->{queues} = +{
        primary => +{
            writing => [],
            reading => [],
        },
        ($self->{filtered} ? (
            secondary => +{
                writing => [],
                reading => [],
            },
        ))
    };
    $self->{timers} = {
        queue => undef,
        signal => undef,
    };

    return bless $self, $this;
}

sub queue {
    my ($self,$type) = @_;
    my $primary = $self->{queues}->{primary};
    my $secondary = $self->{queues}->{secondary};

    # if filter doesn't exist:
    #  input -> primary{writing} -> (+ ping) -> primary{reading} -> output
    # if filter exists
    #  input -> pri{writing} -> pri{reading} -> filter stdin
    #  filter stdout -> secondary{writing} -> (+ping) -> secondary{reading} -> output

    if ($type eq 'input') {
        return $primary->{writing};
    }
    elsif ($type eq 'ping') {
        return $secondary->{writing} if $self->{filtered};
        return $primary->{writing};
    }
    elsif ($type eq 'output') {
        return $secondary->{reading} if $self->{filtered};
        return $primary->{reading};
    }
    elsif ($type eq 'filter stdin') {
        return $primary->{reading};
    }
    elsif ($type eq 'filter stdout') {
        return $secondary->{writing};
    }
    else {
        croakf "invalid type of queue request: %s", $type;
    }
}

sub move_queues {
    my $self = shift;
    my $primary = $self->{queues}->{primary};
    my $secondary = $self->{queues}->{secondary};
    foreach my $queue ($primary, ($secondary || ())) {
        my $size = scalar(@{$queue->{writing}});
        for (my $i = $size - 1 ; $i >= 0 ; $i--) {
            if ($queue->{writing}->[$i]->marked()) {
                my ($buf) = splice($queue->{writing}, $i, 1);
                push $queue->{reading}, $buf;
            }
        }
    }
}

sub init {
    my $self = shift;
    debugf "Initializing Fluent::Agent";

    $self->{input}->init( $self->queue('input') );
    $self->{ping}->init( $self->queue('ping') ) if $self->{ping};
    $self->{filter}->init( $self->queue('filter stdin'),  $self->queue('filter stdout') ) if $self->{filter};
    $self->{output}->init( $self->queue('output') );

    debugf "Initializing complete";
}

sub setup_queue_timer {
    my $self = shift;
    my $timer_queues = UV::timer_init();
    UV::timer_start($timer_queues, $QUEUE_FLUSHER_INTERVAL, $QUEUE_FLUSHER_INTERVAL, sub { $self->move_queues(); });
    $self->{timers}->{queue} = $timer_queues;
    $self;
}

sub setup_signal_watcher {
    my ($self, $term, $reload) = @_;
    my $timer_signal_watcher = UV::timer_init();
    my $watcher = sub {
        return unless $reload->();
        if ($term->()) {
            $self->stop();
        }
        else {
            $self->reload();
            $reload->(1); # reload done
        }
    };
    UV::timer_start($timer_queues, $QUEUE_FLUSHER_INTERVAL, $QUEUE_FLUSHER_INTERVAL, $watcher);
    $self->{timers}->{signal} = $timer_signal_watcher;
    $self;
}

sub reload {
    my $self = shift;

    infof "reloading all plugins";
    $self->{input}->stop();
    $self->{ping} and $self->{ping}->stop();
    $self->{filter} and $self->{filter}->stop();
    $self->{output}->stop();

    $self->{input}->init( $self->queue('input') );
    $self->{ping}->init( $self->queue('ping') ) if $self->{ping};
    $self->{filter}->init( $self->queue('filter stdin'),  $self->queue('filter stdout') ) if $self->{filter};
    $self->{output}->init( $self->queue('output') );
}

sub stop {
    my $self = shift;
    debug "Stopping Fluent::Agent";
    $self->{input}->stop();
    $self->{ping} and $self->{ping}->stop();
    $self->{filter} and $self->{filter}->stop();
    $self->{output}->stop();
    debugf "Stopping complete";
}

sub start {
    my $self = shift;
    debugf "Starting agent uv event loop ...";
    UV::run();
    debugf "Exited uv event loop ...";
}

sub execute {
    my ($self, %args) = @_;
    my $check_terminated = $args{checker}{term};
    my $check_reload = $args{checker}{reload};

    infof "Start to initialize plugins.";

    $self->init();

    infof "All plugins are successfully initialized, starting agent...";

    $self->setup_queue_timer();
    $self->setup_signal_watcher($check_terminated, $check_reload);

    $self->start();

    infof "Fluent::Agent exits.";
}

1;

# Input Plugin
# - fdを監視するイベント登録、きたら処理
#   - writing buffer の check() を監視して、いっぱいだったら mark() する
#   - writing queue に buffer がなければ new して push する

# Output plugin
# - タイマ登録、オブジェクトストレージのflushedを監視して、あったらゲットして処理

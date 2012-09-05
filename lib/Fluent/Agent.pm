package Fluent::Agent v0.0.1;

use 5.014;
use Log::Minimal;

use Try::Tiny;

use Time::Piece;
use Time::HiRes;

use UV;

use Fluent::Agent::Buffer;

use Fluent::Agent::Input;
use Fluent::Agent::Output;
use Fluent::Agent::PingMessage;

#TODO use Fluent::Agent::Filter;

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

    return bless $self, $this;
}

sub queue {
    my ($self,$type) = @_;
    my $primary = $self->{queues}->{primary};
    my $secondary = $self->{queues}->{secondary};

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

sub init {
    my $self = shift;
    debugf "Initializing Fluent::Agent";

    $self->{input}->init( $self->queue('input') );

    $self->{ping}->init( $self->queue('ping') ) if $self->{ping};

    $self->{filter}->init( $self->queue('filter stdin'),  $self->queue('filter stdout') ) if $self->{filter};

    $self->{output}->init( $self->queue('output') );

    debugf "Initializing complete";
}

sub start {
    my $self = shift;
    debugf "Starting agent uv event loop ...";
    UV::run();
    debugf "Exited uv event loop ...";
}

#### TODO rewrite
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
    my ($self, %args) = @_;
    my $check_terminated = $args{checker}{term};
    my $check_reload = $args{checker}{reload};

    #TODO: register timer to check check_reload/check_terminated
    # $check_reload->()
    # $check_terminated->()

    infof "Start to initialize plugins.";

    $self->init();

    infof "All plugins are successfully initialized, starting agent...";

    $self->start();

    infof "Fluent::Agent exits.";
}

1;

# Agentのオブジェクトストレージ
# - input参照とoutput参照が同一のもの : filterがないケース
# - input参照とfilter stdin参照が同一、filter stdout参照とoutput参照が同一 : filterがあるケース
# pluginのinitはオブジェクトストレージつけてやらないとダメか

# Agent側のタイマ
# - reload/termフラグをチェック、立ってたら処理
# - オブジェクトストレージの列をチェックして flush タイミングになったらマークしてflushed列に移す

# Input Plugin
# - fdを監視するイベント登録、きたら処理

# Output plugin
# - タイマ登録、オブジェクトストレージのflushedを監視して、あったらゲットして処理

# 3. UV::run()

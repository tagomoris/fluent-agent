package Fluent::Agent v0.0.1;

use 5.14.0;
use Log::Minimal;

use Try::Tiny;

use Time::Piece;
use Time::HiRes;

use UV;

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

    try { $self->{input}->init(); } catch {
        croakf "Failed to initialize input: %s", $_;
    };
    $self->{ping} and $self->{ping}->init();
    try { $self->{filter} and $self->{filter}->init(); } catch {
        croakf "Failed to initialize filter: %s", $_;
    };
    try { $self->{output}->init(); } catch {
        croakf "Failed to initialize output: %s", $_;
    };

    debugf "Initializing complete";
}

sub start {
    my $self = shift;
    debugf "Starting Fluent::Agent uv event loop ...";
    UV::run();
    debugf "Started.";
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

    #TODO: setup of object storage

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

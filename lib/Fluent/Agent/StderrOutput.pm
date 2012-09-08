package Fluent::Agent::StderrOutput;

use 5.014;
use English;
use Log::Minimal;

use base 'Fluent::Agent::BaseOutput';

use JSON::XS qw//;

sub start {
    my ($self) = @_;
    use IO::Handle;
    autoflush STDOUT 1;
    autoflush STDERR 1;
    $self->{json} = JSON::XS->new->utf;
}

sub output {
    my ($self, $buffer) = @_;
    while ( my $record = shift($buffer->{data}) ) {
        print STDERR $record->[0], " ", $record->[1], " ", $self->{json}->encode($record->[2]), "\n";
    }
}

1;

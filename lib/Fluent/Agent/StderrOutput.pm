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
    $self->{json} = JSON::XS->new->utf8;
}

sub output {
    my ($self, $buffer) = @_;
    debugf "Let's output data: %s", $buffer;
    while ( my $record = $buffer->next_record ) {
        print STDERR $record->[0], " ", $record->[1], " ", $self->{json}->encode($record->[2]), "\n";
    }
}

1;

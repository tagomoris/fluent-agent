package Fluent::Agent::Options v0.0.1;

sub parse {
    # Getopt::Long see single char option as first char of long option, not alias option
    # So we should parse all of single char options by Getopt::Std, and later, parse long options by Getopt::Long
    use Getopt::Std;
    my %opts;
    getopts('hdL:vqi:I:t:T:w:W:p:o:f:s:P:E', \%opts); #TODO filter options

    my %options;
    use Getopt::Long;
    GetOptions(\%options,
               qw(
                     help daemonize log=s verbose quiet
                     input-file=s input-pattern=s tag=s tag-field=s time-format=s time-field=s
                     input-port=i
                     output-file=s
                     forward-primary=s@ forward-secondary=s@
                     stderr
                     ping=s
             )); #TODO filter options
    return merge_commandline_option(%opts, %options);
}

sub merge {
    my ($args, $short, $long, $default) = @_;
    return $args->{$short} || $args->{$long} || $default;
}

sub merge_list {
    my ($args, $short, $long, $default) = @_;
    if ($args->{$short}) { return [$args->{$short}]; }
    if ($args->{$long}) { return $args->{$long}; }
    return $default;
}

sub merge_commandline_option {
    my %args = @_;
    my $args = \%args;
    my $merged = +{
        help      => merge($args, 'h', 'help', 0),
        daemonize => merge($args, 'd', 'daemonize', 0),
        logpath   => merge($args, 'L', 'log', undef),
        verbose   => merge($args, 'v', 'verbose', 0),
        quiet     => merge($args, 'q', 'quiet', 0),

        input_file    => merge($args, 'i', 'input-file', undef),
        input_pattern => merge($args, 'I', 'input-pattern', undef),
        tag           => merge($args, 't', 'tag', undef),
        tag_field     => merge($args, 'T', 'tag-field', undef),
        time_format   => merge($args, 'w', 'time-format', undef),
        time_field    => merge($args, 'W', 'time-field', undef),

        input_port => merge($args, 'p', 'input-port', undef),

        output_file => merge($args, 'o', 'output-file', undef),

        forward_primary   => merge_list($args, 'f', 'forward-primary', undef),
        forward_secondary => merge_list($args, 's', 'forward-secondary', undef),

        stderr => merge_list($args, 'E', 'stderr', undef),

        ping_options => merge($args, 'P', 'ping', undef),

        # TODO filter options
    };
    return $merged;
}

1;


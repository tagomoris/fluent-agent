#!/usr/bin/env perl

use 5.014;
use warnings;
use English;

use UV;
use Data::MessagePack;

use Getopt::Long qw(:config posix_default no_ignore_case gnu_compat);

my %option;
GetOptions(\%option, qw( type|t=s msgs|m=i rate|r=i seconds|s=i parallels|p=i ) );

unless ($option{type} and $option{msgs} and $option{rate} and $option{seconds}) {
    die "type/rate/seconds options must be specified";
}

my $tag = 'dummy';
my $dummy_time = time;

my @lines = <DATA>;
chomp @lines;

my $packer = Data::MessagePack->new->utf8;
my $times = $option{msgs} / 5;
my $data = '';
if ($option{type} eq 'messages') {
    for ( my $i = 0 ; $i < $times ; $i++ ) {
        $data .= join('', map { $packer->pack([$tag, $dummy_time, +{message => $_}]) } @lines );
    }
} elsif ($option{type} eq 'forward') {
    my @entries;
    for ( my $i = 0 ; $i < $times ; $i++ ) {
        push @entries, map { [$dummy_time, +{message => $_}] } @lines;
    }
    $data = $packer->pack([$tag, \@entries]);
} else { # packed forward
    my $entries_stream = '';
    for ( my $i = 0 ; $i < $times ; $i++ ) {
        $entries_stream .= join('', map { $packer->pack([$dummy_time, +{message => $_}]) } @lines );
    }
    $data = $packer->pack([$tag, $entries_stream]);
}

my ($host, $port) = @ARGV;

my $interval = int(1000.0 / $option{rate});
my $parallels = $option{parallels} || 10;

my $max = $option{rate} * $option{seconds};

my @connections = ();
my $send_times = 0;

my $connector = sub {
    my $tcp = UV::tcp_init();
    my $cb = sub {
        my ($status) = @_;
        if ($status != 0) {
            die "failed to connect " . UV::strerror(UV::last_error);
        }
        push @connections, $tcp;
    };
    UV::tcp_connect($tcp, $host, $port, $cb);
};
for ( my $c = 0 ; $c < $parallels ; $c++ ) {
    UV::timer_start(UV::timer_init(), 100, 0, $connector);
}
my $sender = sub {
    my $conn = shift @connections;
    return unless $conn; # all connections are busy

    UV::write($conn, $data, sub{ $send_times += 1; push @connections, $conn; });
};
my $main_timer = UV::timer_init();
UV::timer_start($main_timer, $interval, $interval, $sender);
UV::timer_start(UV::timer_init, $option{seconds} * 1000, 0, sub { UV::timer_stop($main_timer); });

my $start_at = time();

UV::run();

my $end_at = time();
my $secs = $end_at - $start_at;

say "Message type:", $option{type}, ", parallels: ", $parallels;
say "Running: ", $secs, " seconds";
say "Max send count: ", $max, ", messages: ", $max * $option{msgs}, ", rate: ", int($max * $option{msgs} / $secs);
say "Actual count:   ", $send_times, ", messages: ", $send_times * $option{msgs}, ", rate: ", int($send_times * $option{msgs} / $secs);

exit(0);

__DATA__
203.0.113.254 - - [24/Sep/2012:15:58:18 +0900] "GET /lite/archives/999999999.html HTTP/1.1" 200 6240 "-" "Mozilla/5.0 (iPhone; CPU iPhone OS 6_0 like Mac OS X) AppleWebKit/536.26 (KHTML, like Gecko) Mobile/10A403" blog.livedoor.jp - 10183
203.0.113.213 - - [24/Sep/2012:15:58:18 +0900] "GET /image/design/icon_category.gif HTTP/1.1" 200 339 "http://news4vip.livedoor.biz/archives/51907252.html" "Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.0; Trident/5.0)" blog.livedoor.jp - 93
203.0.113.13 - - [24/Sep/2012:15:58:19 +0900] "GET /img/content-top-news.png HTTP/1.1" 200 4850 "http://www.kotaro269.com/archives/cat_10003573.html" "Mozilla/5.0 (Windows NT 5.1) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/21.0.1180.89 Safari/537.1" blog.livedoor.jp - 112
203.0.113.47 - - [24/Sep/2012:15:58:20 +0900] "GET /atom.xml HTTP/1.1" 200 163655 "http://spon.me/archives/51764517.html" "Mozilla/4.0 (compatible; MSIE 8.0; Windows NT 5.1; Trident/4.0; .NET4.0C; .NET CLR 1.1.4322; .NET CLR 2.0.50727; .NET CLR 3.0.4506.2152; .NET CLR 3.5.30729)" blog.livedoor.jp - 87764
203.0.113.99 - - [24/Sep/2012:15:58:21 +0900] "GET /misopan_news/archives/51858597.html HTTP/1.1" 200 26694 "http://anaguro.yanen.org/" "Mozilla/4.0 (compatible; MSIE 8.0; Windows NT 6.1; Trident/4.0; GTB7.4; SLCC2; .NET CLR 2.0.50727; .NET CLR 3.5.30729; .NET CLR 3.0.30729; Media Center PC 6.0; CMNTDF; .NET4.0C; BRI/2)" blog.livedoor.jp - 21287

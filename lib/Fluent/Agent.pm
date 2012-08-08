package Fluent::Agent 0.0.1;

use 5.12.0;
use Carp;

use Time::Piece;
use Time::HiRes;
use Log::Minimal;

use UV;

use IO::Socket::INET;
use Data::MessagePack;
use JSON::XS;


1;

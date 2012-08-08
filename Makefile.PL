use inc::Module::Install;

name     'Fluent-Agent';
all_from 'lib/Fluent/Agent.pm';
license  'perl';

requires 'Data::MessagePack' => '0.35_01';
requires 'JSON::XS';
# requires 'UV';

requires 'Time::Piece';
requires 'Log::Minimal';

WriteAll;

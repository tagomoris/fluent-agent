use inc::Module::Install;

name     'Fluent-Agent';
all_from 'lib/Fluent/Agent.pm';
license  'perl';

requires 'JSON::XS';
requires 'Data::MessagePack';
requires 'Data::MessagePack::Stream';
# requires 'UV';

requires 'List::MoreUtils';
requires 'Time::Piece';
requires 'Log::Minimal';

WriteAll;

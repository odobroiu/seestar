%%%-------------------------------------------------------------------
%%% @author odobroiu
%%% @copyright (C) 2015, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 28. Jan 2015 11:23 PM
%%%-------------------------------------------------------------------
-author("odobroiu").

-record(host, {
    host :: binary(),
    port :: non_neg_integer()
}).
-record(peer, {
    hostname :: inet:hostname() | inet:ip_address()
}).
-record(keyspace, {
    name :: binary()
}).
-record(cluster_config, {
    control_host :: #host{},
    client_options :: proplists:proplist(),
    connect_options :: proplists:proplist(),
    other_config :: proplists:proplist()
}).

-record(metadata, {
    cluster_name :: binary(),
    peers = sets:new() :: set(), %% set of hosts
    keyspaces =[] :: [#keyspace{}], %% set
    partitioner :: binary()
}).
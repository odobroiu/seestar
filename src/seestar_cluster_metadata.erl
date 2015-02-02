%%%-------------------------------------------------------------------
%%% @author odobroiu
%%% @copyright (C) 2015, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 28. Jan 2015 11:22 PM
%%%-------------------------------------------------------------------
-module(seestar_cluster_metadata).
-author("odobroiu").

-include("seestar_cluster_metadata.hrl").
%% API
-export([cluster_name/1, partitioner/1, hosts/1]).

cluster_name(#metadata{cluster_name = ClusterName}) ->
    ClusterName.

partitioner(#metadata{partitioner = Partitioner}) ->
    Partitioner.

hosts(#metadata{peers = Peers}) ->
    Peers.

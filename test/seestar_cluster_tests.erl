-module(seestar_cluster_tests).
-author("odobroiu").

-include_lib("eunit/include/eunit.hrl").
-include_lib("seestar/include/constants.hrl").
%% API
-export([]).

ssl_test_() ->
    {foreach,
        fun() ->
            seestar_ccm:create(2),
            seestar_ccm:configure_ssl(),
            seestar_ccm:start(),
            timer:sleep(500),
            %% Eunit does not start the app, so ssl does not get started
            {ok, _Apps} = application:ensure_all_started(ssl),
            {ok, ClusterPid} = seestar_cluster:new(test_cluster, [
                {control_host, {{127,0,0,1}, 9042} },
                {connect_options, [
                    {ssl, []}
                ]}
            ]),
            ClusterPid
        end,
        fun(ClusterPid) ->
            seestar_cluster:stop(ClusterPid),
            seestar_ccm:remove()
        end,
        [
            fun(Pid) -> {with, Pid, [fun regular_node_down/1]} end,
            fun(Pid) -> {with, Pid, [fun control_node_down/1]} end
        ]}.

cluster_test_() ->
    {foreach,
        fun() ->
            seestar_ccm:create(2),
            seestar_ccm:start(),
            timer:sleep(500),
            {ok, ClusterPid} = seestar_cluster:new(test_cluster, [
                {control_host, {{127,0,0,1}, 9042} }
            ]),
            ClusterPid
        end,
        fun(ClusterPid) ->
            seestar_cluster:stop(ClusterPid),
            seestar_ccm:remove()
        end,
        [
            fun(Pid) -> {with, Pid, [fun regular_node_down/1]} end,
            fun(Pid) -> {with, Pid, [fun control_node_down/1]} end
        ]}.

regular_node_down(Cluster) ->
    Metadata = seestar_cluster:get_metadata(Cluster),
    ?assertEqual(<<"seestar_unit_test_cluster">>, seestar_cluster_metadata:cluster_name(Metadata)),
    ?assertEqual(<<"org.apache.cassandra.dht.Murmur3Partitioner">>, seestar_cluster_metadata:partitioner(Metadata)),
    ?assertEqual(2, sets:size(seestar_cluster_metadata:hosts(Metadata))),
    {NodesUp0, NodesDown0} = seestar_cluster:get_nodes(Cluster),
    ?assertEqual(2, sets:size(NodesUp0)),
    ?assertEqual(0, sets:size(NodesDown0)),
    seestar_ccm:stop_node("node2"),
    timer:sleep(500),
    {NodesUp, NodesDown} = seestar_cluster:get_nodes(Cluster),
    ?assertEqual(1, sets:size(NodesUp)),
    ?assertEqual(1, sets:size(NodesDown)).

control_node_down(ClusterPid) ->
    Metadata = seestar_cluster:get_metadata(ClusterPid),
    ?assertEqual(<<"seestar_unit_test_cluster">>, seestar_cluster_metadata:cluster_name(Metadata)),
    ?assertEqual(<<"org.apache.cassandra.dht.Murmur3Partitioner">>, seestar_cluster_metadata:partitioner(Metadata)),
    ?assertEqual(2, sets:size(seestar_cluster_metadata:hosts(Metadata))),
    seestar_ccm:stop_node("node1"),
    timer:sleep(500),
    {NodesUp, NodesDown} = seestar_cluster:get_nodes(ClusterPid),
    ?assertEqual(1, sets:size(NodesUp)),
    ?assertEqual(1, sets:size(NodesDown)).
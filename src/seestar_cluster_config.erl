%%%-------------------------------------------------------------------
%%% @author odobroiu
%%% @copyright (C) 2015, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 29. Jan 2015 12:08 AM
%%%-------------------------------------------------------------------
-module(seestar_cluster_config).
-author("odobroiu").

-include("seestar_cluster_metadata.hrl").

%% API
-export([control_connection_host/1]).

control_connection_host(#cluster_config{control_host = Host}) ->
    Host.


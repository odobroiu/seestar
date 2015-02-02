%%%-------------------------------------------------------------------
%%% @author odobroiu
%%% @copyright (C) 2015, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 28. Jan 2015 9:38 PM
%%%-------------------------------------------------------------------
-module(seestar_cluster).
-author("odobroiu").

-include("seestar_cluster_metadata.hrl").

-behaviour(gen_server).

%% API
-export([
    new/2,
    stop/1,
    get_metadata/1,
    get_nodes/1]).

%%
-export([
    update_local/1,
    update_peers/1,
    node_update/4
]).

-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    code_change/3,
    terminate/2
]).

-record(cluster_state, {
    cluster_name :: atom(),
    cluster_conf :: #cluster_config{},
    control_connection :: pid(),
    cluster_metadata = #metadata{} :: #metadata{},
    update_meta_local = undefined :: any(),
    update_meta_peers = undefined :: any(),
    nodes_up = sets:new() :: set(),
    nodes_down = sets:new() :: set(),
    event_listener :: pid()
}).

%% -------------------------------------------------------------------------
%% API
%% -------------------------------------------------------------------------

new(ClusterName, Config) ->
    gen_server:start_link(?MODULE, [ClusterName, Config], []).

get_metadata(ClusterName)->
    gen_server:call(ClusterName, get_metadata).

update_local(ClusterName) ->
    gen_server:cast(ClusterName, update_metadata_local).
update_peers(ClusterName) ->
    gen_server:cast(ClusterName, update_metadata_peers).

get_nodes(ClusterName) ->
    gen_server:call(ClusterName, get_nodes).

%% @private
node_update(ClusterName, Hostname, Port, down) ->
    gen_server:cast(ClusterName, {nodedown, #host{host = Hostname, port = Port}});
node_update(ClusterName, Hostname, Port, up) ->
    gen_server:cast(ClusterName, {nodeup, #host{host = Hostname, port = Port}}).

%% @doc Stop the cluster.
-spec stop(pid()) -> ok.
stop(ClusterName) ->
    gen_server:cast(ClusterName, stop).

%% -------------------------------------------------------------------------
%% GenServer behavior
%% -------------------------------------------------------------------------

%% @private
init([ClusterName, ClusterConfiguration]) ->
    process_flag(trap_exit, true),
    #cluster_config{control_host = ControllerHost} = Config = parse_config(ClusterConfiguration),
    {ok, EventListener} = gen_event:start_link(),
    gen_event:add_handler(EventListener, seestar_cluster_listener, [self()]),
    {ok, ControllerConnection} = create_controller_connection(ControllerHost, EventListener, Config),
    State0 = #cluster_state{cluster_name = ClusterName,
                            cluster_conf = Config,
                            control_connection = ControllerConnection,
                            event_listener = EventListener},
    State1 = refresh_metadata_sync(ControllerConnection, State0),
    Peers = State1#cluster_state.cluster_metadata#metadata.peers,
    {NodesUp, NodesDown} = check_all_nodes(sets:to_list(Peers), Config),
    State = State1#cluster_state{nodes_up = NodesUp, nodes_down = NodesDown},
    {ok, State}.

handle_call(get_nodes, _From, #cluster_state{nodes_up = NodesUp, nodes_down = NodesDown} = State) ->
    {reply, {NodesUp, NodesDown}, State};
handle_call(get_metadata, _From, #cluster_state{cluster_metadata = Metadata} = State) ->
    {reply, Metadata, State}.

handle_cast(update_metadata_local, #cluster_state{control_connection = ControlPid} = State) ->
    LocalStatement = seestar_statement:new("SELECT * FROM system.local WHERE key='local'", one),
    LocalRef = seestar_session:execute_async(ControlPid, LocalStatement),
    {noreply, State#cluster_state{update_meta_local = LocalRef}};
handle_cast(update_metadata_peers, #cluster_state{control_connection = ControlPid, update_meta_peers = undefined} = State) ->
    PeerStatement = seestar_statement:new("SELECT * FROM system.peers", one),
    PeerRef = seestar_session:execute_async(ControlPid, PeerStatement),
    {noreply, State#cluster_state{update_meta_peers = PeerRef}};
handle_cast({nodeup, Host}, #cluster_state{nodes_down = NodesDown0, nodes_up = NodesUp0} = State) ->
    NodesDown = sets:del_element(Host, NodesDown0),
    NodesUp = sets:add_element(Host, NodesUp0),
    {noreply, State#cluster_state{nodes_up = NodesUp, nodes_down = NodesDown}};
handle_cast({nodedown, Host}, #cluster_state{nodes_down = NodesDown0, nodes_up = NodesUp0} = State) ->
    NodesDown = sets:add_element(Host, NodesDown0),
    NodesUp = sets:del_element(Host, NodesUp0),
    {noreply, State#cluster_state{nodes_up = NodesUp, nodes_down = NodesDown}};
handle_cast(stop, State) ->
    {stop, normal, State};
handle_cast(Request, State) ->
    {stop, {unexpected_cast, Request}, State}.

handle_info({seestar_response, QueryRef, ResponseFun}, #cluster_state{update_meta_local = QueryRef} =State) ->
    NewState = update_system_local(State, ResponseFun()),
    {noreply, NewState#cluster_state{update_meta_local = undefined}};
handle_info({seestar_response, QueryRef, ResponseFun}, #cluster_state{update_meta_peers = QueryRef} =State) ->
    NewState = update_system_peers(State, ResponseFun()),
    {noreply, NewState#cluster_state{update_meta_peers = undefined}};
handle_info({seestar_response, _QueryRef, _ResponseFun}, State) ->
    %% Probably 2 metadata events were sent, the second one being sent before the result for the first one
    %% ; we only care about the last...
    {noreply, State};
handle_info({'EXIT',ControlPid, _Reason},
    #cluster_state{control_connection = ControlPid, cluster_conf = ClusterConfig }=State) ->
    EventListener = State#cluster_state.event_listener,
    Metadata = State#cluster_state.cluster_metadata,
    ControllerHost = ClusterConfig#cluster_config.control_host,
    PeersList = sets:to_list(sets:del_element(ControllerHost, Metadata#metadata.peers)),
    List = [ControllerHost | PeersList],
    case try_control_host(List, EventListener, ClusterConfig) of
        {ok, NewControlPid} ->
            {NodesUp, NodesDown} = check_all_nodes(PeersList, ClusterConfig),
            {noreply, State#cluster_state{control_connection = NewControlPid, nodes_down = NodesDown, nodes_up = NodesUp}};
        {error, no_hosts} ->
            {stop, no_hosts, State}
    end;

handle_info({'EXIT', _AControlPid, {connection_error,_Reason}}, State) ->
    %% A control connection could not connect
    %% We should not care about this here, as this is handled when creating the connection
    {noreply, State}.

%% @private
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% @private
terminate(_Reason, #cluster_state{control_connection = ControlConnection, event_listener = EventListener}) ->
    seestar_session:stop(ControlConnection),
    gen_event:stop(EventListener),
    ok.


%% -------------------------------------------------------------------------
%% Internal Methods
%% -------------------------------------------------------------------------
parse_config(ClusterConfiguration) ->
    {ControlHost, OtherOptions0} = control_host(ClusterConfiguration),
    {ClientOptions, OtherOptions1} = client_options(OtherOptions0),
    {ConnectOptions, OtherOptions} = connect_options(OtherOptions1),
    #cluster_config{control_host = ControlHost,
                    client_options = ClientOptions,
                    connect_options = ConnectOptions,
                    other_config = OtherOptions}.

control_host(ClusterConfiguration) ->
    {Host, Port} = proplists:get_value(control_host, ClusterConfiguration),
    ControlHost = #host{host = Host, port = Port},
    OtherOptions = proplists:delete(control_host, ClusterConfiguration),
    {ControlHost, OtherOptions}.

client_options(ClusterConfiguration) ->
    ClientOptions = proplists:get_value(client_options, ClusterConfiguration, []),
    OtherOptions = proplists:delete(client_options, ClusterConfiguration),
    {ClientOptions, OtherOptions}.

connect_options(ClusterConfiguration) ->
    ConnectOptions = proplists:get_value(connect_options, ClusterConfiguration, []),
    OtherOptions = proplists:delete(connect_options, ClusterConfiguration),
    {ConnectOptions, OtherOptions}.

create_controller_connection(#host{host = Host, port = Port}, EventListener, ClusterConfig) ->
    ControlParameters = [
        {events, [topology_change, status_change, schema_change]},
        {event_listener, EventListener}
    ],
    ClientOptions = ControlParameters ++ ClusterConfig#cluster_config.client_options,
    ConnectOptions = ClusterConfig#cluster_config.connect_options,
    seestar_session:start_link(Host, Port, ClientOptions, ConnectOptions).

check_all_nodes(Peers, ClusterConfig) ->
    check_all_nodes(Peers, sets:new(), sets:new(), ClusterConfig).

check_all_nodes([], NodesUp, NodesDown, _ClusterConfig)->
    {NodesUp, NodesDown};
check_all_nodes([Host | Rest], NodesUp, NodesDown , ClusterConfig)->
    case is_host_available(Host, ClusterConfig) of
        true ->
            check_all_nodes(Rest, sets:add_element(Host, NodesUp), NodesDown, ClusterConfig);
        false ->
            check_all_nodes(Rest, NodesUp, sets:add_element(Host, NodesDown), ClusterConfig)
    end.

is_host_available(#host{host = Hostname, port = Port}, #cluster_config{client_options = ClientOpts, connect_options = ConnectOpts}) ->
    case seestar_session:start_link(Hostname, Port, ClientOpts, ConnectOpts) of
        {ok, Pid} ->
            unlink(Pid),
            seestar_session:stop(Pid),
            true;
        _Error ->
            false
    end.


refresh_metadata_sync(ControlPid, State0) ->
    SelectLocalStatement = seestar_statement:new("SELECT * FROM system.local WHERE key='local'", one),
    Result0 = seestar_session:execute(ControlPid, SelectLocalStatement),
    State1 = update_system_local(State0, Result0),
    SelectPeersStatement = seestar_statement:new("SELECT * FROM system.peers", one),
    Result1 = seestar_session:execute(ControlPid, SelectPeersStatement),
    update_system_peers(State1, Result1).

update_system_local(#cluster_state{cluster_metadata = Metadata0, cluster_conf = Config} = State, {ok, Result}) ->
    Names = seestar_result:names(Result),
    [Row] = seestar_result:rows(Result),
    Values = lists:zip(Names, Row),
    ClusterName = proplists:get_value(<<"cluster_name">>, Values),
    Partitioner = proplists:get_value(<<"partitioner">>, Values),
    NewPeer = seestar_cluster_config:control_connection_host(Config),
    Peers = update_peer(Metadata0#metadata.peers, NewPeer),
    Metadata = Metadata0#metadata{cluster_name = ClusterName, partitioner = Partitioner, peers = Peers},
    State#cluster_state{cluster_metadata =  Metadata}.

update_system_peers(#cluster_state{cluster_metadata = Metadata0, cluster_conf = Config} = State, {ok, Result}) ->
    Names = seestar_result:names(Result),
    Rows = seestar_result:rows(Result),
    NewPeers = lists:map(
        fun(Row) ->
            Values = lists:zip(Names, Row),
            %% I guess it's ok, the java driver does something similar TODO -> ???
            ControlConnectionHost = seestar_cluster_config:control_connection_host(Config),
            Port = ControlConnectionHost#host.port,
            #host{host = proplists:get_value(<<"peer">>, Values), port = Port}
        end, Rows),
    Peers = update_peers(Metadata0#metadata.peers, NewPeers),
    Metadata = Metadata0#metadata{peers = Peers},
    State#cluster_state{cluster_metadata =  Metadata}.

update_peer(Peers, NewPeer) ->
    sets:add_element(NewPeer, Peers).

update_peers(Peers, NewPeers) ->
    lists:foldl(
        fun(Peer, CurrentList) ->
            update_peer(CurrentList, Peer)
        end, Peers, NewPeers).

try_control_host([], _EventListener, _ClusterConfig) ->
    {error, no_hosts};
try_control_host([Host | Rest], EventListener, ClusterConfig) ->
    case create_controller_connection(Host, EventListener, ClusterConfig) of
        {ok, NewControlPid} ->
            {ok, NewControlPid};
        {error, _Reason} ->
            try_control_host(Rest, EventListener, ClusterConfig)
    end.
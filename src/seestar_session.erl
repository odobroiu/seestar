%%% Copyright 2014 Aleksey Yeschenko
%%%
%%% Licensed under the Apache License, Version 2.0 (the "License");
%%% you may not use this file except in compliance with the License.
%%% You may obtain a copy of the License at
%%%
%%%     http://www.apache.org/licenses/LICENSE-2.0
%%%
%%% Unless required by applicable law or agreed to in writing, software
%%% distributed under the License is distributed on an "AS IS" BASIS,
%%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%%% See the License for the specific language governing permissions and
%%% limitations under the License.

-module(seestar_session).

-behaviour(gen_server).

-include("seestar_messages.hrl").
-include("builtin_types.hrl").
-include("seestar.hrl").
%% API exports.
-export([start_link/2, start_link/3, start_link/4, stop/1]).
-export([prepare/2, prepare_async/2]).
-export([execute/2, execute_async/2]).
-export([next_page/2, next_page_async/2]).

%% gen_server exports.
-export([init/1, terminate/2, handle_call/3, handle_cast/2, handle_info/2,
         code_change/3]).

-type credentials() :: [{string() | binary(), string() | binary()}].
-type events() :: [topology_change | status_change | schema_change].
-type client_option() :: {keyspace, string() | binary()}
                       | {credentials, credentials()}
                       | {events, events()}.

-type connect_option() :: gen_tcp:connect_option() | {connect_timeout, timeout()}
                         | {ssl, [ssl:connect_option()]}.

-type 'query'() :: binary() | string().

-define(b2l(Term), case is_binary(Term) of true -> binary_to_list(Term); false -> Term end).
-define(l2b(Term), case is_list(Term) of true -> list_to_binary(Term); false -> Term end).

-record(req,
        {op :: seestar_frame:opcode(),
         body :: binary(),
         from :: {pid(), reference()},
         sync = true :: boolean(),
         result_data = undefined :: any(),
         decode_data = undefined :: any()}).

-record(st,
        {host :: inet:hostname(),
         transport :: tcp | ssl,
         sock :: inet:socket() | ssl:sslsocket(),
         buffer :: seestar_buffer:buffer(),
         free_ids :: [seestar_frame:stream_id()],
         backlog = queue:new() :: queue_t(),
         reqs :: ets:tid(),
         event_listener :: pid()}).

%% -------------------------------------------------------------------------
%% API
%% -------------------------------------------------------------------------

%% @equiv start_link(Host, Post, [])
-spec start_link(inet:hostname(), inet:port_number()) ->
    any().
start_link(Host, Port) ->
    start_link(Host, Port, []).

%% @equiv start_link(Host, Post, ClientOptions, [])
-spec start_link(inet:hostname(), inet:port_number(), [client_option()]) ->
    any().
start_link(Host, Port, ClientOptions) ->
    start_link(Host, Port, ClientOptions, []).

%% @doc
%% Starts a new connection to a cassandra node on Host:Port.
%% By default it will connect on plain tcp. If you want to connect using ssl, pass
%% {ssl, [ssl_option()]} in the ConnectOptions
%% @end
-spec start_link(inet:hostname(), inet:port_number(), [client_option()], [connect_option()]) ->
    {ok, pid()} | {error, any()}.
start_link(Host, Port, ClientOptions, ConnectOptions) ->
     case gen_server:start_link(?MODULE, [Host, Port, ClientOptions, ConnectOptions], []) of
        {ok, Pid} ->
            case setup(Pid, ClientOptions) of
                ok    -> {ok, Pid};
                Error -> stop(Pid), Error
            end;
        Error ->
            Error
    end.

setup(Pid, Options) ->
    case authenticate(Pid, Options) of
        false ->
            {error, invalid_credentials};
        true ->
            case set_keyspace(Pid, Options) of
                false ->
                    {error, invalid_keyspace};
                true ->
                    case subscribe(Pid, Options) of
                        false -> {error, invalid_events};
                        true  -> ok
                    end
            end
    end.

authenticate(Pid, Options) ->
    Authentication = proplists:get_value(auth, Options),
    case request(Pid, #startup{}, true) of
        #ready{} ->
            true;
        #authenticate{} when Authentication =:= undefined ->
            false;
        #authenticate{} ->
            {AuthModule, AuthModuleParams} = Authentication,
            SendFunction =  fun(#auth_response{} = Request) ->
                                request(Pid, Request, true)
                            end,
            AuthModule:perform_auth(SendFunction, AuthModuleParams)
    end.

set_keyspace(Pid, Options) ->
    case proplists:get_value(keyspace, Options) of
        undefined ->
            true;
        Keyspace ->
            UseKeyspaceStatement = seestar_statement:new("USE " ++ ?b2l(Keyspace), one),
            case execute(Pid, UseKeyspaceStatement) of
                {ok, _Result}    -> true;
                {error, _Reason} -> false
            end
    end.

subscribe(Pid, Options) ->
    case proplists:get_value(events, Options, []) of
        [] ->
            true;
        Events ->
            case request(Pid, #register{event_types = Events}, true) of
                #ready{} -> true;
                #error{} -> false
            end
    end.

%% @doc Stop the client.
%% Closes the socket and terminates the process normally.
-spec stop(pid()) -> ok.
stop(Client) ->
    gen_server:cast(Client, stop).

%% @doc Prepare a query for later execution. The response will contain the prepared
%% query that you will need to pass to the execute methods
%% @see execute/3.
%% @see execute/4.
-spec prepare(pid(), 'query'()) ->
    {ok, Result :: seestar_result:prepared_result()} | {error, Error :: seestar_error:error()}.
prepare(Client, Query) ->
    Req = #prepare{'query' = ?l2b(Query)},
    wrap_response(Req, request(Client, Req, true)).

prepare_async(Client, Query) ->
    Req = #prepare{'query' = ?l2b(Query)},
    request(Client, Req, false).

execute(Client, #'query'{} = Query) ->
    wrap_response(Query, request(Client, Query, true));
execute(Client, #execute{params = #query_params{cached_result_meta = ResultMeta}} = Execute) ->

    wrap_response(Execute,  request(Client, Execute, undefined, ResultMeta, true));
execute(Client, Req) ->
    wrap_response(Req, request(Client, Req, true)).

execute_async(Client, #'query'{params = #query_params{page_size = PageSize}} = Query) ->
    if
        is_number(PageSize)->
            request(Client, Query, undefined, Query, false);
        true ->
            request(Client, Query, false)
    end;
execute_async(Client, #execute{params = #query_params{page_size = PageSize, cached_result_meta = CachedResultMeta}} = Query) ->
    if
        is_number(PageSize)->
            request(Client, Query, CachedResultMeta, Query, false);
        true ->
            request(Client, Query, undefiend, Query, false)
    end;
execute_async(Client, Req) ->
    request(Client, Req, false).

%% @doc Synchronously returns the next page for a previous paginated result
next_page(Client, #rows{initial_query = Req0, metadata = #metadata{paging_state = PagingState}}) ->
    {Req, CachedDecodeData} = next_page_request(Req0, PagingState),
    wrap_response(Req, request(Client, Req, undefined, CachedDecodeData, true)).

%% @doc Asynchronously returns the next page for a previous paginated result
next_page_async(Client, #rows{initial_query = Req0, metadata = #metadata{paging_state = PagingState}}) ->
    {Req, CachedDecodeData} = next_page_request(Req0, PagingState),
    request(Client, Req, CachedDecodeData, Req, false).
%% -------------------------------------------------------------------------
%% gen_server callback functions
%% -------------------------------------------------------------------------

%% @private
init([Host, Port, ClientOptions0, ConnectOptions0]) ->
    {Timeout, ConnectOptions1} = get_timeout(ConnectOptions0),
    {Transport , ConnectOptions} = get_transport(ConnectOptions1),
    {Pid, _ClientOptions} = get_event_listener(ClientOptions0),
    case create_socket(Host, Port, Transport, ConnectOptions, Timeout) of
        {ok, Sock} ->
            ok = socket_setopts(Sock, Transport, [binary, {packet, 0}, {active, true}]),
            {ok, #st{host = Host, sock = Sock, transport = Transport, buffer = seestar_buffer:new(),
                free_ids = lists:seq(0, 127), reqs = ets:new(seestar_reqs, []), event_listener = Pid}};
        {error, Reason} ->
            {stop, {connection_error, Reason}}
    end.

%% @doc
%% Extracts the protocol from the connect options. Returns a tuple where the first element
%% is the transport, and the second is a proplist of the remaining options
-spec get_transport([connect_option()]) -> {ssl|tcp, [connect_option()]}.
get_transport(ConnectOptions0) ->
    case proplists:get_value(ssl, ConnectOptions0) of
        undefined ->
            {tcp, ConnectOptions0};
        SslOptions ->
            NewConnectOptions = proplists:delete(ssl, ConnectOptions0) ++ SslOptions,
            {ssl, NewConnectOptions}
    end.

%% @doc
%% Extracts the timeout from the conenct options. Returns a tuple with the first element
%% being the timeout, and the second a proplist of the remaining options
-spec get_timeout([connect_option()]) -> {ssl|tcp, [connect_option()]}.
get_timeout(ConnectOptions0) ->
    Timeout = proplists:get_value(connect_timeout, ConnectOptions0, infinity),
    NewConnectOptions = proplists:delete(connect_timeout, ConnectOptions0),
    {Timeout, NewConnectOptions}.

get_event_listener(ClientOptions) ->
    EventListener = proplists:get_value(event_listener, ClientOptions, undefined),
    NewClientOptions = proplists:delete(event_listener, ClientOptions),
    {EventListener, NewClientOptions}.

create_socket(Host, Port, ssl, SockOpts, Timeout) ->
    ssl:connect(Host, Port, SockOpts, Timeout);
create_socket(Host, Port, tcp, SockOpts, Timeout) ->
    gen_tcp:connect(Host, Port, SockOpts, Timeout).

socket_setopts(Sock, tcp, Options)->
    inet:setopts(Sock, Options);
socket_setopts(Sock, ssl, Options)->
    ssl:setopts(Sock, Options).

%% @private
terminate(_Reason, _St) ->
    ok.

%% @private
handle_call({request, Op, Body, Sync, ResultData, DecodeData}, From, #st{free_ids = []} = St) ->
    Req = #req{op = Op, body = Body, from = From, sync = Sync, result_data = ResultData, decode_data = DecodeData},
    {noreply, St#st{backlog = queue:in(Req, St#st.backlog)}};

handle_call({request, Op, Body, Sync, ResultData, DecodeData}, {_Pid, Ref} = From, St) ->
    case Sync of
        true  -> ok;
        false -> gen_server:reply(From, Ref)
    end,
    case send_request(
        #req{op = Op, body = Body, from = From, sync = Sync, result_data = ResultData, decode_data = DecodeData}, St) of
        {ok, St1}       -> {noreply, St1};
        {error, Reason} -> {stop, {socket_error, Reason}, St}
    end;

handle_call(Request, _From, St) ->
    {stop, {unexpected_call, Request}, St}.

send_request(
            #req{op = Op, body = Body, from = From, sync = Sync, result_data = ResultData, decode_data = DecodeData},
            #st{sock = Sock, transport = Transport} = St) ->
    ID = hd(St#st.free_ids),
    Frame = seestar_frame:new(ID, [], Op, Body),
    case send_on_wire(Sock, Transport, seestar_frame:encode(Frame)) of
        ok ->
            ets:insert(St#st.reqs, {ID, From, Sync, ResultData, DecodeData}),
            {ok, St#st{free_ids = tl(St#st.free_ids)}};
        {error, _Reason} = Error ->
            Error
    end.

send_on_wire(Sock, tcp, Data) ->
    gen_tcp:send(Sock, Data);
send_on_wire(Sock, ssl, Data) ->
    ssl:send(Sock, Data).

%% @private
handle_cast(stop, #st{sock = Sock, transport = tcp} = St) ->
    gen_tcp:close(Sock),
    {stop, normal, St};

handle_cast(stop, #st{sock = Sock, transport = ssl} = St) ->
    ssl:close(Sock),
    {stop, normal, St};

handle_cast(Request, St) ->
    {stop, {unexpected_cast, Request}, St}.

%% @private
handle_info({Transport, Sock, Data}, #st{sock = Sock, transport = Transport} = St) ->
    {Frames, Buffer} = seestar_buffer:decode(St#st.buffer, Data),
    {noreply, process_frames(Frames, St#st{buffer = Buffer})};

handle_info({tcp_closed, Sock}, #st{sock = Sock, transport = tcp} = St) ->
    {stop, socket_closed, St};

handle_info({tcp_error, Sock, Reason}, #st{sock = Sock, transport = tcp} = St) ->
    {stop, {socket_error, Reason}, St};

handle_info({ssl_closed, Sock}, #st{sock = Sock, transport = ssl} = St) ->
    {stop, socket_closed, St};

handle_info({ssl_error, Sock, Reason}, #st{sock = Sock, transport = ssl} = St) ->
    {stop, {socket_error, Reason}, St};

handle_info(Info, St) ->
    {stop, {unexpected_info, Info}, St}.

%% -------------------------------------------------------------------------
%% Internal
%% -------------------------------------------------------------------------

next_page_request(#'query'{} = Req0, PagingState) ->
    QueryParams = Req0#'query'.params#query_params{paging_state = PagingState},
    {Req0#query{params = QueryParams}, undefined};

next_page_request(#execute{} = Req0, PagingState) ->
    %% For paging execute queries, since we use skip_meta, we need to also return the
    %% ResultMetadata so that we can use it when decoding.
    QueryParams = Req0#execute.params#query_params{paging_state = PagingState},
    {Req0#execute{params = QueryParams}, QueryParams#query_params.cached_result_meta}.

-spec request(pid(), seestar_messages:outgoing(), boolean()) -> any() | seestar_messages:incoming().
request(Client, Request, Sync) ->
    request(Client, Request, undefined, undefined, Sync).

%% @private
%% @doc Sends the request to the process handling the connection. The 2 new parameters are
%% CachedDecodeData -> will be passed to the decode function. This is useful when a part
%% of the response from cassandra is known( eg skip_meta), and that part is needed for the decoding
%% CachedResultData -> will end up being passed to reply_async. This is currently used for keeping
%% the initial query and returning it to the caller in case of a paginated resultSet
-spec request(pid(), seestar_messages:outgoing(), any(), any(), boolean()) -> any() | seestar_messages:incoming().
request(Client, Request, CachedDecodeData, CachedResultData, Sync) ->
    {ReqOp, ReqBody} = seestar_messages:encode(Request),
    case gen_server:call(Client, {request, ReqOp, ReqBody, Sync, CachedResultData, CachedDecodeData}, infinity) of
        {RespOp, RespBody} ->
            seestar_messages:decode(RespOp, RespBody, CachedDecodeData);
        Ref ->
            Ref
    end.

wrap_response(Req, Respone) ->
    case Respone of
        #result{result = #rows{metadata = #metadata{has_more_results = true}} = Result} ->
            {ok, Result#rows{initial_query = Req}};
        #result{result = Result} ->
            {ok, Result};
        #error{} = Error ->
            {error, Error}
    end.

process_frames([Frame|Frames], St) ->
    process_frames(Frames,
                   case seestar_frame:id(Frame) of
                       -1 -> handle_event(Frame, St);
                       _  -> handle_response(Frame, St)
                   end);
process_frames([], St) ->
    process_backlog(St).

handle_event(Frame, #st{event_listener = Pid} = St) when is_pid(Pid) ->
    F = fun() ->
        Op = seestar_frame:opcode(Frame),
        Body = seestar_frame:body(Frame),
        seestar_messages:decode(Op, Body, undefined)
    end,
    gen_event:notify(Pid, F),
    St;
handle_event(_Frame, St)->
    St.

handle_response(Frame, St) ->
    ID = seestar_frame:id(Frame),
    [{ID, From, Sync, ResultData, DecodeData}] = ets:lookup(St#st.reqs, ID),
    ets:delete(St#st.reqs, ID),
    Op = seestar_frame:opcode(Frame),
    Body = seestar_frame:body(Frame),
    case Sync of
        true  -> gen_server:reply(From, {Op, Body});
        false -> reply_async(From, Op, Body, ResultData, DecodeData)
    end,
    St#st{free_ids = [ID|St#st.free_ids]}.

reply_async({Pid, Ref}, Op, Body, ResultMeta, DecodeMeta) ->
    F = fun() ->
            case seestar_messages:decode(Op, Body, DecodeMeta) of
                #result{result = #rows{} = Result} ->
                    {ok, Result#rows{initial_query = ResultMeta}};
                #result{result = Result} ->
                    {ok, Result};
                #error{} = Error ->
                    {error, Error}
            end
        end,
    Pid ! {seestar_response, Ref, F}.

process_backlog(#st{backlog = Backlog, free_ids = FreeIDs} = St) ->
    case queue:is_empty(Backlog) orelse FreeIDs =:= [] of
        true ->
            St;
        false ->
            {{value, Req}, Backlog1} = queue:out(Backlog),
            #req{from = {_Pid, Ref} = From, sync = Sync} = Req,
            case Sync of
                true  -> ok;
                false -> gen_server:reply(From, Ref)
            end,
            case send_request(Req, St#st{backlog = Backlog1}) of
                {ok, St1}       -> process_backlog(St1);
                {error, Reason} -> {stop, {socket_error, Reason}, St}
            end
    end.

%% @private
code_change(_OldVsn, St, _Extra) ->
    {ok, St}.

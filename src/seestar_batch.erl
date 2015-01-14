-module(seestar_batch).

%% API
-export([prepared_query/2, normal_query/2, batch_request/3]).

-include("seestar.hrl").
-include("seestar_messages.hrl").

-type batch_query() :: #batch_query{}.
-type batch_request() :: #batch{}.
-export_type([batch_query/0, batch_request/0]).

%% @doc Return a prepared query that can be added to batch request
%% @see batch_request/3.
-spec prepared_query(seestar_result:prepared_query(), [seestar_cqltypes:value()]) -> batch_query().
prepared_query(#prepared_query{id = ID, request_types = Types}, Values) ->
    #batch_query{kind = prepared, string_or_id = ID, values = #query_values{values = Values, types = Types}}.

%% @doc Return a normal query that can be added to batch request
%% @see batch_request/3.
-spec normal_query(binary(), list(seestar_cqltypes:value())) -> batch_query().
normal_query(Query, Values) ->
    #batch_query{kind = not_prepared, string_or_id = Query, values = #query_values{values = Values}}.

%% @doc Return a batch request that can be sent to cassandra
%% Use {@link seestar_session} module to send the request
%% @see seestar_session:batch/2.
%% @see seestar_session:batch_async/2.
-spec batch_request(logged | unlogged | counter, one | atom(), list(batch_query())) -> batch_request().
batch_request(Type, Consistency, Queries) ->
    #batch{type = Type, consistency = Consistency, queries = Queries}.

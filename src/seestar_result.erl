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

-module(seestar_result).

-include("seestar.hrl").
-include("seestar_messages.hrl").

-export([type/1, rows/1, names/1, types/1, type/2, keyspace/1, table/1, prepared_query/1,
    change/1, has_more_rows/1]).

-type rows_result() :: #rows{}.
-type set_keyspace_result() :: #set_keyspace{}.
-type prepared_result() :: #prepared{}.
-type schema_change_result() :: #schema_change{}.
-type prepared_query() :: #prepared_query{}.
-opaque result() :: void
                | rows_result()
                | set_keyspace_result()
                | prepared_result()
                | schema_change_result().
-export_type([result/0, prepared_result/0, prepared_query/0]).

-type type() :: void | rows | set_keyspace | prepared | schema_change.
-type change() :: created | updated | dropped.

%% -------------------------------------------------------------------------
%% API
%% -------------------------------------------------------------------------

-spec type(Result :: result()) -> type().
type(void) ->
    void;
type(#rows{}) ->
    rows;
type(#set_keyspace{}) ->
    set_keyspace;
type(#prepared{}) ->
    prepared;
type(#schema_change{}) ->
    schema_change.

-spec rows(Rows :: rows_result()) -> [[seestar_cqltypes:value()]].
rows(#rows{rows = Rows}) ->
    Rows.

-spec names(Result :: rows_result() | prepared_result()) -> [binary()].
names(#rows{metadata = #metadata{columns = Columns}}) ->
    [ C#column.name || C <- Columns ];
names(#prepared{request_metadata = #metadata{columns = Columns}}) ->
    [ C#column.name || C <- Columns ].

-spec types(Result :: rows_result() | prepared_result()) -> [seestar_cqltypes:type()].
types(#rows{metadata = #metadata{columns = Columns}}) ->
    [ C#column.type || C <- Columns ];
types(#prepared{request_metadata = #metadata{columns = Columns}}) ->
    [ C#column.type || C <- Columns ].

-spec type(Result :: rows_result() | prepared_result(), Name :: binary()) -> seestar_cqltypes:type().
type(#rows{metadata = #metadata{columns = Columns}}, Name) ->
    hd([ C#column.type || C <- Columns, C#column.name =:= Name ]);
type(#prepared{request_metadata = #metadata{columns = Columns}}, Name) ->
    hd([ C#column.type || C <- Columns, C#column.name =:= Name ]).

-spec keyspace(Result :: set_keyspace_result() | schema_change_result()) -> binary().
keyspace(#set_keyspace{keyspace = Keyspace}) ->
    Keyspace;
keyspace(#schema_change{keyspace = Keyspace}) ->
    Keyspace.

-spec table(Result :: schema_change_result()) -> binary() | undefined.
table(#schema_change{table = Table}) ->
    Table.

%% @doc Returns a prepared query from the result. The returned query can be passed to
%% the {@link seestar_session:execute} or {@link seestar_session:execute_asyn} functions,
%% or can be used a part of a batch query using the {@link seestar_batch} module.
-spec prepared_query(Result :: prepared_result()) -> prepared_query().
prepared_query(#prepared{id = ID, result_metadata = ResultMetadata} = Result) ->
    #prepared_query{id = ID, cached_result_meta = ResultMetadata, request_types = types(Result)}.

-spec change(Result :: schema_change_result()) -> change().
change(#schema_change{change = Change}) ->
    Change.

%% @doc Returns true if the current result is paginated and not all records have been
%% retrieved so far
-spec has_more_rows(Result :: rows_result()) -> boolean().
has_more_rows(#rows{metadata = #metadata{has_more_results = HasMoreRows}}) ->
    HasMoreRows.
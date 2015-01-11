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

%%% @private
-module(seestar_messages).

-export([encode/1, decode/2]).

-include("constants.hrl").
-include("seestar_messages.hrl").

-type outgoing() :: #startup{}
                  | #auth_response{}
                  | #options{}
                  | #'query'{}
                  | #prepare{}
                  | #execute{}
                  | #register{}.

-type incoming() :: #error{}
                  | #ready{}
                  | #authenticate{}
                  | #supported{}
                  | #result{}
                  | #event{}
                  | #auth_success{}
                  | #auth_challenge{}.

-define(VERSION, <<"CQL_VERSION">>).
-define(COMPRESSION, <<"COMPRESSION">>).

%% -------------------------------------------------------------------------
%% encoding functions
%% -------------------------------------------------------------------------

-spec encode(outgoing()) -> {seestar_frame:opcode(), binary()}.
encode(#startup{version = Version, compression = Compression}) ->
    KVPairs =
        case Compression of
            undefined ->
                [{?VERSION, Version}];
            Value when is_binary(Value) ->
                [{?VERSION, Version}, {?COMPRESSION, Value}]
        end,
    {?STARTUP, seestar_types:encode_string_map(KVPairs)};

encode(#auth_response{body = Body}) ->
    {?AUTH_RESPONSE, Body};

encode(#options{}) ->
    {?OPTIONS, <<>>};

encode(#'query'{'query' = Query, params = QueryParams}) ->
    {?QUERY, <<
                (seestar_types:encode_long_string(Query))/binary,
                (encode_query_flags(QueryParams))/binary
             >>};

encode(#prepare{'query' = Query}) ->
    {?PREPARE, seestar_types:encode_long_string(Query)};

encode(#execute{id = ID, params = QueryParams}) ->
    {?EXECUTE, <<
        (seestar_types:encode_short_bytes(ID))/binary,
        (encode_query_flags(QueryParams))/binary
        >>};

encode(#register{event_types = Types}) ->
    % assert validity of event types.
    Unique = lists:usort(Types),
    [] = Unique -- [topology_change, status_change, schema_change],
    Encoded = [ list_to_binary(string:to_upper(atom_to_list(Type))) || Type <- Types ],
    {?REGISTER, seestar_types:encode_string_list(Encoded)}.

%% -------------------------------------------------------------------------
%% decoding functions
%% -------------------------------------------------------------------------

-spec decode(seestar_frame:opcode(), binary()) -> incoming().
decode(?ERROR, Body) ->
    {Code, Rest0} = seestar_types:decode_int(Body),
    {Message, Rest1} = seestar_types:decode_string(Rest0),
    #error{code = Code,
           message = Message,
           details = case Code of
                         ?UNAVAILABLE    -> decode_unavailable(Rest1);
                         ?WRITE_TIMEOUT  -> decode_write_timeout(Rest1);
                         ?READ_TIMEOUT   -> decode_read_timeout(Rest1);
                         ?ALREADY_EXISTS -> decode_already_exists(Rest1);
                         ?UNPREPARED     -> decode_unprepared(Rest1);
                         _               -> undefined
                     end};

decode(?READY, _Body) ->
    #ready{};

decode(?AUTHENTICATE, Body) ->
    {Class, _} = seestar_types:decode_string(Body),
    #authenticate{class = Class};

decode(?SUPPORTED, Body) ->
    {KVPairs, _} = seestar_types:decode_string_multimap(Body),
    #supported{versions = proplists:get_value(?VERSION, KVPairs),
               compression = proplists:get_value(?COMPRESSION, KVPairs)};

decode(?RESULT, Body) ->
    {Kind, Rest} = seestar_types:decode_int(Body),
    #result{result = case Kind of
                         16#01 -> void;
                         16#02 -> decode_rows(Rest);
                         16#03 -> decode_set_keyspace(Rest);
                         16#04 -> decode_prepared(Rest);
                         16#05 -> decode_schema_change(Rest)
                     end};
decode(?AUTH_SUCCESS, _Body) ->
    #auth_success{};
decode(?AUTH_CHALLENGE, Body) ->
    #auth_challenge{body = Body}.

%% -------------------------------------------------------------------------
%% error details
%% -------------------------------------------------------------------------

decode_unavailable(Data) ->
    {Consistency, Rest0} = seestar_types:decode_consistency(Data),
    {Required, Rest1} = seestar_types:decode_int(Rest0),
    {Alive, _} = seestar_types:decode_int(Rest1),
    #unavailable{consistency = Consistency, required = Required, alive = Alive}.

decode_write_timeout(Data) ->
    {Consistency, Rest0} = seestar_types:decode_consistency(Data),
    {Received, Rest1} = seestar_types:decode_int(Rest0),
    {Required, Rest2} = seestar_types:decode_int(Rest1),
    {WriteType, _} = seestar_types:decode_string(Rest2),
    #write_timeout{consistency = Consistency,
                   received = Received,
                   required = Required,
                   write_type = list_to_atom(string:to_lower(binary_to_list(WriteType)))}.

decode_read_timeout(Data) ->
    {Consistency, Rest0} = seestar_types:decode_consistency(Data),
    {Received, Rest1} = seestar_types:decode_int(Rest0),
    {Required, Rest2} = seestar_types:decode_int(Rest1),
    <<DataPresent, _/binary>> = Rest2,
    #read_timeout{consistency = Consistency,
                  received = Received,
                  required = Required,
                  data_present = DataPresent =/= 0}.

decode_already_exists(Data) ->
    {{Keyspace, Table}, _} = decode_table_spec(Data),
    #already_exists{keyspace = Keyspace,
                    table = case Table of
                                 <<>> -> undefined;
                                 _    -> Table
                            end}.

decode_unprepared(Data) ->
    {ID, _} = seestar_types:decode_short_bytes(Data),
    #unprepared{id = ID}.

%% -------------------------------------------------------------------------
%% different result types
%% -------------------------------------------------------------------------

decode_rows(Body) ->
    {Meta, Rest0} = decode_metadata(Body),
    {Count, Rest1} = seestar_types:decode_int(Rest0),
    #rows{metadata = Meta, rows = decode_rows(Meta, Rest1, Count)}.

decode_rows(Meta, Data, Count) ->
    decode_rows(Meta, Data, Count, []).

decode_rows(_, _, 0, Acc) ->
    lists:reverse(Acc);
decode_rows(Meta, Data, Count, Acc) ->
    {Row, Rest} = decode_row(Meta, Data),
    decode_rows(Meta, Rest, Count - 1, [Row|Acc]).

decode_row(Meta, Data) ->
    decode_row(Meta, Data, []).

decode_row([], Data, Row) ->
    {lists:reverse(Row), Data};
decode_row([#column{type = Type}|Meta], Data, Row) ->
    {Value, Rest} = seestar_cqltypes:decode_value_with_size(Type, Data),
    decode_row(Meta, Rest, [Value|Row]).

decode_metadata(Data) ->
    {Flags, Rest0} = seestar_types:decode_int(Data),
    {Count, Rest1} = seestar_types:decode_int(Rest0),
    {TableSpec, Rest2} = case Flags of
                             16#00 -> {undefined, Rest1};
                             16#01 -> decode_table_spec(Rest1)
                         end,
    decode_column_specs(TableSpec, Rest2, Count).

decode_column_specs(TableSpec, Data, Count) ->
    decode_column_specs(TableSpec, Data, Count, []).

decode_column_specs(_, Data, 0, Meta) ->
    {lists:reverse(Meta), Data};
decode_column_specs(TableSpec, Data, Count, Meta) ->
    {Column, Rest} = decode_column_spec(TableSpec, Data),
    decode_column_specs(TableSpec, Rest, Count - 1, [Column|Meta]).

decode_column_spec(undefined, Data) ->
    {TableSpec, Rest} = decode_table_spec(Data),
    decode_column_spec(TableSpec, Rest);
decode_column_spec({Keyspace, Table}, Data) ->
    {Name, Rest0} = seestar_types:decode_string(Data),
    {Type, Rest1} = seestar_cqltypes:decode_type(Rest0),
    {#column{keyspace = Keyspace, table = Table, name = Name, type = Type}, Rest1}.

decode_table_spec(Data) ->
    {Keyspace, Rest0} = seestar_types:decode_string(Data),
    {Table, Rest1} = seestar_types:decode_string(Rest0),
    {{Keyspace, Table}, Rest1}.

decode_set_keyspace(Body) ->
    {Keyspace, _} = seestar_types:decode_string(Body),
    #set_keyspace{keyspace = Keyspace}.

decode_prepared(Body) ->
    {ID, Rest} = seestar_types:decode_short_bytes(Body),
    {Meta, _} = decode_metadata(Rest),
    #prepared{id = ID, metadata = Meta}.

decode_schema_change(Body) ->
    {Change, Rest} = seestar_types:decode_string(Body),
    {{Keyspace, Table}, _} = decode_table_spec(Rest),
    #schema_change{change = list_to_atom(string:to_lower(binary_to_list(Change))),
                   keyspace = Keyspace,
                   table = case Table of
                               <<>> -> undefined;
                               _    -> Table
                           end}.

%% -------------------------------------------------------------------------
%% Internal
%% -------------------------------------------------------------------------

encode_query_flags(QueryParams) ->
    {ValueFlag, Values} = values(QueryParams),
    SkipMetadataFlag = skip_meta(QueryParams),
    {PageSizeFlag, ResultPageSize} = page_size(QueryParams),
    {PagingStateFlag, PagingState} = paging_state(QueryParams),
    {SerialConsistencyFlag, SerialConsistency} = serial_consistency(QueryParams),
    Flags = << 0:3, SerialConsistencyFlag:1, PagingStateFlag:1, PageSizeFlag:1, SkipMetadataFlag:1, ValueFlag:1 >>,
    <<
    (seestar_types:encode_consistency(QueryParams#query_params.consistency))/binary,
    Flags/binary,
    Values/binary, ResultPageSize/binary, PagingState/binary, SerialConsistency/binary
    >>.

serial_consistency(#query_params{serial_consistency = serial}) ->
    {0, <<>>};

serial_consistency(#query_params{serial_consistency = local_serial}) ->
    {1, seestar_types:encode_consistency(local_serial)}.

paging_state(#query_params{paging_state = undefined}) ->
    {0, <<>>};
paging_state(#query_params{paging_state = PagingState}) ->
    {1, seestar_types:encode_bytes(PagingState)}.

page_size(#query_params{page_size = undefined}) ->
    {0, <<>>};
page_size(#query_params{page_size = PageSize}) when is_integer(PageSize) ->
    {1, seestar_types:encode_int(PageSize)}.

skip_meta(_) ->
    0.

values(#query_params{values = []}) ->
    {0, <<>>};
values(#query_params{values = Values, types = Types}) when length(Types) == length(Values) ->
    Variables = << <<(seestar_cqltypes:encode_value_with_size(Type, Value))/binary>>
        || {Type, Value} <- lists:zip(Types, Values) >>,
    {1, <<
    (seestar_types:encode_short(length(Values)))/binary ,
    Variables/binary>>};
values(#query_params{values = Values, types = []}) when is_list(Values) ->
    %% This happens in the case of the unprepared query. Types need to be guessed
    %% TODO -> Could be a little clearer on the whole process
    Variables = << <<(seestar_cqltypes:encode_value_with_size(Value))/binary>> || Value <- Values >>,
    {1, <<
    (seestar_types:encode_short(length(Values)))/binary ,
    Variables/binary>>}.
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

%% Op Codes
%% requests.
-define(STARTUP, 16#01).
-define(AUTH_RESPONSE, 16#0F).
-define(OPTIONS, 16#05).
-define(QUERY, 16#07).
-define(PREPARE, 16#09).
-define(EXECUTE, 16#0A).
-define(BATCH, 16#0D).
-define(REGISTER, 16#0B).
%% responses.
-define(ERROR, 16#00).
-define(READY, 16#02).
-define(AUTHENTICATE, 16#03).
-define(SUPPORTED, 16#06).
-define(RESULT, 16#08).
-define(AUTH_CHALLENGE, 16#0E).
-define(AUTH_SUCCESS, 16#10).

%% event.
-define(EVENT, 16#0C).

%% Used in requests and responses
-record(column,
        {keyspace :: binary(),
        table :: binary(),
        name :: binary(),
        type :: seestar_cqltypes:type()}).

-record(metadata,
        {has_more_results   = false     :: boolean(),
         paging_state       = undefined :: undefined | binary(),
         columns                        :: [#column{}]}).

%% requests.
-record(startup,
        {version = <<"3.0.0">> :: binary(),
         compression :: undefined | binary()}).

-record(auth_response,
        {body :: binary()}).

-record(options,
        {}).

-record(query_values,
        {types  = []    :: [seestar_cqltypes:type()],
         values = []    :: [seestar_cqltypes:value()]}).

-record(query_params,
        {consistency        = one               :: atom(),
         values             = #query_values{}   :: #query_values{},
         cached_result_meta = undefined         :: undefined | #metadata{},
         page_size          = undefined         :: undefined | non_neg_integer(),
         paging_state       = undefined         :: undefined | binary(),
         serial_consistency = serial            :: serial | local_serial}).

-record('query',
        {'query' :: binary(),
         params  :: #query_params{}}).

-record(prepare,
        {'query' :: binary()}).

-record(execute,
        {id :: binary(),
         params  :: #query_params{}}).

-record(batch_query,
        {kind                               :: prepared | not_prepared,
         string_or_id                       :: binary(),
         values         = #query_values{}   :: #query_values{}
        }).
-record(batch,
        {type       = logged    :: logged | unlogged | counter,
        queries     = []        :: list(#batch_query{}),
        consistency = one       :: atom()}).

-record(register,
        {event_types = [] :: [topology_change | status_change | schema_change]}).

%% responses.
-record(ready,
        {}).

-record(auth_success,
        {}).

-record(auth_challenge,
        {body :: binary()}).

-record(authenticate,
        {class :: binary()}).

-record(supported,
        {versions :: [binary()],
         compression :: [binary()]}).

%% error and various details
-record(unavailable,
        {consistency :: atom(),
         required :: integer(),
         alive :: integer()}).

-record(write_timeout,
        {consistency :: atom(),
         received :: integer(),
         required :: integer(),
         write_type :: atom()}).

-record(read_timeout,
        {consistency :: atom(),
         received :: integer(),
         required :: integer(),
         data_present :: boolean()}).

-record(already_exists,
        {keyspace :: binary(),
         table :: binary() | undefined}).

-record(unprepared,
        {id :: binary()}).

-record(error,
        {code :: non_neg_integer(),
         message :: binary(),
         details :: undefined
                  | #unavailable{}
                  | #write_timeout{}
                  | #read_timeout{}
                  | #already_exists{}
                  | #unprepared{}}).

%% result and various result sub-types.
-record(rows,
        {metadata :: metadata(),
         rows :: [[seestar_cqltypes:value()]],
         initial_query :: #execute{} | #'query'{} %% used for fetching the next page
        }).

-record(set_keyspace,
        {keyspace :: binary()}).

-record(prepared,
        {id :: binary(),
         request_metadata :: #metadata{},
         result_metadata :: #metadata{}}).

%% also an event.
-record(schema_change,
        {change :: created | updated | dropped,
         keyspace :: binary(),
         table :: binary() | undefined}).

-record(result,
        {result :: void
                 | #rows{}
                 | #set_keyspace{}
                 | #prepared{}
                 | #schema_change{}}).

%% event.
-record(topology_change,
        {change :: new_node | removed_node,
         ip :: inet:ip_address(),
         port :: inet:port_number()}).

-record(status_change,
        {change :: up | down,
         ip :: inet:ip_address(),
         port :: inet:port_number()}).

-record(event,
        {event :: #topology_change{}
                | #status_change{}
                | #schema_change{}}).

-type metadata() :: #metadata{}.
-type column() :: #column{}.
-export_type([metadata/0, column/0]).


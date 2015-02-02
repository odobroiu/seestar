%%%-------------------------------------------------------------------
%%% @author odobroiu
%%% @copyright (C) 2015, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 31. Jan 2015 1:17 PM
%%%-------------------------------------------------------------------
-module(seestar_bound).
-author("odobroiu").

-include("seestar_messages.hrl").
-include("seestar.hrl").

%% API
-export([new/2, new/3, new/4, set_page_size/2]).

new(Query, Consistency) ->
    new(Query, Consistency, []).

new(Query, Consistency, Values) ->
    new(Query, Consistency, Values, undefined).

new(#prepared_query{id = QueryID, request_types = Types, cached_result_meta = ResultMeta}, Consistency, Values, PageSize) ->
    #execute{id = QueryID,
        params = #query_params{consistency = Consistency,
                               values = #query_values{values = Values, types = Types},
                               cached_result_meta = ResultMeta,
                               page_size = PageSize}}.

set_page_size(#execute{params = Params} = Statement, PageSize) ->
    NewParams = Params#query_params{page_size = PageSize},
    Statement#execute{params = NewParams}.

%%%-------------------------------------------------------------------
%%% @author odobroiu
%%% @copyright (C) 2015, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 31. Jan 2015 11:54 AM
%%%-------------------------------------------------------------------
-module(seestar_statement).
-author("odobroiu").

-include("seestar_messages.hrl").

-define(l2b(Term), case is_list(Term) of true -> list_to_binary(Term); false -> Term end).

%% API
-export([new/2, new/3, new/4, set_page_size/2]).

new(Query, Consistency) ->
    new(Query, Consistency, []).

new(Query, Consistency, Values) ->
    new(Query, Consistency, Values, undefined).

new(Query, Consistency, Values, PageSize) ->
    #'query'{query = ?l2b(Query),
             params = #query_params{consistency = Consistency,
                                    values = #query_values{values = Values},
                                    page_size = PageSize}}.

set_page_size(#'query'{params = Params} = Statement, PageSize) ->
    NewParams = Params#query_params{page_size = PageSize},
    Statement#'query'{params = NewParams}.
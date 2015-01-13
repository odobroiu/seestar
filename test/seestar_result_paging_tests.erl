-module(seestar_result_paging_tests).

-include_lib("eunit/include/eunit.hrl").
-include_lib("seestar/include/constants.hrl").

paging_test_() ->
    {foreach,
        fun test_utils:connect/0,
        fun test_utils:close/1,
        [fun(Pid) -> {with, Pid, [fun query_sync/1]} end,
         fun(Pid) -> {with, Pid, [fun query_async/1]} end,
         fun(Pid) -> {with, Pid, [fun execute_sync/1]} end,
         fun(Pid) -> {with, Pid, [fun execute_async/1]} end
        ]}.


query_sync(Pid) ->
    insert_data(Pid),
    %% Check if updated
    {ok, PagedSelectResult} = seestar_session:perform(Pid, "SELECT * FROM seestar_test_table", one, 100),
    NumberOfRows = count_rows(Pid, PagedSelectResult),
    ?assertEqual(2000, NumberOfRows).

query_async(Pid) ->
    insert_data(Pid),
    %% Check if updated
    Ref = seestar_session:perform_async(Pid, "SELECT * FROM seestar_test_table", one, 100),
    receive
        {seestar_response, Ref, F} ->
            {ok, PagedSelectResult} = F(),
            NumberOfRows = count_rows(Pid, PagedSelectResult),
            ?assertEqual(2000, NumberOfRows)
    end.

execute_sync(Pid) ->
    insert_data(Pid),
    %% Check if updated

    SelectQuery = "SELECT * FROM seestar_test_table",
    {ok, PreparedResult} = seestar_session:prepare(Pid, SelectQuery),
    QueryID = seestar_result:query_id(PreparedResult),

    {ok, PagedSelectResult} = seestar_session:execute(Pid, QueryID, one, 100),
    NumberOfRows = count_rows(Pid, PagedSelectResult),
    ?assertEqual(2000, NumberOfRows).

execute_async(Pid) ->
    insert_data(Pid),
    %% Check if updated
    SelectQuery = "SELECT * FROM seestar_test_table",
    {ok, PreparedResult} = seestar_session:prepare(Pid, SelectQuery),
    QueryID = seestar_result:query_id(PreparedResult),

    Ref = seestar_session:execute_async(Pid, QueryID, one, 100),
    receive
        {seestar_response, Ref, F} ->
            {ok, PagedSelectResult} = F(),
            NumberOfRows = count_rows_async(Pid, PagedSelectResult),
            ?assertEqual(2000, NumberOfRows)
    end.


%% -------------------------------------------------------------------------
%% Internal
%% -------------------------------------------------------------------------
insert_data(Pid) ->
    test_utils:create_keyspace(Pid, "seestar", 1),
    {ok, _Res1} = seestar_session:perform(Pid, "USE seestar", one),
    CreateTable = <<"CREATE TABLE seestar_test_table (id int primary key, value text)">>,
    {ok, _Res2} = seestar_session:perform(Pid, CreateTable, one),
    InsertQuery = <<"INSERT INTO seestar_test_table(id, value) values (?, ?)">>,
    {ok, PreparedResult} = seestar_session:prepare(Pid, InsertQuery),
    QueryID = seestar_result:query_id(PreparedResult),
    Types = seestar_result:types(PreparedResult),
    PreparedQueriesList = [
        seestar_batch:prepared_query(QueryID, Types, [I, <<"The fox">>])
        || I <- lists:seq(1, 2000)
    ],
    Batch = seestar_batch:batch_request(logged, one, PreparedQueriesList),
    {ok, void} = seestar_session:batch(Pid, Batch).

count_rows(Pid, PagedSelectResult) ->
    count_rows(Pid, PagedSelectResult, 0).

count_rows(Pid, PagedSelectResult, N) ->
    case seestar_result:has_more_rows(PagedSelectResult) of
        false ->
            N + length(seestar_result:rows(PagedSelectResult));
        true ->
            {ok, NextPage} = seestar_session:next_page(Pid, PagedSelectResult),
            count_rows(Pid, NextPage, N + length(seestar_result:rows(PagedSelectResult)))
    end.

count_rows_async(Pid, PagedSelectResult) ->
    count_rows_async(Pid, PagedSelectResult, 0).

count_rows_async(Pid, PagedSelectResult, N) ->
    case seestar_result:has_more_rows(PagedSelectResult) of
        false ->
            N + length(seestar_result:rows(PagedSelectResult));
        true ->
            Ref = seestar_session:next_page_async(Pid, PagedSelectResult),
            receive
                {seestar_response, Ref, F} ->
                    {ok, NewPagedSelectResult} = F(),
                    count_rows_async(Pid, NewPagedSelectResult, N + length(seestar_result:rows(PagedSelectResult)))
            end
    end.







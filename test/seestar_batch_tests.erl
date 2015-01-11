-module(seestar_batch_tests).

-include_lib("eunit/include/eunit.hrl").
-include_lib("seestar/include/constants.hrl").

batch_test_() ->
    {foreach,
        fun test_utils:connect/0,
        fun test_utils:close/1,
        [fun(Pid) -> {with, Pid, [fun simple/1]} end
        ]}.


simple(Pid) ->
    test_utils:create_keyspace(Pid, "seestar", 1),
    {ok, _Res1} = seestar_session:perform(Pid,  "USE seestar", one),

    CreateTable = <<"CREATE TABLE seestar_test_table (id int primary key, value text)">>,
    {ok, _Res2} = seestar_session:perform(Pid, CreateTable, one),

    InsertQuery = <<"INSERT INTO seestar_test_table(id, value) values (?, ?)">>,
    {ok, PreparedResult} = seestar_session:prepare(Pid, InsertQuery),
    QueryID = seestar_result:query_id(PreparedResult),
    Types = seestar_result:types(PreparedResult),

    NormalQueriesList = [
        seestar_batch:normal_query(<<"INSERT INTO seestar_test_table(id, value) values (?, ?)">>, [I, <<"The fox">>] )
        || I <- lists:seq(1,100)
    ],

    PreparedQueriesList = [
        seestar_batch:prepared_query(QueryID, Types, [I, <<"The fox">>])
        || I <- lists:seq(101,200)
    ],

    Batch = seestar_batch:batch_request(logged, one, NormalQueriesList ++ PreparedQueriesList),
    {ok, void} = seestar_session:batch(Pid, Batch),
    %% Check if updated
    {ok, SelectResult} = seestar_session:perform(Pid, "SELECT * FROM seestar_test_table", one),
    ?assertEqual(200, length(seestar_result:rows(SelectResult))).



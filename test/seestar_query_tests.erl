-module(seestar_query_tests).

-include_lib("eunit/include/eunit.hrl").
-include_lib("seestar/include/constants.hrl").

query_test_() ->
    {foreach,
        fun test_utils:connect/0,
        fun test_utils:close/1,
        [fun(Pid) -> {with, Pid, [fun test_schema_queries/1]} end
        ,fun(Pid) -> {with, Pid, [fun insert_update_delete/1]} end
        ]}.


test_schema_queries(Pid) ->
    Qry0 = "CREATE KEYSPACE seestar "
    "WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1}",
    {ok, Res0} = seestar_session:perform(Pid, Qry0, one),
    ?assertEqual(schema_change, seestar_result:type(Res0)),
    ?assertEqual(<<"seestar">>, seestar_result:keyspace(Res0)),
    ?assertEqual(undefined, seestar_result:table(Res0)),

    {error, Err0} = seestar_session:perform(Pid, Qry0, one),
    ?assertEqual(?ALREADY_EXISTS, seestar_error:code(Err0)),
    ?assertEqual(<<"seestar">>, seestar_error:keyspace(Err0)),
    ?assertEqual(undefined, seestar_error:table(Err0)),

    Qry1 = "USE seestar",
    {ok, Res1} = seestar_session:perform(Pid, Qry1, one),
    ?assertEqual(set_keyspace, seestar_result:type(Res1)),
    ?assertEqual(<<"seestar">>, seestar_result:keyspace(Res1)),

    Qry2 = "CREATE TABLE seestar_test_table (id int primary key, value text)",
    {ok, Res2} = seestar_session:perform(Pid, Qry2, one),
    ?assertEqual(schema_change, seestar_result:type(Res2)),
    ?assertEqual(<<"seestar">>, seestar_result:keyspace(Res2)),
    ?assertEqual(<<"seestar_test_table">>, seestar_result:table(Res2)),

    {error, Err1} = seestar_session:perform(Pid, Qry2, one),
    ?assertEqual(?ALREADY_EXISTS, seestar_error:code(Err1)),
    ?assertEqual(<<"seestar">>, seestar_error:keyspace(Err1)),
    ?assertEqual(<<"seestar_test_table">>, seestar_error:table(Err1)).

insert_update_delete(Pid) ->
    test_utils:create_keyspace(Pid, "seestar", 1),
    {ok, _Res1} = seestar_session:perform(Pid, "USE seestar", one),

    CreateTable = "CREATE TABLE seestar_test_table (id int primary key, value text)",
    {ok, _Res2} = seestar_session:perform(Pid, CreateTable, one),

    %% Insert a row
    {ok, void} = seestar_session:perform(Pid, "INSERT INTO seestar_test_table(id, value) values (?, ?)", one, [1, <<"The quick brown fox">>]),

    %% Check if row exists
    {ok, SelectResult} = seestar_session:perform(Pid, "SELECT * FROM seestar_test_table where id = ?", one, [1]),
    ?assertEqual(2, length(seestar_result:types(SelectResult))),
    ?assertEqual([[1, <<"The quick brown fox">>]], seestar_result:rows(SelectResult)),

    %% Update row
    {ok, void} = seestar_session:perform(Pid, "UPDATE seestar_test_table set value = ? where id = ?", one, [<<"UpdatedText">>, 1]),

    %% Check if updated
    {ok, SelectResult2} = seestar_session:perform(Pid, "SELECT * FROM seestar_test_table where id = ?", one, [1]),
    ?assertEqual([[1, <<"UpdatedText">>]], seestar_result:rows(SelectResult2)),

    %% Delete Row
    {ok, void} = seestar_session:perform(Pid, "DELETE FROM seestar_test_table where id = ?", one, [1]),

    %% Check if row no longer exists
    {ok, SelectResult3} = seestar_session:perform(Pid, "SELECT * FROM seestar_test_table where id = ?", one, [1]),
    ?assertEqual([], seestar_result:rows(SelectResult3)).

-module(seestar_session_tests).

-include_lib("eunit/include/eunit.hrl").
-include_lib("seestar/include/constants.hrl").

schema_test_() ->
    {foreach,
        fun test_utils:connect/0,
        fun test_utils:close/1,
        [   fun(Pid) -> {with, Pid, [fun test_schema_queries/1]} end]}.

session_test_() ->
    {setup,
        fun test_utils:connect/0,
        fun test_utils:close/1,
        fun (ConnectionPid) ->
            [
                {foreach, fun()-> connect_to_keyspace(ConnectionPid) end, fun drop_keyspace/1,
                    [
                        fun(Pid) -> {with, Pid, [fun test_native_types/1]} end,
                        fun(Pid) -> {with, Pid, [fun test_collection_types/1]} end,
                        fun(Pid) -> {with, Pid, [fun test_counter_type/1]} end ,
                        fun(Pid) -> {with, Pid, [fun result_paging_query_sync/1]} end,
                        fun(Pid) -> {with, Pid, [fun result_paging_query_async/1]} end,
                        fun(Pid) -> {with, Pid, [fun result_paging_execute_sync/1]} end,
                        fun(Pid) -> {with, Pid, [fun result_paging_execute_async/1]} end,
                        fun(Pid) -> {with, Pid, [fun perform_insert_update_delete/1]} end,
                        fun(Pid) -> {with, Pid, [fun batch_tests/1]} end,
                        fun(Pid) -> {with, Pid, [fun multiple_inserts/1]} end
                    ]}
            ]
        end
    }.

%% -------------------------------------------------------------------------
%% Test Cases
%% -------------------------------------------------------------------------

test_schema_queries(Pid) ->
    Qry0 = "CREATE KEYSPACE seestar "
    "WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1}",
    CreateKeyspaceStatement = seestar_statement:new(Qry0, one),
    {ok, Res0} = seestar_session:execute(Pid, CreateKeyspaceStatement),
    ?assertEqual(schema_change, seestar_result:type(Res0)),
    ?assertEqual(<<"seestar">>, seestar_result:keyspace(Res0)),
    ?assertEqual(undefined, seestar_result:table(Res0)),

    {error, Err0} = seestar_session:execute(Pid, CreateKeyspaceStatement),
    ?assertEqual(?ALREADY_EXISTS, seestar_error:code(Err0)),
    ?assertEqual(<<"seestar">>, seestar_error:keyspace(Err0)),
    ?assertEqual(undefined, seestar_error:table(Err0)),

    Qry1 = "USE seestar",
    UseStatement = seestar_statement:new(Qry1, one),
    {ok, Res1} = seestar_session:execute(Pid, UseStatement),
    ?assertEqual(set_keyspace, seestar_result:type(Res1)),
    ?assertEqual(<<"seestar">>, seestar_result:keyspace(Res1)),

    Qry2 = "CREATE TABLE seestar_test_table (id int primary key, value text)",
    CreateTableStatement = seestar_statement:new(Qry2, one),
    {ok, Res2} = seestar_session:execute(Pid, CreateTableStatement),
    ?assertEqual(schema_change, seestar_result:type(Res2)),
    ?assertEqual(<<"seestar">>, seestar_result:keyspace(Res2)),
    ?assertEqual(<<"seestar_test_table">>, seestar_result:table(Res2)),

    {error, Err1} = seestar_session:execute(Pid, CreateTableStatement),
    ?assertEqual(?ALREADY_EXISTS, seestar_error:code(Err1)),
    ?assertEqual(<<"seestar">>, seestar_error:keyspace(Err1)),
    ?assertEqual(<<"seestar_test_table">>, seestar_error:table(Err1)).

test_native_types(Pid) ->
    Qry0 = "CREATE TABLE seestar.has_all_types (
                asciicol ascii,
                bigintcol bigint,
                blobcol blob,
                booleancol boolean,
                decimalcol decimal,
                doublecol double,
                floatcol float,
                inetcol inet,
                intcol int,
                textcol text,
                timestampcol timestamp,
                timeuuidcol timeuuid,
                uuidcol uuid,
                varcharcol varchar,
                varintcol varint,
                PRIMARY KEY(asciicol)
            )",
    CreateTableStament = seestar_statement:new(Qry0, one),
    {ok, _} = seestar_session:execute(Pid, CreateTableStament),
    % test serialization.
    Qry1 = "INSERT INTO seestar.has_all_types (
               asciicol, bigintcol, blobcol, booleancol, decimalcol, doublecol, floatcol,
               inetcol, intcol, textcol, timestampcol, timeuuidcol, uuidcol, varcharcol, varintcol)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
    {ok, Res1} = seestar_session:prepare(Pid, Qry1),
    PreparedQuery = seestar_result:prepared_query(Res1),
    Types = seestar_result:types(Res1),
    ?assertEqual([ascii, bigint, blob, boolean, decimal, double, float,
        inet, int, varchar, timestamp, timeuuid, uuid, varchar, varint],
        Types),
    Row0 = [<<"abcd">>, 1234567890123456789, <<4,2>>, true, {1995211882, 5},
        9999999.999, 99999.9921875, {127,0,0,1}, 100, <<"Voilá!">>, {1368,199874,337000},
        <<146,135,233,168,39,16,17,187,131,194,96,197,71,12,191,14>>,
        <<113,68,80,223,85,99,74,129,188,158,84,49,50,156,40,232>>,
        <<>>, 10000000000000000000000000],
    Row1 = [<<"cdef">>, 1234567890123456789, <<2,4>>, false, {1995211882, 6}, 9999999.999,
        99999.9921875, {255,255,255,255,255,255,255,255}, 200, <<"текст">>, {1368,199874,337000},
        <<135,99,103,104,40,81,17,187,181,58,96,197,71,12,191,14>>,
        <<148,125,144,228,220,27,68,12,148,158,178,154,25,169,42,113>>,
        <<>>, 100000000000000000000000000],
    BoundStmt1 = seestar_bound:new(PreparedQuery, one, Row0),
    BoundStmt2 = seestar_bound:new(PreparedQuery, one, Row1),
    {ok, _} = seestar_session:execute(Pid, BoundStmt1),
    {ok, _} = seestar_session:execute(Pid, BoundStmt2),
    % test deserialization.
    Qry2 = "SELECT asciicol, bigintcol, blobcol, booleancol, decimalcol, doublecol, floatcol,
                   inetcol, intcol, textcol, timestampcol, timeuuidcol, uuidcol, varcharcol, varintcol
            FROM seestar.has_all_types",
    SelectStmt = seestar_statement:new(Qry2, one),
    {ok, Res2} = seestar_session:execute(Pid, SelectStmt),
    ?assertEqual(Types, seestar_result:types(Res2)),
    ?assertEqual([Row0, Row1], seestar_result:rows(Res2)).

test_counter_type(Pid) ->
    Qry0 = "CREATE TABLE seestar.has_counter_type (id int PRIMARY KEY, counter counter)",
    CreateTableStmt = seestar_statement:new(Qry0, one),
    {ok, _} = seestar_session:execute(Pid, CreateTableStmt),
    Qry1 = "UPDATE seestar.has_counter_type SET counter = counter + ? WHERE id = ?",
    {ok, Res1} = seestar_session:prepare(Pid, Qry1),
    PreparedQuery = seestar_result:prepared_query(Res1),
    [ {ok, _} = seestar_session:execute(Pid, seestar_bound:new(PreparedQuery, one, [C, 0])) || C <- [ 1, -2, 3 ] ],
    Qry2 = "SELECT id, counter FROM seestar.has_counter_type WHERE id = 0",
    SelectStmt = seestar_statement:new(Qry2, one),
    {ok, Res2} = seestar_session:execute(Pid, SelectStmt),
    ?assertEqual([[0, 2]], seestar_result:rows(Res2)).

test_collection_types(Pid) ->
    Qry0 = "CREATE TABLE seestar.has_collection_types (
                id int,
                mapcol map<text,blob>,
                setcol set<int>,
                listcol list<boolean>,
                PRIMARY KEY(id)
            )",
    CreateTableStmt = seestar_statement:new(Qry0, one),
    {ok, _} = seestar_session:execute(Pid, CreateTableStmt),
    Qry1 = "INSERT INTO seestar.has_collection_types (id, mapcol, setcol, listcol) VALUES (?, ?, ?, ?)",
    {ok, Res1} = seestar_session:prepare(Pid, Qry1),
    PreparedQuery = seestar_result:prepared_query(Res1),
    Row0 = [0, null, null, null],
    Row1 = [1, dict:from_list([{<<"k1">>, <<"v1">>}]), sets:from_list([1]), [true]],
    Row2 = [2, dict:from_list([{<<"k1">>, <<"v1">>}, {<<"k2">>, <<"v2">>}]), sets:from_list([1,2]), [true, false]],
    [ {ok, _} = seestar_session:execute(Pid, seestar_bound:new(PreparedQuery, one, R)) || R <- [Row0, Row1, Row2] ],
    SelectStmt = seestar_statement:new("SELECT id, mapcol, setcol, listcol FROM seestar.has_collection_types" , one),
    {ok, Res2} = seestar_session:execute(Pid, SelectStmt),
    ?assertEqual([Row1, Row0, Row2], seestar_result:rows(Res2)).

result_paging_query_sync(Pid) ->
    insert_data_for_paging(Pid),
    %% Check if updated
    SelectStmt0 = seestar_statement:new("SELECT * FROM seestar_test_table" , one),
    SelectStmt1 = seestar_statement:set_page_size(SelectStmt0, 100),
    {ok, PagedSelectResult} = seestar_session:execute(Pid, SelectStmt1),
    NumberOfRows = count_rows(Pid, PagedSelectResult),
    ?assertEqual(2000, NumberOfRows).

result_paging_query_async(Pid) ->
    insert_data_for_paging(Pid),
    %% Check if updated
    SelectStmt0 = seestar_statement:new("SELECT * FROM seestar_test_table", one),
    SelectStmt = seestar_statement:set_page_size(SelectStmt0, 100),
    Ref = seestar_session:execute_async(Pid, SelectStmt),
    receive
        {seestar_response, Ref, F} ->
            {ok, PagedSelectResult} = F(),
            NumberOfRows = count_rows(Pid, PagedSelectResult),
            ?assertEqual(2000, NumberOfRows)
    end.

result_paging_execute_sync(Pid) ->
    insert_data_for_paging(Pid),
    %% Check if updated

    SelectQuery = "SELECT * FROM seestar_test_table",
    {ok, PreparedResult} = seestar_session:prepare(Pid, SelectQuery),
    PreparedQuery = seestar_result:prepared_query(PreparedResult),

    BoundStmt0 = seestar_bound:new(PreparedQuery, one),
    BoundStmt = seestar_bound:set_page_size(BoundStmt0, 100),
    {ok, PagedSelectResult} = seestar_session:execute(Pid, BoundStmt),
    NumberOfRows = count_rows(Pid, PagedSelectResult),
    ?assertEqual(2000, NumberOfRows).

result_paging_execute_async(Pid) ->
    insert_data_for_paging(Pid),
    %% Check if updated
    SelectQuery = "SELECT * FROM seestar_test_table",
    {ok, PreparedResult} = seestar_session:prepare(Pid, SelectQuery),
    QueryID = seestar_result:prepared_query(PreparedResult),

    BoundStmt0 = seestar_bound:new(QueryID, one),
    BoundStmt = seestar_bound:set_page_size(BoundStmt0, 100),

    Ref = seestar_session:execute_async(Pid, BoundStmt),
    receive
        {seestar_response, Ref, F} ->
            {ok, PagedSelectResult} = F(),
            NumberOfRows = count_rows_async(Pid, PagedSelectResult),
            ?assertEqual(2000, NumberOfRows)
    end.

perform_insert_update_delete(Pid) ->
    CreateTableStmt = seestar_statement:new("CREATE TABLE seestar_test_table (id int primary key, value text)", one),
    {ok, _Res2} = seestar_session:execute(Pid, CreateTableStmt),

    %% Insert a row
    InsertRowStmt = seestar_statement:new("INSERT INTO seestar_test_table(id, value) values (?, ?)",
        one,
        [1, <<"The quick brown fox">>]),
    {ok, void} = seestar_session:execute(Pid, InsertRowStmt),

    %% Check if row exists
    SelectRowStmt = seestar_statement:new("SELECT * FROM seestar_test_table where id = ?", one, [1]),
    {ok, SelectResult} = seestar_session:execute(Pid, SelectRowStmt),
    ?assertEqual([[1, <<"The quick brown fox">>]], seestar_result:rows(SelectResult)),

    %% Update row
    UpdateStmt = seestar_statement:new("UPDATE seestar_test_table set value = ? where id = ?", one, [<<"UpdatedText">>, 1]),
    {ok, void} = seestar_session:execute(Pid, UpdateStmt),

    %% Check if updated
    {ok, SelectResult2} = seestar_session:execute(Pid, SelectRowStmt),
    ?assertEqual([[1, <<"UpdatedText">>]], seestar_result:rows(SelectResult2)),

    %% Delete Row
    DeleteStmt = seestar_statement:new("DELETE FROM seestar_test_table where id = ?", one, [1]),
    {ok, void} = seestar_session:execute(Pid, DeleteStmt),

    %% Check if row no longer exists
    {ok, SelectResult3} = seestar_session:execute(Pid, SelectRowStmt),
    ?assertEqual([], seestar_result:rows(SelectResult3)).

batch_tests(Pid) ->
    CreateTableStmt = seestar_statement:new(<<"CREATE TABLE seestar_test_table (id int primary key, value text)">>, one),
    {ok, _Res2} = seestar_session:execute(Pid, CreateTableStmt),

    InsertQuery = <<"INSERT INTO seestar_test_table(id, value) values (?, ?)">>,
    {ok, PreparedResult} = seestar_session:prepare(Pid, InsertQuery),
    PreparedQuery = seestar_result:prepared_query(PreparedResult),

    NormalQueriesList = [
        seestar_batch:normal_query(<<"INSERT INTO seestar_test_table(id, value) values (?, ?)">>, [I, <<"The fox">>] )
        || I <- lists:seq(1,100)
    ],

    PreparedQueriesList = [
        seestar_batch:prepared_query(PreparedQuery, [I, <<"The fox">>])
        || I <- lists:seq(101,200)
    ],

    Batch = seestar_batch:batch_request(logged, one, NormalQueriesList ++ PreparedQueriesList),
    {ok, void} = seestar_session:execute(Pid, Batch),
    %% Check if updated
    SelectStmt = seestar_statement:new("SELECT * FROM seestar_test_table", one),
    {ok, SelectResult} = seestar_session:execute(Pid, SelectStmt),
    ?assertEqual(200, length(seestar_result:rows(SelectResult))).

multiple_inserts(Pid) ->
    CreateTableStmt = seestar_statement:new(<<"CREATE TABLE seestar_test_table (id int primary key, value text)">>, one),
    {ok, _Res2} = seestar_session:execute(Pid, CreateTableStmt),

    %% Prepare Query
    Query = "INSERT INTO seestar_test_table(id, value) values (?, ?)",
    {ok, Res1} = seestar_session:prepare(Pid, Query),
    PreparedQuery = seestar_result:prepared_query(Res1),

    N = 10000,

    [seestar_session:execute_async(Pid, seestar_bound:new(PreparedQuery, one, [ID, <<"The fox">>])) || ID <- lists:seq(1, N)],

    wait_for_results(N).

%% -------------------------------------------------------------------------
%% Internal
%% -------------------------------------------------------------------------
connect_to_keyspace(Pid)->
    test_utils:create_keyspace(Pid, "seestar", 1),
    {ok, _Res1} = seestar_session:execute(Pid, seestar_statement:new("USE seestar", one)),
    Pid.

drop_keyspace(Pid)->
    test_utils:drop_keyspace(Pid, "seestar").

insert_data_for_paging(Pid) ->
    CreateTableStmt = seestar_statement:new(<<"CREATE TABLE seestar_test_table (id int primary key, value text)">>, one),
    {ok, _Res2} = seestar_session:execute(Pid, CreateTableStmt),
    InsertQuery = <<"INSERT INTO seestar_test_table(id, value) values (?, ?)">>,
    {ok, PreparedResult} = seestar_session:prepare(Pid, InsertQuery),
    QueryID = seestar_result:prepared_query(PreparedResult),
    PreparedQueriesList = [
        seestar_batch:prepared_query(QueryID, [I, <<"The fox">>])
        || I <- lists:seq(1, 2000)
    ],
    Batch = seestar_batch:batch_request(logged, one, PreparedQueriesList),
    {ok, void} = seestar_session:execute(Pid, Batch).

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

wait_for_results(0) ->
    ok;
wait_for_results(N) ->
    receive
        {seestar_response, _Ref, F} ->
            {ok, _SelectResult} = F(),
            wait_for_results(N-1)
    end.

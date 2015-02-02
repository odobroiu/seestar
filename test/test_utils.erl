-module(test_utils).
-author("odobroiu").

%% API
-export([connect/0, close/1, create_keyspace/3, drop_keyspace/2]).

connect() ->
    seestar_ccm:create(),
    seestar_ccm:start(),
    wait_for_cassandra_to_accept_connections(10, 50),
    {ok, Pid} = seestar_session:start_link("localhost", 9042),
    unlink(Pid),
    Pid.

close(Pid) ->
    seestar_session:stop(Pid),
    seestar_ccm:remove().

create_keyspace(Pid, Name, RF) ->
    Qry = "CREATE KEYSPACE ~s WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': ~w}",
    CreateKeyspaceStatement = seestar_statement:new(lists:flatten(io_lib:format(Qry, [Name, RF])), one),
    {ok, _} = seestar_session:execute(Pid, CreateKeyspaceStatement).

drop_keyspace(Pid, Name) ->
    Qry = "DROP KEYSPACE ~s",
    DropKeyspaceStatement = seestar_statement:new(lists:flatten(io_lib:format(Qry, [Name])), one),
    {ok, _} = seestar_session:execute(Pid, DropKeyspaceStatement).

wait_for_cassandra_to_accept_connections(Retries, SleepTime) ->
    CurrentStatus = os:cmd("cqlsh"),
    case re:run(CurrentStatus, "error", [{capture, first, list}]) of
        nomatch ->
            ok;
        {match,["error"]} ->
            timer:sleep(SleepTime),
            wait_for_cassandra_to_accept_connections(Retries-1, SleepTime)
    end.
-module(test_utils).
-author("odobroiu").

%% API
-export([connect/0, close/1, create_keyspace/3]).

connect() ->
    seestar_ccm:create(),
    seestar_ccm:start(),
    timer:sleep(500),
    {ok, Pid} = seestar_session:start_link("localhost", 9042),
    unlink(Pid),
    Pid.

close(Pid) ->
    seestar_session:stop(Pid),
    seestar_ccm:remove().

create_keyspace(Pid, Name, RF) ->
    Qry = "CREATE KEYSPACE ~s WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': ~w}",
    {ok, _} = seestar_session:perform(Pid, lists:flatten(io_lib:format(Qry, [Name, RF])), one).
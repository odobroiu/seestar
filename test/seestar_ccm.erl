-module(seestar_ccm).

-export([
    create/0,
    create/1,
    start/0,
    stop/0,
    remove/0,
    update_config/1
]).

create() ->
    create(1).

create(Nodes) ->
    cmd("create seestar_unit_test_cluster -n ~w -b --install-dir ~s", [Nodes, getenv("CASSANDRA_DIR", "./")]).

remove() ->
    stop(),
    cmd("remove").

start() ->
    cmd("start").

stop() ->
    cmd("stop").

cmd(Cmd) ->
    os:cmd("ccm " ++ Cmd).

cmd(Pattern, Variables) ->
    cmd(lists:flatten(io_lib:format(Pattern, Variables))).

update_config(Configs) ->
    cmd("updateconf ~s" , [string:join(Configs, " ")]).

getenv(Name, Default) ->
    case os:getenv(Name) of
        false -> Default;
        Value -> Value
    end.

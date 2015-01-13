-module(seestar_auth_tests).

-include_lib("eunit/include/eunit.hrl").
-include_lib("seestar/include/constants.hrl").

auth_test_() ->
    {foreach,
        fun() ->
            seestar_ccm:create(),
            seestar_ccm:update_config(["authenticator:PasswordAuthenticator"]),
            seestar_ccm:start(),
            wait_for_cassandra_to_accept_auth_requests(1000)
        end,
        fun(_) ->
            seestar_ccm:remove()
        end,
        [
            fun single_test_function/0
        ]}.

%% Single test function so that we do not have to wait for the cluster to initialize
%% given that it seems that setting up password auth takes a while
single_test_function() ->
    %% Fail when no credentials provided
    {error, invalid_credentials} = seestar_session:start_link("localhost", 9042),

    %% Fail when bad credentials provided
    {error, invalid_credentials} = seestar_session:start_link("localhost", 9042,
        [{auth , {seestar_password_auth, {<<"bad">>, <<"credentials">>}}}]),

    %% Succeed when good credentials provided
    {ok, Pid} = seestar_session:start_link("localhost", 9042,
        [{auth , {seestar_password_auth, {<<"cassandra">>, <<"cassandra">>}}}]),
    unlink(Pid),
    seestar_session:stop(Pid).

%% -------------------------------------------------------------------------
%% Internal
%% -------------------------------------------------------------------------

wait_for_cassandra_to_accept_auth_requests(SleepTime) ->
    CurrentStatus = os:cmd("cqlsh -u cassandra -p cassandra"),
    case re:run(CurrentStatus, "AuthenticationException", [{capture, first, list}]) of
        nomatch ->
            ?debugMsg("Cassandra PasswordAuth ready ... "),
            ok;
        {match,["AuthenticationException"]} ->
            ?debugFmt("Waiting for cass password auth to be ready. Sleeping ~p ..." , [SleepTime]),
            timer:sleep(SleepTime),
            wait_for_cassandra_to_accept_auth_requests(SleepTime)
    end.

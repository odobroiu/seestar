-module(seestar_password_auth).

-include("seestar_messages.hrl").

%% API
-export([perform_auth/2]).

%% -------------------------------------------------------------------------
%% API
%% -------------------------------------------------------------------------

%% @doc
%% Method that needs to be implemented by any password authentication module
%% The sendFun accepts a #auth_response{} and returns #error{} | #auth_challenge{} | #auth_success{}
%% {Username, Password} is the data passed to the seestar_session module
%% @end
-spec perform_auth(SendFun :: function(), {Username :: binary(), Password :: binary()}) -> Success :: boolean().
perform_auth(SendFun, {Username, Password}) when is_function(SendFun), is_binary(Username), is_binary(Password) ->
    case SendFun(encode_credentials(Username, Password)) of
        #auth_challenge{} ->
            %% We do not expect any auth_challenge, so probably auth module is wrong
            false;
        #auth_success{} ->
            true;
        #error{} ->
            false
    end.

%% -------------------------------------------------------------------------
%% Internal
%% -------------------------------------------------------------------------

encode_credentials(Username, Password) when is_binary(Username), is_binary(Password)->
    Auth = << 0, Username/binary, 0, Password/binary >>,
    #auth_response{body= seestar_types:encode_bytes(Auth)}.

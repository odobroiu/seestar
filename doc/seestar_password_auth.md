

# Module seestar_password_auth #
* [Function Index](#index)
* [Function Details](#functions)


<a name="index"></a>

## Function Index ##


<table width="100%" border="1" cellspacing="0" cellpadding="2" summary="function index"><tr><td valign="top"><a href="#perform_auth-2">perform_auth/2</a></td><td>
Method that needs to be implemented by any password authentication module
The sendFun accepts a #auth_response{} and returns #error{} | #auth_challenge{} | #auth_success{}
{Username, Password} is the data passed to the seestar_session module.</td></tr></table>


<a name="functions"></a>

## Function Details ##

<a name="perform_auth-2"></a>

### perform_auth/2 ###


<pre><code>
perform_auth(SendFun::function(), X2::{Username::binary(), Password::binary()}) -&gt; Success::boolean()
</code></pre>
<br />


Method that needs to be implemented by any password authentication module
The sendFun accepts a #auth_response{} and returns #error{} | #auth_challenge{} | #auth_success{}
{Username, Password} is the data passed to the seestar_session module

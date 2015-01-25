

# Module seestar_session #
* [Data Types](#types)
* [Function Index](#index)
* [Function Details](#functions)

__Behaviours:__ [`gen_server`](gen_server.md).

<a name="types"></a>

## Data Types ##




### <a name="type-client_option">client_option()</a> ###



<pre><code>
client_option() = {keyspace, string() | binary()} | {credentials, <a href="#type-credentials">credentials()</a>} | {events, <a href="#type-events">events()</a>}
</code></pre>





### <a name="type-connect_option">connect_option()</a> ###



<pre><code>
connect_option() = <a href="gen_tcp.md#type-connect_option">gen_tcp:connect_option()</a> | {connect_timeout, timeout()} | {ssl, [<a href="ssl.md#type-connect_option">ssl:connect_option()</a>]}
</code></pre>





### <a name="type-credentials">credentials()</a> ###



<pre><code>
credentials() = [{string() | binary(), string() | binary()}]
</code></pre>





### <a name="type-events">events()</a> ###



<pre><code>
events() = [topology_change | status_change | schema_change]
</code></pre>





### <a name="type-query">query()</a> ###



<pre><code>
query() = binary() | string()
</code></pre>


<a name="index"></a>

## Function Index ##


<table width="100%" border="1" cellspacing="0" cellpadding="2" summary="function index"><tr><td valign="top"><a href="#batch-2">batch/2</a></td><td>Synchronously execute a batch query
Use <a href="seestar_batch.md"><code>seestar_batch</code></a> module functions to create the request.</td></tr><tr><td valign="top"><a href="#batch_async-2">batch_async/2</a></td><td>Asynchronously execute a batch query
Use <a href="seestar_batch.md"><code>seestar_batch</code></a> module functions to create the request.</td></tr><tr><td valign="top"><a href="#execute-3">execute/3</a></td><td></td></tr><tr><td valign="top"><a href="#execute-4">execute/4</a></td><td></td></tr><tr><td valign="top"><a href="#execute-5">execute/5</a></td><td>Synchronously execute a prepared query using the specified consistency level.</td></tr><tr><td valign="top"><a href="#execute_async-3">execute_async/3</a></td><td></td></tr><tr><td valign="top"><a href="#execute_async-4">execute_async/4</a></td><td></td></tr><tr><td valign="top"><a href="#execute_async-5">execute_async/5</a></td><td>Asynchronously execute a prepared query using the specified consistency level.</td></tr><tr><td valign="top"><a href="#next_page-2">next_page/2</a></td><td>Synchronously returns the next page for a previous paginated result.</td></tr><tr><td valign="top"><a href="#next_page_async-2">next_page_async/2</a></td><td>Asynchronously returns the next page for a previous paginated result.</td></tr><tr><td valign="top"><a href="#perform-3">perform/3</a></td><td></td></tr><tr><td valign="top"><a href="#perform-4">perform/4</a></td><td></td></tr><tr><td valign="top"><a href="#perform-5">perform/5</a></td><td>Synchoronously perform a CQL query using the specified consistency level.</td></tr><tr><td valign="top"><a href="#perform_async-3">perform_async/3</a></td><td></td></tr><tr><td valign="top"><a href="#perform_async-4">perform_async/4</a></td><td></td></tr><tr><td valign="top"><a href="#perform_async-5">perform_async/5</a></td><td>Asynchronously perform a CQL query using the specified consistency level.</td></tr><tr><td valign="top"><a href="#prepare-2">prepare/2</a></td><td>Prepare a query for later execution.</td></tr><tr><td valign="top"><a href="#start_link-2">start_link/2</a></td><td>Equivalent to <a href="#start_link-3"><tt>start_link(Host, Post, [])</tt></a>.</td></tr><tr><td valign="top"><a href="#start_link-3">start_link/3</a></td><td>Equivalent to <a href="#start_link-4"><tt>start_link(Host, Post, ClientOptions, [])</tt></a>.</td></tr><tr><td valign="top"><a href="#start_link-4">start_link/4</a></td><td>
Starts a new connection to a cassandra node on Host:Port.</td></tr><tr><td valign="top"><a href="#stop-1">stop/1</a></td><td>Stop the client.</td></tr></table>


<a name="functions"></a>

## Function Details ##

<a name="batch-2"></a>

### batch/2 ###

`batch(Client, Req) -> any()`

Synchronously execute a batch query
Use [`seestar_batch`](seestar_batch.md) module functions to create the request.
<a name="batch_async-2"></a>

### batch_async/2 ###

`batch_async(Client, Req) -> any()`

Asynchronously execute a batch query
Use [`seestar_batch`](seestar_batch.md) module functions to create the request.
<a name="execute-3"></a>

### execute/3 ###

`execute(Client, Query, Consistency) -> any()`

__See also:__ [execute/5](#execute-5).
<a name="execute-4"></a>

### execute/4 ###

`execute(Client, Query, Consistency, PageSize) -> any()`

__See also:__ [execute/5](#execute-5).
<a name="execute-5"></a>

### execute/5 ###


<pre><code>
execute(Client::pid(), Prepared_query::<a href="seestar_result.md#type-prepared_query">seestar_result:prepared_query()</a>, Values::[<a href="seestar_cqltypes.md#type-value">seestar_cqltypes:value()</a>], Consistency::<a href="seestar.md#type-consistency">seestar:consistency()</a>, PageSize::non_neg_integer() | undefined) -&gt; {ok, Result::<a href="seestar_result.md#type-result">seestar_result:result()</a>} | {error, Error::<a href="seestar_error.md#type-error">seestar_error:error()</a>}
</code></pre>
<br />

Synchronously execute a prepared query using the specified consistency level.
Use [`seestar_result`](seestar_result.md) module functions to work with the result.

__See also:__ [perform/3](#perform-3), [prepare/2](#prepare-2).
<a name="execute_async-3"></a>

### execute_async/3 ###

`execute_async(Client, Query, Consistency) -> any()`

__See also:__ [execute_async/5](#execute_async-5).
<a name="execute_async-4"></a>

### execute_async/4 ###

`execute_async(Client, Query, Consistency, PageSize) -> any()`

__See also:__ [execute_async/5](#execute_async-5).
<a name="execute_async-5"></a>

### execute_async/5 ###


<pre><code>
execute_async(Client::pid(), Prepared_query::<a href="seestar_result.md#type-prepared_query">seestar_result:prepared_query()</a>, Values::[<a href="seestar_cqltypes.md#type-value">seestar_cqltypes:value()</a>], Consistency::<a href="seestar.md#type-consistency">seestar:consistency()</a>, PageSize::non_neg_integer() | undefined) -&gt; {ok, Result::<a href="seestar_result.md#type-result">seestar_result:result()</a>} | {error, Error::<a href="seestar_error.md#type-error">seestar_error:error()</a>}
</code></pre>
<br />

Asynchronously execute a prepared query using the specified consistency level.
Use [`seestar_result`](seestar_result.md) module functions to work with the result.

__See also:__ [prepare/2](#prepare-2).
<a name="next_page-2"></a>

### next_page/2 ###

`next_page(Client, Rows) -> any()`

Synchronously returns the next page for a previous paginated result
<a name="next_page_async-2"></a>

### next_page_async/2 ###

`next_page_async(Client, Rows) -> any()`

Asynchronously returns the next page for a previous paginated result
<a name="perform-3"></a>

### perform/3 ###

`perform(Client, Query, Consistency) -> any()`

__See also:__ [perform/5](#perform-5).
<a name="perform-4"></a>

### perform/4 ###

`perform(Client, Query, Consistency, Values) -> any()`

__See also:__ [perform/5](#perform-5).
<a name="perform-5"></a>

### perform/5 ###

`perform(Client, Query, Consistency, Values, PageSize) -> any()`

Synchoronously perform a CQL query using the specified consistency level.
Returns a result of an appropriate type (void, rows, set_keyspace, schema_change).
Use [`seestar_result`](seestar_result.md) module functions to work with the result.
<a name="perform_async-3"></a>

### perform_async/3 ###


<pre><code>
perform_async(Client::pid(), Query::<a href="#type-query">query()</a>, Consistency::<a href="seestar.md#type-consistency">seestar:consistency()</a>) -&gt; any()
</code></pre>
<br />

__See also:__ [perform_async/5](#perform_async-5).
<a name="perform_async-4"></a>

### perform_async/4 ###


<pre><code>
perform_async(Client::pid(), Query::<a href="#type-query">query()</a>, Consistency::<a href="seestar.md#type-consistency">seestar:consistency()</a>, Values::[<a href="seestar_cqltypes.md#type-value">seestar_cqltypes:value()</a>] | non_neg_integer()) -&gt; any()
</code></pre>
<br />

__See also:__ [perform_async/5](#perform_async-5).
<a name="perform_async-5"></a>

### perform_async/5 ###


<pre><code>
perform_async(Client::pid(), Query::<a href="#type-query">query()</a>, Consistency::<a href="seestar.md#type-consistency">seestar:consistency()</a>, Values::[<a href="seestar_cqltypes.md#type-value">seestar_cqltypes:value()</a>], PageSize::undefined | non_neg_integer()) -&gt; any()
</code></pre>
<br />

Asynchronously perform a CQL query using the specified consistency level.
<a name="prepare-2"></a>

### prepare/2 ###


<pre><code>
prepare(Client::pid(), Query::<a href="#type-query">query()</a>) -&gt; {ok, Result::<a href="seestar_result.md#type-prepared_result">seestar_result:prepared_result()</a>} | {error, Error::<a href="seestar_error.md#type-error">seestar_error:error()</a>}
</code></pre>
<br />

Prepare a query for later execution. The response will contain the prepared
query that you will need to pass to the execute methods

__See also:__ [execute/3](#execute-3), [execute/4](#execute-4).
<a name="start_link-2"></a>

### start_link/2 ###


<pre><code>
start_link(Host::<a href="inet.md#type-hostname">inet:hostname()</a>, Port::<a href="inet.md#type-port_number">inet:port_number()</a>) -&gt; any()
</code></pre>
<br />

Equivalent to [`start_link(Host, Post, [])`](#start_link-3).
<a name="start_link-3"></a>

### start_link/3 ###


<pre><code>
start_link(Host::<a href="inet.md#type-hostname">inet:hostname()</a>, Port::<a href="inet.md#type-port_number">inet:port_number()</a>, ClientOptions::[<a href="#type-client_option">client_option()</a>]) -&gt; any()
</code></pre>
<br />

Equivalent to [`start_link(Host, Post, ClientOptions, [])`](#start_link-4).
<a name="start_link-4"></a>

### start_link/4 ###


<pre><code>
start_link(Host::<a href="inet.md#type-hostname">inet:hostname()</a>, Port::<a href="inet.md#type-port_number">inet:port_number()</a>, ClientOptions::[<a href="#type-client_option">client_option()</a>], ConnectOptions::[<a href="#type-connect_option">connect_option()</a>]) -&gt; {ok, pid()} | {error, any()}
</code></pre>
<br />


Starts a new connection to a cassandra node on Host:Port.
By default it will connect on plain tcp. If you want to connect using ssl, pass
{ssl, [ssl_option()]} in the ConnectOptions
<a name="stop-1"></a>

### stop/1 ###


<pre><code>
stop(Client::pid()) -&gt; ok
</code></pre>
<br />

Stop the client.
Closes the socket and terminates the process normally.

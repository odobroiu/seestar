

# Module seestar_batch #
* [Data Types](#types)
* [Function Index](#index)
* [Function Details](#functions)



<a name="types"></a>

## Data Types ##




### <a name="type-batch_query">batch_query()</a> ###



<pre><code>
batch_query() = #batch_query{}
</code></pre>





### <a name="type-batch_request">batch_request()</a> ###



<pre><code>
batch_request() = #batch{}
</code></pre>


<a name="index"></a>

## Function Index ##


<table width="100%" border="1" cellspacing="0" cellpadding="2" summary="function index"><tr><td valign="top"><a href="#batch_request-3">batch_request/3</a></td><td>Return a batch request that can be sent to cassandra
Use <a href="seestar_session.md"><code>seestar_session</code></a> module to send the request.</td></tr><tr><td valign="top"><a href="#normal_query-2">normal_query/2</a></td><td>Return a normal query that can be added to batch request.</td></tr><tr><td valign="top"><a href="#prepared_query-2">prepared_query/2</a></td><td>Return a prepared query that can be added to batch request.</td></tr></table>


<a name="functions"></a>

## Function Details ##

<a name="batch_request-3"></a>

### batch_request/3 ###


<pre><code>
batch_request(Type::logged | unlogged | counter, Consistency::one | atom(), Queries::[<a href="#type-batch_query">batch_query()</a>]) -&gt; <a href="#type-batch_request">batch_request()</a>
</code></pre>
<br />

Return a batch request that can be sent to cassandra
Use [`seestar_session`](seestar_session.md) module to send the request

__See also:__ [seestar_session:batch/2](seestar_session.md#batch-2), [seestar_session:batch_async/2](seestar_session.md#batch_async-2).
<a name="normal_query-2"></a>

### normal_query/2 ###


<pre><code>
normal_query(Query::binary(), Values::[<a href="seestar_cqltypes.md#type-value">seestar_cqltypes:value()</a>]) -&gt; <a href="#type-batch_query">batch_query()</a>
</code></pre>
<br />

Return a normal query that can be added to batch request

__See also:__ [batch_request/3](#batch_request-3).
<a name="prepared_query-2"></a>

### prepared_query/2 ###


<pre><code>
prepared_query(Prepared_query::<a href="seestar_result.md#type-prepared_query">seestar_result:prepared_query()</a>, Values::[<a href="seestar_cqltypes.md#type-value">seestar_cqltypes:value()</a>]) -&gt; <a href="#type-batch_query">batch_query()</a>
</code></pre>
<br />

Return a prepared query that can be added to batch request

__See also:__ [batch_request/3](#batch_request-3).

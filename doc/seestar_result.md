

# Module seestar_result #
* [Data Types](#types)
* [Function Index](#index)
* [Function Details](#functions)



<a name="types"></a>

## Data Types ##




### <a name="type-change">change()</a> ###



<pre><code>
change() = created | updated | dropped
</code></pre>





### <a name="type-prepared_query">prepared_query()</a> ###



<pre><code>
prepared_query() = #prepared_query{}
</code></pre>





### <a name="type-prepared_result">prepared_result()</a> ###



<pre><code>
prepared_result() = #prepared{}
</code></pre>





### <a name="type-result">result()</a> ###


__abstract datatype__: `result()`




### <a name="type-rows_result">rows_result()</a> ###



<pre><code>
rows_result() = #rows{}
</code></pre>





### <a name="type-schema_change_result">schema_change_result()</a> ###



<pre><code>
schema_change_result() = #schema_change{}
</code></pre>





### <a name="type-set_keyspace_result">set_keyspace_result()</a> ###



<pre><code>
set_keyspace_result() = #set_keyspace{}
</code></pre>





### <a name="type-type">type()</a> ###



<pre><code>
type() = void | rows | set_keyspace | prepared | schema_change
</code></pre>


<a name="index"></a>

## Function Index ##


<table width="100%" border="1" cellspacing="0" cellpadding="2" summary="function index"><tr><td valign="top"><a href="#change-1">change/1</a></td><td></td></tr><tr><td valign="top"><a href="#has_more_rows-1">has_more_rows/1</a></td><td></td></tr><tr><td valign="top"><a href="#keyspace-1">keyspace/1</a></td><td></td></tr><tr><td valign="top"><a href="#names-1">names/1</a></td><td></td></tr><tr><td valign="top"><a href="#prepared_query-1">prepared_query/1</a></td><td></td></tr><tr><td valign="top"><a href="#rows-1">rows/1</a></td><td></td></tr><tr><td valign="top"><a href="#table-1">table/1</a></td><td></td></tr><tr><td valign="top"><a href="#type-1">type/1</a></td><td></td></tr><tr><td valign="top"><a href="#type-2">type/2</a></td><td></td></tr></table>


<a name="functions"></a>

## Function Details ##

<a name="change-1"></a>

### change/1 ###


<pre><code>
change(Result::<a href="#type-schema_change_result">schema_change_result()</a>) -&gt; <a href="#type-change">change()</a>
</code></pre>
<br />


<a name="has_more_rows-1"></a>

### has_more_rows/1 ###


<pre><code>
has_more_rows(Result::<a href="#type-rows_result">rows_result()</a>) -&gt; boolean()
</code></pre>
<br />


<a name="keyspace-1"></a>

### keyspace/1 ###


<pre><code>
keyspace(Result::<a href="#type-set_keyspace_result">set_keyspace_result()</a> | <a href="#type-schema_change_result">schema_change_result()</a>) -&gt; binary()
</code></pre>
<br />


<a name="names-1"></a>

### names/1 ###


<pre><code>
names(Result::<a href="#type-rows_result">rows_result()</a> | <a href="#type-prepared_result">prepared_result()</a>) -&gt; [binary()]
</code></pre>
<br />


<a name="prepared_query-1"></a>

### prepared_query/1 ###


<pre><code>
prepared_query(Result::<a href="#type-prepared_result">prepared_result()</a>) -&gt; <a href="#type-prepared_query">prepared_query()</a>
</code></pre>
<br />


<a name="rows-1"></a>

### rows/1 ###


<pre><code>
rows(Rows::<a href="#type-rows_result">rows_result()</a>) -&gt; [[<a href="seestar_cqltypes.md#type-value">seestar_cqltypes:value()</a>]]
</code></pre>
<br />


<a name="table-1"></a>

### table/1 ###


<pre><code>
table(Result::<a href="#type-schema_change_result">schema_change_result()</a>) -&gt; binary() | undefined
</code></pre>
<br />


<a name="type-1"></a>

### type/1 ###


<pre><code>
type(Result::<a href="#type-result">result()</a>) -&gt; <a href="#type-type">type()</a>
</code></pre>
<br />


<a name="type-2"></a>

### type/2 ###


<pre><code>
type(Result::<a href="#type-rows_result">rows_result()</a> | <a href="#type-prepared_result">prepared_result()</a>, Name::binary()) -&gt; <a href="seestar_cqltypes.md#type-type">seestar_cqltypes:type()</a>
</code></pre>
<br />



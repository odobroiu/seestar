

# Module seestar_cqltypes #
* [Data Types](#types)
* [Function Index](#index)
* [Function Details](#functions)



<a name="types"></a>

## Data Types ##




### <a name="type-decimal">decimal()</a> ###



<pre><code>
decimal() = {Unscaled::integer(), Scale::integer()}
</code></pre>





### <a name="type-native">native()</a> ###



<pre><code>
native() = ascii | bigint | blob | boolean | counter | decimal | double | float | int | text | timestamp | uuid | varchar | varint | timeuuid | inet
</code></pre>





### <a name="type-type">type()</a> ###



<pre><code>
type() = <a href="#type-native">native()</a> | {list | set, <a href="#type-native">native()</a>} | {map, <a href="#type-native">native()</a>, <a href="#type-native">native()</a>} | {custom, string()}
</code></pre>





### <a name="type-value">value()</a> ###



<pre><code>
value() = null | integer() | binary() | boolean() | float() | <a href="inet.md#type-ip_address">inet:ip_address()</a> | <a href="#type-decimal">decimal()</a> | list() | <a href="#type-dict_t">dict_t()</a> | <a href="#type-set_t">set_t()</a>
</code></pre>


<a name="index"></a>

## Function Index ##


<table width="100%" border="1" cellspacing="0" cellpadding="2" summary="function index"><tr><td valign="top"><a href="#encode_value_with_size-1">encode_value_with_size/1</a></td><td></td></tr></table>


<a name="functions"></a>

## Function Details ##

<a name="encode_value_with_size-1"></a>

### encode_value_with_size/1 ###


<pre><code>
encode_value_with_size(Value::{<a href="#type-type">type()</a>, <a href="#type-value">value()</a>} | <a href="#type-value">value()</a>) -&gt; binary()
</code></pre>
<br />



-ifdef(only_builtin_types).
    -type dict_t() :: dict().
    -type queue_t() :: queue().
    -type set_t() :: set().
-else.
    -type dict_t() :: dict:dict().
    -type queue_t() :: queue:queue().
    -type set_t() :: set:set().
-endif.

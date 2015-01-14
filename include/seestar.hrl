-record(prepared_query,
{
    id                  :: binary(),
    request_types       :: [seestar_cqltypes:type()],
    cached_result_meta  :: seestar_messages:metadata()
}).

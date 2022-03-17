# TODO remove 'experimental' before going live
big_query_tables = {
    'pages': 'httparchive:experimental.pages',
    'requests': 'httparchive:experimental.requests'
}

topic = 'har-gcs'
subscription = 'projects/httparchive/subscriptions/har-gcs-pipeline'

# mapping of headers to DB fields
# IF YOU CHANGE THESE YOU HAVE TO REBUILD THE REQUESTS TABLE!!!!!!!!!!!!!!!!!!!!!!!!!!
ghReqHeaders = {
    'accept': "req_accept",
    'accept-charset': "req_accept_charset",
    'accept-encoding': "req_accept_encoding",
    'accept-language': "req_accept_language",
    'connection': "req_connection",
    'host': "req_host",
    'if-modified-since': "req_if_modified_since",
    'if-none-match': "req_if_none_match",
    'referer': "req_referer",
    'user-agent': "req_user_agent"
}
ghRespHeaders = {
    'accept-ranges': "resp_accept_ranges",
    'age': "resp_age",
    'cache-control': "resp_cache_control",
    'connection': "resp_connection",
    'content-encoding': "resp_content_encoding",
    'content-language': "resp_content_language",
    'content-length': "resp_content_length",
    'content-location': "resp_content_location",
    'content-type': "resp_content_type",
    'date': "resp_date",
    'etag': "resp_etag",
    'expires': "resp_expires",
    'keep-alive': "resp_keep_alive",
    'last-modified': "resp_last_modified",
    'location': "resp_location",
    'pragma': "resp_pragma",
    'server': "resp_server",
    'transfer-encoding': "resp_transfer_encoding",
    'vary': "resp_vary",
    'via': "resp_via",
    'x-powered-by': "resp_x_powered_by"
}
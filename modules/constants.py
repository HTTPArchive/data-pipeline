import importlib.resources as pkg_resources
import json


def _get_schema(path):
    return json.loads(pkg_resources.read_text("schema", path))


# TODO remove 'experimental' before going live
BIGQUERY = {
    "datasets": {
        "summary_pages_all": "httparchive:experimental_summary_pages",
        "summary_requests_all": "httparchive:experimental_summary_requests",
        "pages_all": "httparchive:experimental_pages",
        "technologies_all": "httparchive:experimental_technologies",
        "lighthouse_all": "httparchive:experimental_lighthouse",
        "requests_all": "httparchive:experimental_requests",
        "response_bodies_all": "httparchive:experimental_response_bodies",
        "summary_pages_home": "httparchive:summary_pages",
        "summary_requests_home": "httparchive:summary_requests",
        "pages_home": "httparchive:pages",
        "technologies_home": "httparchive:technologies",
        "lighthouse_home": "httparchive:lighthouse",
        "requests_home": "httparchive:requests",
        "response_bodies_home": "httparchive:response_bodies",
    },
    "schemas": {
        "summary_pages": {"fields": _get_schema("summary_pages.json")},
        "summary_requests": {"fields": _get_schema("summary_requests.json")},
        "pages": {"fields": _get_schema("pages.json")},
        "technologies": {"fields": _get_schema("technologies.json")},
        "lighthouse": {"fields": _get_schema("lighthouse.json")},
        "requests": {"fields": _get_schema("requests.json")},
        "response_bodies": {"fields": _get_schema("response_bodies.json")},
        "all_pages": {"fields": _get_schema("all_pages.json")},
        "all_requests": {"fields": _get_schema("all_requests.json")},
    },
}

# mapping of headers to DB fields
GH_REQ_HEADERS = {
    "accept": "req_accept",
    "accept-charset": "req_accept_charset",
    "accept-encoding": "req_accept_encoding",
    "accept-language": "req_accept_language",
    "connection": "req_connection",
    "host": "req_host",
    "if-modified-since": "req_if_modified_since",
    "if-none-match": "req_if_none_match",
    "referer": "req_referer",
    "user-agent": "req_user_agent",
}
GH_RESP_HEADERS = {
    "accept-ranges": "resp_accept_ranges",
    "age": "resp_age",
    "cache-control": "resp_cache_control",
    "connection": "resp_connection",
    "content-encoding": "resp_content_encoding",
    "content-language": "resp_content_language",
    "content-length": "resp_content_length",
    "content-location": "resp_content_location",
    "content-type": "resp_content_type",
    "date": "resp_date",
    "etag": "resp_etag",
    "expires": "resp_expires",
    "keep-alive": "resp_keep_alive",
    "last-modified": "resp_last_modified",
    "location": "resp_location",
    "pragma": "resp_pragma",
    "server": "resp_server",
    "transfer-encoding": "resp_transfer_encoding",
    "vary": "resp_vary",
    "via": "resp_via",
    "x-powered-by": "resp_x_powered_by",
}

import importlib.resources as pkg_resources
import json
from enum import Enum


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
        "parsed_css_all": "httparchive:experimental_parsed_css",
        "summary_pages_home": "httparchive:summary_pages",
        "summary_requests_home": "httparchive:summary_requests",
        "pages_home": "httparchive:pages",
        "technologies_home": "httparchive:technologies",
        "lighthouse_home": "httparchive:lighthouse",
        "requests_home": "httparchive:requests",
        "response_bodies_home": "httparchive:response_bodies",
        "all_pages": "httparchive:all.pages",
        "all_requests": "httparchive:all.requests",
        "parsed_css_home": "httparchive:experimental_parsed_css",
    },
    "schemas": {
        "summary_pages": {"fields": _get_schema("summary_pages.json")},
        "summary_requests": {"fields": _get_schema("summary_requests.json")},
        "pages": {"fields": _get_schema("pages.json")},
        "technologies": {"fields": _get_schema("technologies.json")},
        "lighthouse": {"fields": _get_schema("lighthouse.json")},
        "requests": {"fields": _get_schema("requests.json")},
        "response_bodies": {"fields": _get_schema("response_bodies.json")},
        "parsed_css": {"fields": _get_schema("parsed_css.json")},
        "all_pages": {"fields": _get_schema("all_pages.json")},
        "all_requests": {"fields": _get_schema("all_requests.json")},
    },
    # See BigQuery API JobConfigurationLoad doc for additional parameters
    #   https://cloud.google.com/bigquery/docs/reference/rest/v2/Job#jobconfigurationload
    "additional_bq_parameters": {
        "all_pages": {
            'timePartitioning': {'type': 'DAY', 'field': 'date', 'requirePartitionFilter': True},
            'clustering': {'fields': ['client', 'is_root_page', 'rank']},
            'maxBadRecords': 100,
        },
        "all_requests": {
            'timePartitioning': {'type': 'DAY', 'field': 'date', 'requirePartitionFilter': True},
            'clustering': {'fields': ['client', 'is_root_page', 'is_main_document', 'type']},
            'maxBadRecords': 100,
        },
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


class MaxContentSize(Enum):
    # BigQuery can handle rows up to 100 MB when using `WriteToBigQuery.Method.FILE_LOADS`
    FILE_LOADS = 100 * 1000000
    # BigQuery can handle rows up to 10 MB when using `WriteToBigQuery.Method.STREAMING_INSERTS`
    STREAMING_INSERTS = 10 * 1000000

    # limit response bodies to 20MB
    RESPONSE_BODIES = 20 * 1000000

TECHNOLOGY_QUERIES = {
    "adoption": """
        CREATE TEMPORARY FUNCTION GET_ADOPTION(
            records ARRAY<STRUCT<
                client STRING,
                origins INT64
            >>
        ) RETURNS STRUCT<
            desktop INT64,
            mobile INT64
        > LANGUAGE js AS '''
        return Object.fromEntries(records.map(({client, origins}) => {
            return [client, origins];
        }));
        ''';

        SELECT
            STRING(DATE(date)) as date,
            app AS technology,
            rank,
            geo,
            GET_ADOPTION(ARRAY_AGG(STRUCT(
                client,
                origins
            ))) AS adoption
        FROM
            `httparchive.core_web_vitals.technologies`
        """,
    "lighthouse": """
    CREATE TEMPORARY FUNCTION GET_LIGHTHOUSE(
        records ARRAY<STRUCT<
            client STRING,
            median_lighthouse_score_accessibility NUMERIC,
            median_lighthouse_score_best_practices NUMERIC,
            median_lighthouse_score_performance NUMERIC,
            median_lighthouse_score_pwa NUMERIC,
            median_lighthouse_score_seo NUMERIC
    >>
    ) RETURNS ARRAY<STRUCT<
    name STRING,
    desktop STRUCT<
        median_score NUMERIC
    >,
    mobile STRUCT<
        median_score NUMERIC
    >
    >> LANGUAGE js AS '''
    const METRIC_MAP = {
        accessibility: 'median_lighthouse_score_accessibility',
        best_practices: 'median_lighthouse_score_best_practices',
        performance: 'median_lighthouse_score_performance',
        pwa: 'median_lighthouse_score_pwa',
        seo: 'median_lighthouse_score_seo',
    };

    // Initialize the Lighthouse map.
    const lighthouse = Object.fromEntries(Object.keys(METRIC_MAP).map(metricName => {
        return [metricName, {name: metricName}];
    }));

    // Populate each client record.
    records.forEach(record => {
        Object.entries(METRIC_MAP).forEach(([metricName, median_score]) => {
            lighthouse[metricName][record.client] = {median_score: record[median_score]};
        });
    });

    return Object.values(lighthouse);
    ''';

    SELECT
        STRING(DATE(date)) as date,
        app AS technology,
        rank,
        geo,
        GET_LIGHTHOUSE(ARRAY_AGG(STRUCT(
            client,
            median_lighthouse_score_accessibility,
            median_lighthouse_score_best_practices,
            median_lighthouse_score_performance,
            median_lighthouse_score_pwa,
            median_lighthouse_score_seo

        ))) AS lighthouse
    FROM
        `httparchive.core_web_vitals.technologies`
    """
}
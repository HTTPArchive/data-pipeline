import datetime
import json
import logging
import os
import re

import apache_beam as beam
from apache_beam.io import ReadFromPubSub, WriteToBigQuery, BigQueryDisposition
import apache_beam.io.fileio as fileio
from apache_beam.io.gcp.bigquery_tools import FileFormat, RetryStrategy
from dateutil import parser as date_parser

from modules import constants, utils


class ReadHarFiles(beam.PTransform):
    def __init__(self, subscription=None, _input=None):
        super().__init__()
        self.subscription = subscription
        self.input = _input

    def expand(self, p):
        # PubSub pipeline
        if self.subscription:
            files = (
                p
                | ReadFromPubSub(subscription=self.subscription, with_attributes=True)
                | beam.Filter(lambda e: e.attributes['objectId'].endswith('.har.gz'))
                | 'GetFileName' >> beam.Map(
                    lambda e: f"gs://{e.attributes['bucketId']}/{e.attributes['objectId']}")
                | beam.io.ReadAllFromText(with_filename=True)
            )
        # GCS pipeline
        else:
            matching = self.input if '.har.gz' in self.input else f"{self.input}/*.har.gz"

            # TODO using ReadAllFromText instead of ReadFromText[WithFilename] to avoid listing file sizes locally
            #   https://stackoverflow.com/questions/60874942/avoid-recomputing-size-of-all-cloud-storage-files-in-beam-python-sdk
            #   https://issues.apache.org/jira/browse/BEAM-9620
            #   not an issue for the java SDK
            #     https://beam.apache.org/releases/javadoc/2.37.0/org/apache/beam/sdk/io/contextualtextio/ContextualTextIO.Read.html#withHintMatchesManyFiles--
            # files = p | beam.io.ReadFromTextWithFilename(matching)

            files = (
                p
                | fileio.MatchFiles(matching)  # TODO replace with match continuously for streaming?
                     | 'ExtractPath' >> beam.Map(lambda f: f.path)
                     | beam.Reshuffle()
                | beam.io.ReadAllFromText(with_filename=True)
            )

        return files


class WriteBigQuery(beam.PTransform):
    def __init__(self, table, schema, streaming=None):
        super().__init__()
        self.table = table
        self.schema = schema
        self.streaming = streaming

    def resolve_params(self):
        if self.streaming:
            # streaming pipeline
            return {
                'create_disposition': BigQueryDisposition.CREATE_IF_NEEDED,
                'write_disposition': BigQueryDisposition.WRITE_APPEND,
                'with_auto_sharding': True,

                # parameters for STREAMING_INSERTS
                'method': WriteToBigQuery.Method.STREAMING_INSERTS,
                'ignore_unknown_columns': True,
                'insert_retry_strategy': RetryStrategy.RETRY_ON_TRANSIENT_ERROR,

                # parameters for FILE_LOADS
                # 'method': WriteToBigQuery.Method.FILE_LOADS,
                # 'triggering_frequency': 5 * 60,  # seconds
                # 'additional_bq_parameters': {'ignoreUnknownValues': True},
            }
        else:
            # batch pipeline
            return {
                'create_disposition': BigQueryDisposition.CREATE_IF_NEEDED,
                'write_disposition': BigQueryDisposition.WRITE_TRUNCATE,
                'method': WriteToBigQuery.Method.FILE_LOADS,
                'temp_file_format': FileFormat.JSON,
                'additional_bq_parameters': {'ignoreUnknownValues': True},
            }

    def expand(self, pcoll, **kwargs):
        return pcoll | WriteToBigQuery(table=self.table, schema=self.schema, **self.resolve_params())


def initialize_status_info(file_name, page):
    # file name parsing kept for backward compatibility before 2022-03-01
    dir_name, base_name = os.path.split(file_name)

    # use crawl date instead of test date, should always be the first of the month
    #  folder name contains the crawl date, file name contains the test date
    # date = utils.test_date(page, base_name)
    date = utils.crawl_date(dir_name)

    metadata = page.get('_metadata', {})

    return {
        'archive': 'All',  # only one value found when porting logic from PHP
        'label': '{dt:%b} {dt.day} {dt.year}'.format(dt=date),
        'crawlid': metadata.get('crawlid', 0),
        'wptid': page.get('testID', base_name.split('.')[0]),
        'medianRun': 1,  # only available in RAW json (median.firstview.run), not HAR json
        'pageid': metadata.get('pageid', hash(base_name)),  # hash file name for consistent id
        'rank': int(metadata['rank']) if metadata.get('rank') else None,
        'date': '{:%Y_%m_%d}'.format(date),
        'client': metadata.get('layout', utils.client_name(file_name)).lower(),
    }


class ImportHarJson(beam.DoFn):
    def process(self, element, **kwargs):
        file_name, data = element
        try:
            page, requests = self.generate_pages(file_name, data)
            yield beam.pvalue.TaggedOutput('page', page)
            yield beam.pvalue.TaggedOutput('requests', requests)
        except Exception:
            logging.exception(
                f"Unable to unpack HAR, check previous logs for detailed errors. "
                f"file_name={file_name}, element={element}"
            )

    @staticmethod
    def generate_pages(file_name, element):
        if not element:
            logging.warning("HAR file read error.")
            return None

        try:
            har = json.loads(element)
        except Exception:
            logging.warning(f"JSON decode failed for: {file_name}", exc_info=True)
            return None

        log = har['log']
        pages = log['pages']
        if len(pages) == 0:
            logging.warning(f"No pages found for: {file_name}")
            return None

        status_info = initialize_status_info(file_name, pages[0])

        # STEP 1: Create a partial "page" record so we get a pageid.
        try:
            page = utils.remove_empty_keys(ImportHarJson.import_page(pages[0], status_info).items())
            page_id = page['pageid']
        except Exception:
            logging.warning(f"import_page() failed for status_info:{status_info}", exc_info=True)
            return None

        # STEP 2: Create all the resources & associate them with the pageid.
        entries, first_url, first_html_url = ImportHarJson.import_entries(log['entries'], page_id, status_info)
        if not entries:
            logging.warning(f"import_entries() failed for status_info:{status_info}")
            return None
        else:
            # STEP 3: Go back and fill out the rest of the "page" record based on all the resources.
            agg_stats = ImportHarJson.aggregate_stats(entries, page_id, first_url, first_html_url, status_info)
            if not agg_stats:
                logging.warning(f"aggregate_stats() failed for status_info:{status_info}")
                return None
            else:
                page.update(agg_stats)

        return page, entries

    # TODO finish porting logic
    @staticmethod
    def import_entries(entries, pageid, status_info):
        requests = []
        first_url = ''
        first_html_url = ''

        for entry in entries:
            ret_request = {
                'client': status_info['client'],
                'date': status_info['date'],
                'pageid': pageid,
                'crawlid': status_info['crawlid'],
                # we use this below for expAge calculation
                'startedDateTime': utils.datetime_to_epoch(entry['startedDateTime']),
                'time': entry['time'],
                '_cdn_provider': entry.get('_cdn_provider'),
                '_gzip_save': entry.get('_gzip_save')  # amount response WOULD have been reduced if it had been gzipped
            }

            # REQUEST
            try:
                request = entry['request']
            except KeyError:
                # TODO metric for failed parsing
                logging.warning(f"Entry does not contain a request, status_info={status_info}, entry={entry}")
                continue

            url = request['url']
            request_headers, request_other_headers, request_cookie_size = utils.parse_header(
                request['headers'], constants.ghReqHeaders, cookie_key='cookie')
            ret_request.update({
                'method': request['method'],
                'httpVersion': request['httpVersion'],
                'url': url,
                'urlShort': url[:255],
                'reqHeadersSize': request.get('headersSize') if int(request.get('headersSize', 0)) > 0 else None,
                'reqBodySize': request.get('bodySize') if int(request.get('bodySize', 0)) > 0 else None,
                'reqOtherHeaders': request_other_headers,
                'reqCookieLen': request_cookie_size
            })

            # RESPONSE
            response = entry['response']
            status = response['status']
            ret_request.update({
                'status': status,
                'respHttpVersion': response['httpVersion'],
                'redirectUrl': response.get('url'),
                'respHeadersSize': response.get('headersSize') if int(response.get('headersSize', 0)) > 0 else None,
                'respBodySize': response.get('bodySize') if int(response.get('bodySize', 0)) > 0 else None,
                'respSize': response['content']['size']
            })

            # TODO revisit this logic - is this the right way to get extention, type, format from mimetype?
            #  consider using mimetypes library instead https://docs.python.org/3/library/mimetypes.html
            mime_type = response['content']['mimeType']
            ext = utils.get_ext(url)
            typ = utils.pretty_type(mime_type, ext)
            frmt = utils.get_format(typ, mime_type, ext)

            ret_request.update({
                'mimeType': mime_type.lower(),
                'ext': ext.lower(),
                'type': typ.lower(),
                'format': frmt.lower()
            })

            response_headers, response_other_headers, response_cookie_size = utils.parse_header(
                response['headers'], constants.ghRespHeaders, cookie_key='set-cookie', output_headers=request_headers)
            ret_request.update({
                'respOtherHeaders': response_other_headers,
                'respCookieLen': response_cookie_size
            })

            # calculate expAge - number of seconds before resource expires
            # CVSNO - use the new computeRequestExpAge function.
            exp_age = 0
            cc = request_headers.get('resp_cache_control')[0] if 'resp_cache_control' in request_headers else None
            if cc and ('must-revalidate' in cc or 'no-cache' in cc or 'no-store' in cc):
                # These directives dictate the response can NOT be cached.
                exp_age = 0
            elif cc and re.match(r"max-age=\d+", cc):
                try:
                    exp_age = int(re.findall(r"\d+", cc)[0])
                except Exception:
                    # TODO compare results from old and new pipeline for these errors
                    logging.warning(f"Unable to parse max-age, cc:{cc}", exc_info=True)
            elif 'resp_expires' in response_headers:
                # According to RFC 2616 ( http://www.w3.org/Protocols/rfc2616/rfc2616-sec13.html#sec13.2.4 ):
                #     freshness_lifetime = expires_value - date_value
                # If the Date: response header is present, we use that.
                # Otherwise, fall back to $startedDateTime which is based on the client so might suffer from clock skew.
                try:
                    start_date = date_parser.parse(response_headers.get('resp_date')[0]).timestamp() \
                        if 'resp_date' in response_headers \
                        else ret_request['startedDateTime']
                    end_date = date_parser.parse(response_headers['resp_expires'][0]).timestamp()
                    # TODO try regex to resolve issues parsing apache ExpiresByType Directive
                    #   https://httpd.apache.org/docs/2.4/mod/mod_expires.html#expiresbytype
                    # end_date = date_parser.parse(
                    #   re.findall(r"\d+", response_headers['resp_expires'][0])[0]).timestamp()
                    exp_age = end_date - start_date
                except Exception:
                    logging.warning(f"Could not parse dates. "
                                    f"start=(resp_date:{response_headers.get('resp_date')},"
                                    f"startedDateTime:{ret_request.get('startedDateTime')}), "
                                    f"end=(resp_expires:{response_headers.get('resp_expires')}), "
                                    f"status_info:{status_info}", exc_info=True)

            ret_request.update({
                'expAge': int(max(exp_age, 0))
            })

            # NOW add all the headers from both the request and response.
            ret_request.update({k: ", ".join(v) for k, v in request_headers.items()})

            # TODO consider doing this sooner? why process if status is bad?
            # wrap it up
            first_req = False
            first_html = False
            if not first_url:
                if (400 <= status <= 599) or 12000 <= status:
                    logging.error(
                        "ERROR($gPagesTable pageid: {}): The first request ({}}) failed with status {}}.",
                        pageid, url, status
                    )
                    return None
                # This is the first URL found associated with the page - assume it's the base URL.
                first_req = True
                first_url = url

            if not first_html_url:
                # This is the first URL found associated with the page that's HTML.
                first_html = True
                first_html_url = url

            ret_request.update({
                'firstReq': first_req,
                'firstHtml': first_html
            })

            requests.append(ret_request)

        return requests, first_url, first_html_url

    @staticmethod
    def import_page(page, status_info):
        return {
            'client': status_info['client'],
            'date': status_info['date'],
            'pageid': status_info['pageid'],
            'createDate': int(datetime.datetime.now().timestamp()),
            'startedDateTime': utils.datetime_to_epoch(page['startedDateTime']),
            'archive': status_info['archive'],
            'label': status_info['label'],
            'crawlid': status_info['crawlid'],
            # TODO confirm - it's ok to get url from page info rather than status info as originally implemented?
            'url': page['_URL'],
            'urlhash': utils.get_url_hash(page['_URL']),
            'urlShort': page['_URL'][:255],
            'TTFB': page.get('_TTFB'),
            'renderStart': page.get('_render'),
            'fullyLoaded': page.get('_fullyLoaded'),
            'visualComplete': page.get('_visualComplete'),
            'onLoad':
                page.get('_docTime')
                if page.get('_docTime') != 0
                else max(page.get('_visualComplete'), page.get('_fullyLoaded')),
            'gzipTotal': page.get('_gzip_total'),
            'gzipSavings': page.get('_gzip_savings'),
            'numDomElements': page.get('_domElements'),
            'onContentLoaded': page.get('_domContentLoadedEventStart'),
            'cdn': page.get('_base_page_cdn'),
            'SpeedIndex': page.get('_SpeedIndex'),
            'PageSpeed': page.get('_pageSpeed', {}).get('score'),
            '_connections': page.get('_connections'),
            '_adult_site': page.get('_adult_site', False),
            'avg_dom_depth': page.get('_avg_dom_depth'),
            'doctype': page.get('_doctype'),
            'document_height': page['_document_height'] if page.get('_document_height') and int(
                page['_document_height']) > 0 else 0,
            'document_width': page['_document_width'] if page.get('_document_width') and int(
                page['_document_width']) > 0 else 0,
            'localstorage_size': page['_localstorage_size'] if page.get('_localstorage_size') and int(
                page['_localstorage_size']) > 0 else 0,
            'sessionstorage_size': page['_sessionstorage_size'] if page.get('_sessionstorage_size') and int(
                page['_sessionstorage_size']) > 0 else 0,
            'meta_viewport': page.get('_meta_viewport'),
            'num_iframes': page.get('_num_iframes'),
            'num_scripts': page.get('_num_scripts'),
            'num_scripts_sync': page.get('_num_scripts_sync'),
            'num_scripts_async': page.get('_num_scripts_async'),
            'usertiming': page.get('_usertiming')
        }

    @staticmethod
    def aggregate_stats(entries, page_id, first_url, first_html_url, status_info):
        # CVSNO - move this error checking to the point before this function is called
        if not first_url:
            logging.error("ERROR($gPagesTable pageid: {}}): no first URL found.", page_id)
            return None
        if not first_html_url:
            logging.error("ERROR($gPagesTable pageid: {}): no first HTML URL found.", page_id)
            return None

        # initialize variables for counting the page's stats
        bytes_total = 0
        req_total = 0
        size = {}
        count = {}

        # This is a list of all mime types AND file formats that we care about.
        typs = ["css", "image", "script", "html", "font", "other", "audio", "video", "text", "xml", "gif", "jpg", "png",
                "webp", "svg", "ico", "flash", "swf", "mp4", "flv", "f4v"]
        # initialize the hashes
        for typ in typs:
            size[typ] = 0
            count[typ] = 0
        domains = {}
        maxage_null = max_age_0 = max_age_1 = max_age_30 = max_age_365 = max_age_more = 0
        bytes_html_doc = num_redirects = num_errors = num_glibs = num_https = num_compressed = max_domain_reqs = 0

        for entry in entries:
            url = entry['urlShort']
            pretty_type = entry['type']
            resp_size = int(entry['respSize'])
            req_total += 1
            bytes_total += resp_size
            count[pretty_type] += 1
            size[pretty_type] += resp_size

            frmt = entry.get('format')
            if frmt and pretty_type in ['image', 'video']:
                if frmt not in typs:
                    logging.warning(f"Unexpected type, found format:{frmt}, status_info:{status_info}")
                else:
                    count[frmt] += 1
                    size[frmt] += resp_size

            # count unique domains (really hostnames)
            matches = re.findall(r'http[s]*://([^/]*)', url)
            if url and matches:
                hostname = matches[0]
                if hostname not in domains:
                    domains[hostname] = 0
                else:
                    domains[hostname] += 1
            else:
                logging.error("ERROR($gPagesTable pageid: {}): No hostname found in URL: {}", page_id, url)

            # count expiration windows
            exp_age = entry.get('expAge')
            day_secs = 24 * 60 * 60
            if not exp_age:
                maxage_null += 1
            elif int(exp_age) == 0:
                max_age_0 += 1
            elif exp_age <= day_secs:
                max_age_1 += 1
            elif exp_age <= 30 * day_secs:
                max_age_30 += 1
            elif exp_age <= 365 * day_secs:
                max_age_365 += 1
            else:
                max_age_more += 1

            if entry.get('firstHtml'):
                bytes_html_doc = resp_size  # CVSNO - can we get this UNgzipped?!

            status = entry.get('status')
            if 300 <= status < 400 and status != 304:
                num_redirects += 1
            elif 400 <= status < 600:
                num_errors += 1

            if url.startswith('https://'):
                num_https += 1

            if 'googleapis.com' in entry.get('req_host', ''):
                num_glibs += 1

            if entry.get('resp_content_encoding', '') == 'gzip' or entry.get('resp_content_encoding', '') == 'deflate':
                num_compressed += 1

        for domain in domains:
            max_domain_reqs = max(max_domain_reqs, domains[domain])

        ret = {
            'reqTotal': req_total, 'bytesTotal': bytes_total,
            'reqJS': count['script'], 'bytesJS': size['script'],
            'reqImg': count['image'], 'bytesImg': size['image'],
            'reqJson': 0, 'bytesJson': 0,
        }
        for typ in typs:
            ret.update({
                'req{}'.format(typ.title()): count[typ],
                'bytes{}'.format(typ.title()): size[typ]
            })

        ret.update({
            'numDomains': len(domains),
            'maxageNull': maxage_null,
            'maxage0': max_age_0,
            'maxage1': max_age_1,
            'maxage30': max_age_30,
            'maxage365': max_age_365,
            'maxageMore': max_age_more,
            'bytesHtmlDoc': bytes_html_doc,
            'numRedirects': num_redirects,
            'numErrors': num_errors,
            'numGlibs': num_glibs,
            'numHttps': num_https,
            'numCompressed': num_compressed,
            'maxDomainReqs': max_domain_reqs,
            'wptid': status_info['wptid'],
            'wptrun': status_info['medianRun'],
            'rank': status_info['rank']
        })

        return ret

import datetime
import json
import logging
import os
import re

import apache_beam as beam
from apache_beam.io import ReadFromPubSub
from apache_beam.transforms.combiners import Sample
from dateutil import parser as date_parser

from modules import constants, utils


# helper for testing
class SampleFiles(beam.PTransform):
    def __init__(self, sample_size, timeout=1*60):
        super().__init__()
        self.sample_size = sample_size
        self.timeout = timeout

    def expand(self, input_or_inputs):
        return (input_or_inputs
                # | WindowInto(
                #     FixedWindows(self.timeout),
                #     trigger=Repeatedly(AfterAny(AfterCount(self.sample_size), AfterProcessingTime(self.timeout))),
                #     accumulation_mode=AccumulationMode.DISCARDING)
                | Sample.FixedSizeGlobally(self.sample_size)
                | beam.FlatMap(lambda e: e))


class ReadFiles(beam.PTransform):
    def __init__(self, streaming, subscription, _input):
        super().__init__()
        self.streaming = streaming
        self.subscription = subscription
        self.input = _input

    def expand(self, p):
        # streaming pipeline
        if self.streaming:
            files = (p
                     | ReadFromPubSub(subscription=self.subscription, with_attributes=True)
                     | 'GetFileName' >> beam.Map(
                        lambda e: f"gs://{e.attributes['bucketId']}/{e.attributes['objectId']}")
                     | 'ReadFile' >> beam.io.ReadAllFromText(with_filename=True))
        # batch pipeline
        else:
            files = p | 'ReadFile' >> beam.io.ReadFromTextWithFilename(self.input)

        return files


def initialize_status_info(file_name, page):
    # file name parsing kept for backward compatibility before 2022-03-01
    dir_name, base_name = os.path.split(file_name)

    date = datetime.datetime.strptime(
        page['testID'][:6] if page.get('testID') else base_name[:6],
        '%y%m%d')

    metadata = page.get('_metadata', {})

    return {
            'archive': 'All',  # only one value found when porting logic from PHP
            'label': '{dt:%b} {dt.day} {dt.year}'.format(dt=date),
            'crawlid': metadata.get('crawlid', 0),
            'wptid': page.get('testID', base_name.split('.')[0]),
            'medianRun': 1,  # TODO from rick - median.firstview.run
            'pageid': metadata.get('pageid', hash(base_name)),  # hash file name for consistent id
            'rank': int(metadata.get('rank', 0)),
            'date': '{:%Y_%m_%d}'.format(date),
            'client': utils.client_name(metadata.get('layout', dir_name.split('/')[-1].split('-')[0])),
        }


class ImportHarJson(beam.DoFn):
    def process(self, element, **kwargs):
        file_name, data = element
        page, requests = self.generate_pages(file_name, data)
        yield beam.pvalue.TaggedOutput('page', page)
        yield beam.pvalue.TaggedOutput('requests', requests)

    @staticmethod
    def generate_pages(file_name, element):
        if not element:
            utils.log_exeption_and_raise("HAR file read error.")

        try:
            har = json.loads(element)
        except Exception as exp:
            utils.log_exeption_and_raise("JSON decode failed", exp)

        log = har['log']
        pages = log['pages']
        if len(pages) == 0:
            utils.log_exeption_and_raise("No pages found")

        status_info = initialize_status_info(file_name, pages[0])

        # STEP 1: Create a partial "page" record so we get a pageid.
        page = utils.remove_empty_keys(ImportHarJson.import_page(pages[0], status_info).items())
        if not page:
            return
        page_id = page['pageid']

        # STEP 2: Create all the resources & associate them with the pageid.
        entries, first_url, first_html_url = ImportHarJson.import_entries(log['entries'], page_id, status_info)
        if not entries:
            utils.log_exeption_and_raise("ImportEntries failed for pageid:{}", page_id)
            return
        else:
            # STEP 3: Go back and fill out the rest of the "page" record based on all the resources.
            agg_stats = ImportHarJson.aggregate_stats(entries, page_id, first_url, first_html_url, status_info)
            if not agg_stats:
                logging.error("ERROR($gStatusTable statusid: {}): AggregateStats failed. Purging pageid {}",
                              status_info['statusid'], page_id)
                return
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
                'startedDateTime': utils.datetime_to_epoch(entry['startedDateTime']),  # we use this below for expAge calculation
                'time': entry['time'],
                '_cdn_provider': entry.get('_cdn_provider'),
                '_gzip_save': entry.get('_gzip_save')  # amount response WOULD have been reduced if it had been gzipped
            }

            # REQUEST
            request = entry['request']
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
            elif cc and 'max-age=' in cc:
                exp_age = int(re.findall(r"\d+", cc)[0])
            elif 'resp_expires' in response_headers:
                # According to RFC 2616 ( http://www.w3.org/Protocols/rfc2616/rfc2616-sec13.html#sec13.2.4 ):
                #     freshness_lifetime = expires_value - date_value
                # If the Date: response header is present, we use that.
                # Otherwise we fall back to $startedDateTime which is based on the client so might suffer from clock skew.
                start_date = response_headers.get('resp_date')[0] \
                    if 'resp_date' in response_headers \
                    else ret_request['startedDateTime']
                end_date = response_headers['resp_expires'][0]
                try:
                    exp_age = (date_parser.parse(end_date) - date_parser.parse(start_date)).total_seconds()
                except Exception:
                    logging.exception("Could not parse dates start={}, end={}", start_date, end_date, exc_info=True)

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
                    logging.error("ERROR($gPagesTable pageid: {}): The first request ({}}) failed with status {}}.",
                                  pageid, url, status)
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
        if not (status_info.get('label') and status_info.get('archive')):
            logging.exception("'label' or 'crawlid' was null in import_page")
            return None

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
            'onLoad': page.get('_docTime') if page.get('_docTime') != 0 else max(page.get('_visualComplete'), page.get('_fullyLoaded')),
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
            'document_height': page.get('_document_height') if int(page.get('_document_height', 0)) > 0 else 0,
            'document_width': page.get('_document_width') if int(page.get('_document_width', 0)) > 0 else 0,
            'localstorage_size': page.get('_localstorage_size') if int(page.get('_localstorage_size', 0)) > 0 else 0,
            'sessionstorage_size': page.get('_sessionstorage_size') if int(
                page.get('_sessionstorage_size', 0)) > 0 else 0,
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
            return
        if not first_html_url:
            logging.error("ERROR($gPagesTable pageid: {}): no first HTML URL found.", page_id)
            return

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

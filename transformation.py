import datetime
import hashlib
import json
import logging
import re
import uuid

import apache_beam as beam
from dateutil import parser as date_parser

import constants
import utils


class TestLogging(beam.DoFn):
    def process(self, element, *args, **kwargs):
        self.log_and_apply(element)

    @staticmethod
    def log_and_apply(f, log=logging.info):
        log(f)
        return f


class ImportHarJson(beam.DoFn):
    def process(self, element):
        page, requests = self.generate_pages(element)
        yield beam.pvalue.TaggedOutput('page', page)
        yield beam.pvalue.TaggedOutput('requests', requests)

    @staticmethod
    def generate_pages(element):
        # TODO: status table,info,id?
        status_info = {
            'archive': 'TODO',
            'label': 'TODO',
            'crawlid': 'TODO',
            'url': 'https://google.com'
        }

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

        # STEP 1: Create a partial "page" record so we get a pageid.
        page = ImportHarJson.remove_empty_keys(ImportHarJson.import_page(pages[0], status_info).items())
        # TODO revisit this ported logic
        # if not page:
        #     return None

        # STEP 2: Create all the resources & associate them with the pageid.
        entries = ImportHarJson.import_entries(log['entries'], page['pageid'], status_info)
        if not entries:
            # TODO raise exception or log error?
            utils.log_exeption_and_raise("ImportEntries failed. Purging pageid $pageid")

        # else:
        # TODO 	// STEP 3: Go back and fill out the rest of the "page" record based on all the resources.
        # 	$bAgg = aggregateStats($pageid, $firstUrl, $firstHtmlUrl, $statusInfo);
        # 	t_aggregate('AggregateStats');
        # 	if ( false === $bAgg ) {
        # 		dprint("ERROR($gStatusTable statusid: $statusInfo[statusid]): AggregateStats failed. Purging pageid $pageid");
        # 		purgePage($pageid);
        # 	}
        # 	else {
        # 		return true;
        # 	}
        # }
        #

        return page, entries

    # TODO finish porting logic
    @staticmethod
    def import_entries(entries, pageid, status_info=None, first_url='', first_html_url=''):
        # TODO remove this default assignment, only used for prototyping
        if status_info is None:
            status_info = {}

        requests = []

        for entry in entries:
            ret_request = {
                'pageid': pageid,
                'crawlid': status_info['crawlid'],
                'startedDateTime': entry['startedDateTime'],  # we use this below for expAge calculation
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
                'reqHeadersSize': request.get('headersSize') if 0 < request.get('headersSize') else None,
                'reqBodySize': request.get('bodySize') if 0 < request.get('bodySize') else None,
                'reqOtherHeaders': request_other_headers,
                'reqCookieLen': request_cookie_size
            })


            # RESPONSE
            response = entry['response']
            status = response['status']
            ret_request.update({
                'status': status,
                'respHttpVersion': response['httpVersion'],
                'url': response.get('url'),
                'respHeadersSize': response.get('headersSize') if 0 < response.get('headersSize', 0) else None,
                'respBodySize': response.get('bodySize') if 0 < response.get('bodySize', 0) else None,
                'respSize': response['content']['size']
            })

            # TODO revisit this logic - is this the right way to get extention, type, format from mimetype?
            #  consider using mimetypes library instead https://docs.python.org/3/library/mimetypes.html
            mime_type = response['content']['mimeType']
            ext = utils.get_ext(url)
            typ = utils.pretty_type(mime_type, ext)
            frmt = utils.get_format(typ, mime_type, ext)

            ret_request.update({
                'mimeType': mime_type,
                'ext': ext,
                'type': typ,
                'format': frmt
            })

            response_headers, response_other_headers, response_cookie_size = utils.parse_header(
                response['headers'], constants.ghRespHeaders, cookie_key='set-cookie', output_headers=request_headers)
            ret_request.update({
                # 'respOtherHeaders': response_headers,
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
            elif 'resp_expires' in response_headers:  # TODO breaking when collection = ['0']
                # According to RFC 2616 ( http://www.w3.org/Protocols/rfc2616/rfc2616-sec13.html#sec13.2.4 ):
                #     freshness_lifetime = expires_value - date_value
                # If the Date: response header is present, we use that.
                # Otherwise we fall back to $startedDateTime which is based on the client so might suffer from clock skew.
                start_date = response_headers.get('resp_date')[0] if 'resp_date' in response_headers else ret_request['startedDateTime']
                end_date = response_headers['resp_expires'][0]
                try:
                    exp_age = (date_parser.parse(end_date) - date_parser.parse(start_date)).total_seconds()
                except Exception:
                    logging.exception("Could not parse dates start={}, end={}".format(start_date, end_date), exc_info=True)

            ret_request.update({
                'expAge': max(exp_age, 0)
            })

    # 		// NOW add all the headers from both the request and response.
    # 		$aHeaders = array_keys($hHeaders);
    # 		for ( $h = 0; $h < count($aHeaders); $h++ ) {
    # 			$header = $aHeaders[$h];
    # 			array_push($aTuples, "$header = '" . mysqli_real_escape_string($link, $hHeaders[$header]) . "'");
    # 		}
    #
    # 		// CUSTOM RULES
    # 		if ( array_key_exists('_custom_rules', $entry) ) {
    # 			$customrules = $entry->{'_custom_rules'};
    # 			if ( array_key_exists('ModPageSpeed', $customrules) ) {
    # 				$count = $customrules->{'ModPageSpeed'}->{'count'};
    # 				// TODO array_push($aTuples, "reqBodySize = $count");
    # 			}
    # 		}
    #
    # 		// wrap it up
    # 		$bFirstReq = 0;
    # 		$bFirstHtml = 0;
    # 		if ( ! $firstUrl ) {
    # 			if ( (400 <= $status && 599 >= $status) || (12000 <= $status) ) {
    # 				dprint("ERROR($gPagesTable pageid: $pageid): The first request ($url) failed with status $status.");
    # 				return false;
    # 			}
    # 			// This is the first URL found associated with the page - assume it's the base URL.
    # 			$bFirstReq = 1;
    # 			$firstUrl = $url;
    # 		}
    # 		if ( ! $firstHtmlUrl && 200 == $status && $type == 'html' ) {
    # 			// This is the first URL found associated with the page that's HTML.
    # 			$bFirstHtml = 1;
    # 			$firstHtmlUrl = $url;
    # 		}
    # 		array_push($aTuples, "firstReq = $bFirstReq");
    # 		array_push($aTuples, "firstHtml = $bFirstHtml");
    #
    # 		$cmd = "REPLACE INTO $gRequestsTable SET " . implode(", ", $aTuples) . ";";
    # 		doSimpleCommand($cmd);
    # 	}
    # }
            requests.append(ret_request)

        return requests


    # TODO finish porting logic
    @staticmethod
    def import_page(page, status_info):
        if not (status_info['label'] or status_info['archive']):
            logging.exception("'label' or 'crawlid' was null in import_page")
            return None

        return {
            'pageid': uuid.uuid4().int,  # TODO placeholder
            'createDate': int(datetime.datetime.now().timestamp()),
            'startedDateTime': page['startedDateTime'],
            'archive': status_info['archive'],
            'label': status_info['label'],
            'crawlid': status_info['crawlid'],
            'url': status_info['url'],
            'urlhash': ImportHarJson.get_url_hash(status_info['url']),
            'urlshort': status_info['url'][0:255],
            '_TTFB': page.get('_TTFB'),
            'renderStart': page.get('_render'),
            'fullyLoaded': page.get('_fullyLoaded'),
            'visualComplete': page.get('_visualComplete'),
            'onLoad': max(page.get('_docTime'), page.get('_visualComplete'), page.get('_fullyLoaded')),
            'gzipTotal': page.get('_gzip_total'),
            'gzipSavings': page.get('_gzip_savings'),
            'numDomElements': page.get('_domElements'),
            'onContentLoaded': page.get('_domContentLoadedEventStart'),
            'cdn': page.get('_base_page_cdn'),
            'SpeedIndex': page.get('_SpeedIndex'),
            'PageSpeed': page.get('_pageSpeed', {}).get('score'),
            '_connections': page.get('_connections'),
            '_adult_site': page.get('_adult_site'),
            'avg_dom_depth': page.get('_avg_dom_depth'),
            'doctype': page.get('_doctype'),
            'document_height': page.get('_document_height') if 0 < int(page.get('_document_height', 0)) else None,
            'document_width': page.get('_document_width') if 0 < int(page.get('_document_width', 0)) else None,
            'localstorage_size': page.get('_localstorage_size') if 0 < int(page.get('_localstorage_size', 0)) else None,
            'sessionstorage_size': page.get('_sessionstorage_size') if 0 < int(page.get('_sessionstorage_size', 0)) else None,
            'meta_viewport': page.get('_meta_viewport'),
            'num_iframes': page.get('_num_iframes'),
            'num_scripts': page.get('_num_scripts'),
            'num_scripts_sync': page.get('_num_scripts_sync'),
            'num_scripts_async': page.get('_num_scripts_async'),
            'usertiming': page.get('_usertiming')
        }

    @staticmethod
    def get_url_hash(url):
        return int(hashlib.md5(url.encode()).hexdigest()[0:4], 16)

    @staticmethod
    # note: check values instead of keys (i.e. original implementation)
    def get_if_keys_exist(dictionary, check_keys, return_key):
        if all(value in dictionary for value in dictionary[check_keys]):
            return dictionary.get(return_key)
        else:
            return None

    @staticmethod
    def remove_empty_keys(d):
        return {k: v for k, v in d if v is not None}

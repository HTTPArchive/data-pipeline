import datetime
import hashlib
import json
import logging
import uuid

import apache_beam as beam

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
        return [self.generate_pages(element)]

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
            logging.exception("HAR file read error.")

        har = json.loads(element)
        if not har:
            logging.exception("JSON decode failed")

        log = har['log']
        pages = log['pages']
        if len(pages) == 0:
            logging.exception("No pages found")

        # STEP 1: Create a partial "page" record so we get a pageid.
        # page = {k: v for k, v in ImportHarJson.import_page(pages[0], status_info).items() if v is not None}
        page = ImportHarJson.remove_empty_keys(ImportHarJson.import_page(pages[0], status_info).items())
        # TODO revisit this ported logic
        # if not page:
        #     return None

        # STEP 2: Create all the resources & associate them with the pageid.
        entries = ImportHarJson.import_entries(log['entries'], page['pageid'], status_info)
        if not entries:
            # TODO exception or error?
            logging.exception("ImportEntries failed. Purging pageid $pageid")

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

        return page

    # TODO finish porting logic
    @staticmethod
    def import_entries(entries, pageid, status_info=None, first_url='', first_html_url=''):
        # TODO remove this default assignment, only used for prototyping
        if status_info is None:
            status_info = {}

        entries_transformed = []

        for entry in entries:
            ret = {
                'pageid': pageid,
                'crawlid': status_info['crawlid'],
                'startedDateTime': entry['startedDateTime'],  # we use this below for expAge calculation
                'time': entry['time'],
                '_cdn_provider': entry.get('_cdn_provider'),
                '_gzip_save': entry.get('_gzip_save')  # amount response WOULD have been reduced if it had been gzipped
            }

            # REQUEST
            request = entry['request']
            ret.update({
                'method': request['method'],
                'httpVersion': request['httpVersion']
            })

            ret.update({
                'url': request['url'],
                'urlShort': request['url'][0:255],
                'reqHeadersSize': request.get('headersSize') if 0 < request.get('headersSize') else None,
                'reqBodySize': request.get('bodySize') if 0 < request.get('bodySize') else None
            })

            headers = request['headers']
            other = []
            hHeaders = {}  # Headers can appear multiple times, so we have to concat them all then add them to avoid setting a column twice.
            cookielen = 0
            for header in headers:
                name = header['name']
                lc_name = name.lower()
                value = header['value'][0:255]
                orig_value = header['value']
                if lc_name in utils.ghReqHeaders.keys():
                    # This is one of the standard headers we want to save
                    column = utils.ghReqHeaders[lc_name]
                    if hHeaders.get(column):
                        hHeaders[column].append(value)
                    else:
                        hHeaders[column] = [value]
                elif 'cookie' == lc_name:
                    # We don't save the Cookie header, just the size.
                    cookielen += len(orig_value)
                else:
                    other.append("{} = {}".format(name, orig_value))

            ret.update({
                'reqOtherHeaders': ", ".join(other),
                'reqCookieLen': cookielen
            })

            entries_transformed.append(ret)

    # 		// RESPONSE
    # 		$response = $entry->{ 'response' };
    # 		$status = $response->{ 'status' };
    # 		array_push($aTuples, "status = $status");
    # 		array_push($aTuples, "respHttpVersion = '" . $response->{ 'httpVersion' } . "'");
    # 		if ( property_exists($response, 'url') && null !== $response->{'url'} ) {
    # 			array_push($aTuples, "redirectUrl = '" . mysqli_real_escape_string($link, $response->{ 'url' }) . "'");
    # 		}
    # 		$respHeadersSize = $response->{ 'headersSize' };
    # 		if ( $respHeadersSize && 0 < $respHeadersSize ) {
    # 			array_push($aTuples, "respHeadersSize = $respHeadersSize");
    # 		}
    # 		$respBodySize = $response->{ 'bodySize' };
    # 		if ( $respBodySize && 0 < $respBodySize ) {
    # 			array_push($aTuples, "respBodySize = $respBodySize");
    # 		}
    # 		$content = $response->{ 'content' };
    # 		array_push($aTuples, "respSize = " . $content->{ 'size' });
    # 		$mimeType = mysqli_real_escape_string($link, $content->{ 'mimeType' });
    # 		array_push($aTuples, "mimeType = '$mimeType'");
    # 		$ext = mysqli_real_escape_string($link, getExt($url));
    # 		array_push($aTuples, "ext = '$ext'");
    # 		$type = prettyType($mimeType, $ext);
    # 		array_push($aTuples, "type = '$type'");
    # 		$format = getFormat($type, $mimeType, $ext);
    # 		array_push($aTuples, "format = '$format'");
    # 		$headers = $response->{ 'headers' };
    # 		$other = "";
    # 		$cookielen = 0;
    # 		for ( $h = 0; $h < count($headers); $h++ ) {
    # 			$header = $headers[$h];
    # 			$name = $header->{ 'name' };
    # 			$lcname = strtolower($name);
    # 			$value = substr($header->{ 'value' }, 0, 255);
    # 			$origValue = $header->{ 'value' };
    # 			if ( array_key_exists($lcname, $ghRespHeaders) ) {
    # 				// This is one of the standard headers we want to save.
    # 				$column = $ghRespHeaders[$lcname];
    # 				$hHeaders[$column] = ( array_key_exists($column, $hHeaders) ? $hHeaders[$column] . ", $value" : $value );
    # 			}
    # 			else if ( "set-cookie" == $lcname ) {
    # 				// We don't save the Set-Cookie header, just the size.
    # 				$cookielen += strlen($origValue);
    # 			}
    # 			else {
    # 				// All other headers are lumped together.
    # 				$other .= ( $other ? ", " : "" ) . "$name = $origValue";
    # 			}
    # 		}
    # 		if ( $other ) {
    # 			array_push($aTuples, "respOtherHeaders = '" . mysqli_real_escape_string($link, $other) . "'");
    # 		}
    # 		if ( $cookielen ) {
    # 			array_push($aTuples, "respCookieLen = $cookielen");
    # 		}
    # 		// calculate expAge - number of seconds before resource expires
    # 		// CVSNO - use the new computeRequestExpAge function.
    # 		$expAge = 0;
    # 		$cc = ( array_key_exists('resp_cache_control', $hHeaders) ? $hHeaders['resp_cache_control'] : null );
    # 		if ( $cc &&
    # 			 ( FALSE !== stripos($cc, "must-revalidate") || FALSE !== stripos($cc, "no-cache") || FALSE !== stripos($cc, "no-store") ) ) {
    # 			// These directives dictate the response can NOT be cached.
    # 			$expAge = 0;
    # 		}
    # 		else if ( $cc  && FALSE !== ($posMaxage = stripos($cc, "max-age=")) ) {
    # 			$expAge = intval(substr($cc, $posMaxage+8));
    # 		}
    # 		else if ( array_key_exists('resp_expires', $hHeaders) ) {
    # 			// According to RFC 2616 ( http://www.w3.org/Protocols/rfc2616/rfc2616-sec13.html#sec13.2.4 ):
    # 			//     freshness_lifetime = expires_value - date_value
    # 			// If the Date: response header is present, we use that.
    # 			// Otherwise we fall back to $startedDateTime which is based on the client so might suffer from clock skew.
    # 			$startEpoch = ( array_key_exists('resp_date', $hHeaders) ? strtotime($hHeaders['resp_date']) : $startedDateTime );
    # 			$expAge = strtotime($hHeaders['resp_expires']) - $startEpoch;
    # 		}
    # 		array_push($aTuples, "expAge = " . ( $expAge < 0 ? 0 : $expAge ));
    #
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
        return entries_transformed


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

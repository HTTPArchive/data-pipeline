import datetime
import json
import logging

import apache_beam as beam


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
        if not element:
            logging.exception("HAR file read error.")

        har = json.loads(element)
        if not har:
            logging.exception("JSON decode failed")

        log = har['log']
        pages = log['pages']
        if len(pages) == 0:
            logging.exception("No pages found")

        page = {k: v for k, v in ImportHarJson.import_page(pages[0]).items() if v is not None}
        # TODO revisit this ported logic
        # if not page:
        #     return None

        entries = log['entries']
        # STEP 2: Create all the resources & associate them with the pageid.
        # $firstUrl = "";
        # $firstHtmlUrl = "";
        bEntries = ImportHarJson.import_entries(entries)
        if not bEntries:
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
    def import_entries(entries, first_url='', first_html_url=''):
        return entries

    # TODO finish porting logic
    @staticmethod
    def import_page(page):
        return {
            'createDate': datetime.datetime.now().timestamp(),
            'startedDateTime': page['startedDateTime'],
            'archive': 'TODO',
            'label': 'TODO',
            'crawlid': 'TODO',
            'url': 'TODO',
            'urlhash': 'TODO',
            'urlshort': 'TODO',
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
            'document_height': page.get('_document_height'),
            'document_width': page.get('_document_width'),
            'localstorage_size': page.get('_localstorage_size'),
            'sessionstorage_size': page.get('_sessionstorage_size'),
            'meta_viewport': page.get('_meta_viewport'),
            'num_iframes': page.get('_num_iframes'),
            'num_scripts': page.get('_num_scripts'),
            'num_scripts_sync': page.get('_num_scripts_sync'),
            'num_scripts_async': page.get('_num_scripts_async'),
            'usertiming': page.get('_usertiming')
        }


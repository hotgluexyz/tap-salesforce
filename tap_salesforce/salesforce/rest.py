# pylint: disable=protected-access
import singer
import singer.utils as singer_utils
from requests.exceptions import HTTPError
from tap_salesforce.salesforce import Salesforce
from tap_salesforce.salesforce.exceptions import TapSalesforceException

LOGGER = singer.get_logger()

MAX_RETRIES = 4

class Rest():

    def __init__(self, sf: Salesforce):
        self.sf = sf

    def query(self, catalog_entry, state, query_override=None):
        start_date = self.sf.get_start_date(state, catalog_entry)
        query = self.sf._build_query_string(catalog_entry, start_date) if query_override is None else query_override

        return self._query_recur(query, catalog_entry, start_date)

    # pylint: disable=too-many-arguments
    def _query_recur(
            self,
            query,
            catalog_entry,
            start_date_str,
            end_date=None,
            retries=MAX_RETRIES):
        params = {"q": query}
        url = "{}/services/data/v{}/queryAll".format(self.sf.instance_url, self.sf.version)
        headers = self.sf._get_standard_headers()

        sync_start = singer_utils.now()
        if end_date is None:
            end_date = sync_start

        if retries == 0:
            raise TapSalesforceException(
                "Ran out of retries attempting to query Salesforce Object {}".format(
                    catalog_entry['stream']))

        retryable = False
        try:
            for rec in self._sync_records(url, headers, catalog_entry, params):
                yield rec

            # If the date range was chunked (an end_date was passed), sync
            # from the end_date -> now
            if end_date < sync_start:
                next_start_date_str = singer_utils.strftime(end_date)
                query = self.sf._build_query_string(catalog_entry, next_start_date_str)
                for record in self._query_recur(
                        query,
                        catalog_entry,
                        next_start_date_str,
                        retries=retries):
                    yield record

        except HTTPError as ex:
            response = ex.response.json()
            if isinstance(response, list) and response[0].get("errorCode") == "QUERY_TIMEOUT":
                start_date = singer_utils.strptime_with_tz(start_date_str)
                day_range = (end_date - start_date).days
                LOGGER.info(
                    "Salesforce returned QUERY_TIMEOUT querying %d days of %s",
                    day_range,
                    catalog_entry['stream'])
                retryable = True
            else:
                raise ex

        if not retryable:
            LOGGER.info("[Rest] Not retrying: Stream:%s - Query:%s", catalog_entry['stream'], query)
            return

        start_date = singer_utils.strptime_with_tz(start_date_str)
        half_day_range = (end_date - start_date) // 2
        end_date = end_date - half_day_range

        if half_day_range.days == 0:
            raise TapSalesforceException(
                "Attempting to query by 0 day range, this would cause infinite looping.")

        query = self.sf._build_query_string(catalog_entry, singer_utils.strftime(start_date),
                                            singer_utils.strftime(end_date))
        LOGGER.info("[Rest] Retrying: Stream: %s - Query: %s", catalog_entry['stream'], query)
        for record in self._query_recur(
                query,
                catalog_entry,
                start_date_str,
                end_date,
                retries - 1):
            yield record

    def _sync_records(self, url, headers, catalog_entry, params):
        while True:
            LOGGER.info("[Rest] Fetching records from: Stream: %s - URL: %s", catalog_entry['stream'], url)
            resp = self.sf._make_request('GET', url, headers=headers, params=params, validate_json=True)
            resp_json = resp.json()

            for rec in resp_json.get('records'):
                yield rec

            next_records_url = resp_json.get('nextRecordsUrl')

            if next_records_url is None:
                LOGGER.info("[Rest] No more records to fetch")
                break

            url = "{}{}".format(self.sf.instance_url, next_records_url)

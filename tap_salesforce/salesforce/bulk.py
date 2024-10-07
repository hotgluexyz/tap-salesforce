# pylint: disable=protected-access
import csv
import json
import sys
import time
import tempfile
import singer
from singer import metrics
import requests
from requests.exceptions import RequestException
from concurrent.futures import ThreadPoolExecutor

import xmltodict

from tap_salesforce.salesforce.exceptions import (
    TapSalesforceException, TapSalesforceQuotaExceededException)

BATCH_STATUS_POLLING_SLEEP = 20
PK_CHUNKED_BATCH_STATUS_POLLING_SLEEP = 60
ITER_CHUNK_SIZE = 1024
DEFAULT_CHUNK_SIZE = 250000 # Max is 250000
FALLBACK_CHUNK_SIZE = 2000

LOGGER = singer.get_logger()

# pylint: disable=inconsistent-return-statements
def find_parent(stream):
    parent_stream = stream
    if stream.endswith("CleanInfo"):
        parent_stream = stream[:stream.find("CleanInfo")]
    elif stream.endswith("FieldHistory"):
        parent_stream = stream[:stream.find("FieldHistory")]
    elif stream.endswith("History"):
        parent_stream = stream[:stream.find("History")]

    # If the stripped stream ends with "__" we can assume the parent is a custom table
    if parent_stream.endswith("__"):
        parent_stream += 'c'

    return parent_stream


class Bulk():

    @property
    def bulk_url(self):
        return "{}/services/async/" + self.sf.version + "/{}"

    def __init__(self, sf):
        # Set csv max reading size to the platform's max size available.
        csv.field_size_limit(sys.maxsize)
        self.sf = sf
        self.closed_jobs = []

    def has_permissions(self):
        try:
            self.check_bulk_quota_usage()
        except requests.exceptions.HTTPError as err:
            if err.response is not None:
                for error_response_item in err.response.json():
                    if error_response_item.get('errorCode') == 'API_DISABLED_FOR_ORG':
                        return False
        return True

    def query(self, catalog_entry, state):
        self.check_bulk_quota_usage()

        for record in self._bulk_query(catalog_entry, state):
            yield record

        self.sf.jobs_completed += 1

    # pylint: disable=line-too-long
    def check_bulk_quota_usage(self):
        endpoint = "limits"
        url = self.sf.data_url.format(self.sf.instance_url, endpoint)

        with metrics.http_request_timer(endpoint):
            resp = self.sf._make_request('GET', url, headers=self.sf._get_standard_headers()).json()

        quota = resp.get('DailyBulkApiRequests')
        if not quota:
            quota = resp.get('DailyBulkApiBatches')
        quota_max = quota['Max']
        
        max_requests_for_run = int((self.sf.quota_percent_per_run * quota_max) / 100)

        quota_remaining = quota['Remaining']
        percent_used = (1 - (quota_remaining / quota_max)) * 100

        if percent_used > self.sf.quota_percent_total:
            total_message = ("Salesforce has reported {}/{} ({:3.2f}%) total Bulk API quota " +
                             "used across all Salesforce Applications. Terminating " +
                             "replication to not continue past configured percentage " +
                             "of {}% total quota.").format(quota_max - quota_remaining,
                                                           quota_max,
                                                           percent_used,
                                                           self.sf.quota_percent_total)
            raise TapSalesforceQuotaExceededException(total_message)
        elif self.sf.jobs_completed > max_requests_for_run:
            partial_message = ("This replication job has completed {} Bulk API jobs ({:3.2f}% of " +
                               "total quota). Terminating replication due to allotted " +
                               "quota of {}% per replication.").format(self.sf.jobs_completed,
                                                                       (self.sf.jobs_completed / quota_max) * 100,
                                                                       self.sf.quota_percent_per_run)
            raise TapSalesforceQuotaExceededException(partial_message)

    def _get_bulk_headers(self):
        return {"X-SFDC-Session": self.sf.access_token,
                "Content-Type": "application/json"}

    def _can_pk_chunk_job(self, failure_message): # pylint: disable=no-self-use
        return "QUERY_TIMEOUT" in failure_message or \
               "Retried more than 15 times" in failure_message or \
               "Failed to write query result" in failure_message

    def try_bulking_with_pk_chunking(self, catalog_entry, state, use_fall_back_chunk_size=False):
        start_date = self.sf.get_start_date(state, catalog_entry)
        batch_status = self._bulk_query_with_pk_chunking(catalog_entry, start_date, use_fall_back_chunk_size)
        job_id = batch_status['job_id']
        self.sf.pk_chunking = True
        # Write job ID and batch state for resumption
        tap_stream_id = catalog_entry['tap_stream_id']
        self.tap_stream_id = tap_stream_id
        state = singer.write_bookmark(state, tap_stream_id, 'JobID', job_id)
        state = singer.write_bookmark(state, tap_stream_id, 'BatchIDs', batch_status['completed'][:])

        # Parallelize the batch result processing
        with ThreadPoolExecutor() as executor:
            futures = [
                executor.submit(self.process_batch, job_id, completed_batch_id, catalog_entry, state)
                for completed_batch_id in batch_status['completed']
            ]

            # Process the results as they complete
            for future in futures:
                for result in future.result():
                    yield result

    def _bulk_query(self, catalog_entry, state):
        try:
            yield from self.try_bulking_with_pk_chunking(catalog_entry, state)
        except Exception as e:
            try:
                yield from self.try_bulking_with_pk_chunking(catalog_entry, state, True)
            except Exception as e:
                if job_id in self.closed_jobs:
                    LOGGER.info(f"Another batch failed before. Ignoring this new job...")
                    pass
                LOGGER.info(f"PK Chunking failled on job {job_id}. Trying without it...")
                self._close_job(job_id)
                
                if hasattr(self,"tap_stream_id"):
                    with open("streams_pk_chunking_failing.txt", "a") as file:
                        file.write(self.tap_stream_id + "\n")  # Append data with a newline character

                job_id = self._create_job(catalog_entry)
                start_date = self.sf.get_start_date(state, catalog_entry)
                self.sf.pk_chunking = False

                batch_id = self._add_batch(catalog_entry, job_id, start_date)

                self._close_job(job_id)

                batch_status = self._poll_on_batch_status(job_id, batch_id)
                if batch_status['state'] == 'Failed':
                    raise TapSalesforceException(batch_status['stateMessage'])
                else:
                    for result in self.get_batch_results(job_id, batch_id, catalog_entry):
                        yield result

    def process_batch(self, job_id, batch_id, catalog_entry, state):
        """Process a single batch and yield results."""
        for result in self.get_batch_results(job_id, batch_id, catalog_entry):
            yield result

        # Update state and log progress
        state['bookmarks'][catalog_entry['tap_stream_id']]["BatchIDs"].remove(batch_id)
        LOGGER.info("Finished syncing batch %s. Removing batch from state.", batch_id)
        LOGGER.info("Batches to go: %d", len(state['bookmarks'][catalog_entry['tap_stream_id']]["BatchIDs"]))
        singer.write_state(state)

    def _bulk_query_with_pk_chunking(self, catalog_entry, start_date, use_fall_back_chunk_size=False):
        LOGGER.info("Trying Bulk Query with PK Chunking")

        # Create a new job
        job_id = self._create_job(catalog_entry, True, use_fall_back_chunk_size)

        self._add_batch(catalog_entry, job_id, start_date, False)

        batch_status = self._poll_on_pk_chunked_batch_status(job_id)
        batch_status['job_id'] = job_id

        if batch_status['failed']:
            raise TapSalesforceException("One or more batches failed during PK chunked job")

        # Close the job after all the batches are complete
        self._close_job(job_id)

        return batch_status

    def _create_job(self, catalog_entry, pk_chunking=False, use_fall_back_chunk_size=False):
        url = self.bulk_url.format(self.sf.instance_url, "job")
        body = {"operation": "queryAll", "object": catalog_entry['stream'], "contentType": "CSV"}

        headers = self._get_bulk_headers()
        headers['Sforce-Disable-Batch-Retry'] = "true"

        if pk_chunking:
            LOGGER.info("ADDING PK CHUNKING HEADER")
            chunk_size = DEFAULT_CHUNK_SIZE if not use_fall_back_chunk_size else FALLBACK_CHUNK_SIZE
            headers['Sforce-Enable-PKChunking'] = "true; chunkSize={}".format(chunk_size)
            LOGGER.info(f"[use_fall_back_chunk_size:{use_fall_back_chunk_size}] HEADERS: {headers}")

            # If the stream ends with 'CleanInfo' or 'History', we can PK Chunk on the object's parent
            if any(catalog_entry['stream'].endswith(suffix) for suffix in ["CleanInfo", "History"]):
                parent = find_parent(catalog_entry['stream'])
                headers['Sforce-Enable-PKChunking'] = headers['Sforce-Enable-PKChunking'] + "; parent={}".format(parent)

        with metrics.http_request_timer("create_job") as timer:
            timer.tags['sobject'] = catalog_entry['stream']
            resp = self.sf._make_request(
                'POST',
                url,
                headers=headers,
                body=json.dumps(body))

        job = resp.json()

        return job['id']

    def _add_batch(self, catalog_entry, job_id, start_date, order_by_clause=True):
        endpoint = "job/{}/batch".format(job_id)
        url = self.bulk_url.format(self.sf.instance_url, endpoint)

        body = self.sf._build_query_string(catalog_entry, start_date, order_by_clause=order_by_clause)

        headers = self._get_bulk_headers()
        headers['Content-Type'] = 'text/csv'

        with metrics.http_request_timer("add_batch") as timer:
            timer.tags['sobject'] = catalog_entry['stream']
            resp = self.sf._make_request('POST', url, headers=headers, body=body)

        batch = xmltodict.parse(resp.text)

        return batch['batchInfo']['id']

    def _poll_on_pk_chunked_batch_status(self, job_id):
        batches = self._get_batches(job_id)

        while True:
            queued_batches = [b['id'] for b in batches if b['state'] == "Queued"]
            in_progress_batches = [b['id'] for b in batches if b['state'] == "InProgress"]

            if not queued_batches and not in_progress_batches:
                completed_batches = [b['id'] for b in batches if b['state'] == "Completed"]
                failed_batches = [b['id'] for b in batches if b['state'] == "Failed"]
                if len(failed_batches) > 0:
                    LOGGER.error(f"{[{b['id']:b.get('stateMessage')} for b in batches if b['state'] == 'Failed']}")
                return {'completed': completed_batches, 'failed': failed_batches}
            else:
                time.sleep(PK_CHUNKED_BATCH_STATUS_POLLING_SLEEP)
                batches = self._get_batches(job_id)

    def _poll_on_batch_status(self, job_id, batch_id):
        batch_status = self._get_batch(job_id=job_id,
                                       batch_id=batch_id)

        while batch_status['state'] not in ['Completed', 'Failed', 'Not Processed']:
            LOGGER.info(f'job_id: {job_id}, batch_id: {batch_id} - batch_status["state"]: {batch_status["state"]} - Sleeping for {BATCH_STATUS_POLLING_SLEEP} seconds...')
            time.sleep(BATCH_STATUS_POLLING_SLEEP)
            batch_status = self._get_batch(job_id=job_id,
                                           batch_id=batch_id)

        return batch_status

    def job_exists(self, job_id):
        try:
            endpoint = "job/{}".format(job_id)
            url = self.bulk_url.format(self.sf.instance_url, endpoint)
            headers = self._get_bulk_headers()

            with metrics.http_request_timer("get_job"):
                self.sf._make_request('GET', url, headers=headers)

            return True # requests will raise for a 400 InvalidJob

        except RequestException as ex:
            if ex.response.headers["Content-Type"] == 'application/json':
                exception_code = ex.response.json()['exceptionCode']
                if exception_code == 'InvalidJob':
                    return False
            raise

    def _get_batches(self, job_id):
        endpoint = "job/{}/batch".format(job_id)
        url = self.bulk_url.format(self.sf.instance_url, endpoint)
        headers = self._get_bulk_headers()

        with metrics.http_request_timer("get_batches"):
            resp = self.sf._make_request('GET', url, headers=headers)

        batches = xmltodict.parse(resp.text,
                                  xml_attribs=False,
                                  force_list=('batchInfo',))['batchInfoList']['batchInfo']

        return batches

    def _get_batch(self, job_id, batch_id):
        endpoint = "job/{}/batch/{}".format(job_id, batch_id)
        url = self.bulk_url.format(self.sf.instance_url, endpoint)
        headers = self._get_bulk_headers()

        with metrics.http_request_timer("get_batch"):
            resp = self.sf._make_request('GET', url, headers=headers)

        batch = xmltodict.parse(resp.text)

        return batch['batchInfo']

    def get_batch_results(self, job_id, batch_id, catalog_entry):
        """Given a job_id and batch_id, queries the batch results and reads
        CSV lines, yielding each line as a record."""
        headers = self._get_bulk_headers()
        endpoint = f"job/{job_id}/batch/{batch_id}/result"
        batch_url = self.bulk_url.format(self.sf.instance_url, endpoint)

        # Timing the request
        with metrics.http_request_timer("batch_result_list") as timer:
            timer.tags['sobject'] = catalog_entry['stream']
            batch_result_resp = self.sf._make_request('GET', batch_url, headers=headers)

        # Parse the result list from the XML response
        batch_result_list = xmltodict.parse(batch_result_resp.text, xml_attribs=False, force_list={'result'})['result-list']

        # Use ThreadPoolExecutor to parallelize the processing of results
        for result in batch_result_list['result']:
            url = batch_url + f"/{result}"
            headers['Content-Type'] = 'text/csv'

            # Use a context manager for temporary file handling
            with tempfile.NamedTemporaryFile(mode="w+", encoding="utf8", delete=False) as csv_file:
                # Stream the CSV content from Salesforce Bulk API
                try:
                    resp = self.sf._make_request('GET', url, headers=headers, stream=True)
                    resp.raise_for_status()  # Ensure we handle errors from the request

                    # Write chunks of CSV data to the temp file
                    for chunk in resp.iter_content(chunk_size=ITER_CHUNK_SIZE, decode_unicode=True):
                        if chunk:
                            csv_file.write(chunk.replace('\0', ''))  # Replace NULL bytes

                    csv_file.seek(0)  # Move back to the start of the file after writing

                except requests.exceptions.RequestException as e:
                    # Handle any request errors (timeouts, connection errors, etc.)
                    raise TapSalesforceException(f"Error fetching results: {str(e)}")

            # Now process the CSV file
            with open(csv_file.name, mode='r', encoding='utf8') as f:
                csv_reader = csv.reader(f, delimiter=',', quotechar='"')

                try:
                    # Read column names from the first line
                    column_name_list = next(csv_reader)
                except StopIteration:
                    # Handle case where no data is returned (empty CSV)
                    raise TapSalesforceException(f"No data found in batch {batch_id} result.")

                # Process each row in the CSV file
                for line in csv_reader:
                    record = dict(zip(column_name_list, line))
                    yield record

    def _close_job(self, job_id):
        if job_id in self.closed_jobs:
            LOGGER.info(f"Job {job_id} already closed. Skipping the request")
            return
        self.closed_jobs.append(job_id)
        endpoint = "job/{}".format(job_id)
        url = self.bulk_url.format(self.sf.instance_url, endpoint)
        body = {"state": "Closed"}

        with metrics.http_request_timer("close_job"):
            self.sf._make_request(
                'POST',
                url,
                headers=self._get_bulk_headers(),
                body=json.dumps(body))

    # pylint: disable=no-self-use
    def _iter_lines(self, response):
        """Clone of the iter_lines function from the requests library with the change
        to pass keepends=True in order to ensure that we do not strip the line breaks
        from within a quoted value from the CSV stream."""
        pending = None

        for chunk in response.iter_content(decode_unicode=True, chunk_size=ITER_CHUNK_SIZE):
            if pending is not None:
                chunk = pending + chunk

            lines = chunk.splitlines(keepends=True)

            if lines and lines[-1] and chunk and lines[-1][-1] == chunk[-1]:
                pending = lines.pop()
            else:
                pending = None

            for line in lines:
                yield line

        if pending is not None:
            yield pending

import time
import singer
import singer.utils as singer_utils
from singer import Transformer, metadata, metrics
from requests.exceptions import RequestException
from tap_salesforce.salesforce.bulk import Bulk

LOGGER = singer.get_logger()

BLACKLISTED_FIELDS = set(['attributes'])

def remove_blacklisted_fields(data):
    return {k: v for k, v in data.items() if k not in BLACKLISTED_FIELDS}

# pylint: disable=unused-argument
def transform_bulk_data_hook(data, typ, schema):
    result = data
    if isinstance(data, dict):
        result = remove_blacklisted_fields(data)

    # Salesforce can return the value '0.0' for integer typed fields. This
    # causes a schema violation. Convert it to '0' if schema['type'] has
    # integer.
    if data == '0.0' and 'integer' in schema.get('type', []):
        result = '0'

    # Salesforce Bulk API returns CSV's with empty strings for text fields.
    # When the text field is nillable and the data value is an empty string,
    # change the data so that it is None.
    if data == "" and "null" in schema['type']:
        result = None

    return result

def get_stream_version(catalog_entry, state):
    tap_stream_id = catalog_entry['tap_stream_id']
    catalog_metadata = metadata.to_map(catalog_entry['metadata'])
    replication_key = catalog_metadata.get((), {}).get('replication-key')

    if singer.get_bookmark(state, tap_stream_id, 'version') is None:
        stream_version = int(time.time() * 1000)
    else:
        stream_version = singer.get_bookmark(state, tap_stream_id, 'version')

    if replication_key:
        return stream_version
    return int(time.time() * 1000)

def resume_syncing_bulk_query(sf, catalog_entry, job_id, state, counter):
    bulk = Bulk(sf)
    current_bookmark = singer.get_bookmark(state, catalog_entry['tap_stream_id'], 'JobHighestBookmarkSeen') or sf.get_start_date(state, catalog_entry)
    current_bookmark = singer_utils.strptime_with_tz(current_bookmark)
    batch_ids = singer.get_bookmark(state, catalog_entry['tap_stream_id'], 'BatchIDs')

    start_time = singer_utils.now()
    stream = catalog_entry['stream']
    stream_alias = catalog_entry.get('stream_alias')
    catalog_metadata = metadata.to_map(catalog_entry.get('metadata'))
    replication_key = catalog_metadata.get((), {}).get('replication-key')
    stream_version = get_stream_version(catalog_entry, state)
    schema = catalog_entry['schema']

    if not bulk.job_exists(job_id):
        LOGGER.info("Found stored Job ID that no longer exists, resetting bookmark and removing JobID from state.")
        return counter

    # Iterate over the remaining batches, removing them once they are synced
    for batch_id in batch_ids[:]:
        with Transformer(pre_hook=transform_bulk_data_hook) as transformer:
            for rec in bulk.get_batch_results(job_id, batch_id, catalog_entry):
                counter.increment()
                rec = transformer.transform(rec, schema)
                rec = fix_record_anytype(rec, schema)
                singer.write_message(
                    singer.RecordMessage(
                        stream=(
                            stream_alias or stream),
                        record=rec,
                        version=stream_version,
                        time_extracted=start_time))

                # Update bookmark if necessary
                replication_key_value = replication_key and singer_utils.strptime_with_tz(rec[replication_key])
                if replication_key_value and replication_key_value <= start_time and replication_key_value > current_bookmark:
                    current_bookmark = singer_utils.strptime_with_tz(rec[replication_key])

        state = singer.write_bookmark(state,
                                      catalog_entry['tap_stream_id'],
                                      'JobHighestBookmarkSeen',
                                      singer_utils.strftime(current_bookmark))
        batch_ids.remove(batch_id)
        LOGGER.info("Finished syncing batch %s. Removing batch from state.", batch_id)
        LOGGER.info("Batches to go: %d", len(batch_ids))
        singer.write_state(state)

    return counter

def sync_stream(sf, catalog_entry, state, input_state, catalog):
    stream = catalog_entry['stream']

    with metrics.record_counter(stream) as counter:
        try:
            sync_records(sf, catalog_entry, state, input_state, counter, catalog)
            singer.write_state(state)
        except RequestException as ex:
            raise Exception("Error syncing {}: {} Response: {}".format(
                stream, ex, ex.response.text)) from ex
        except Exception as ex:
            raise Exception("Error syncing {}: {}".format(
                stream, ex)) from ex

        return counter


def get_selected_streams(catalog):
    selected = []
    for stream in catalog["streams"]:
        breadcrumb = next(s for s in stream["metadata"] if s.get("breadcrumb")==[])
        metadata = breadcrumb.get("metadata")
        if metadata:
            if metadata.get("selected"):
                selected.append(stream["stream"])
    return selected


def sync_records(sf, catalog_entry, state, input_state, counter, catalog):
    chunked_bookmark = singer_utils.strptime_with_tz(sf.get_start_date(state, catalog_entry))
    stream = catalog_entry['stream']
    schema = catalog_entry['schema']
    stream_alias = catalog_entry.get('stream_alias')
    catalog_metadata = metadata.to_map(catalog_entry['metadata'])
    replication_key = catalog_metadata.get((), {}).get('replication-key')
    stream_version = get_stream_version(catalog_entry, state)
    stream = stream.replace("/","_")
    activate_version_message = singer.ActivateVersionMessage(stream=(stream_alias or stream),
                                                             version=stream_version)

    start_time = singer_utils.now()
    

    LOGGER.info('Syncing Salesforce data for stream %s', stream)
    records_post = []
    new_state = {}
    #reset the state
    old_key = state["current_stream"]
    new_state["current_stream"] = state["current_stream"].replace("/","_")
    new_state["bookmarks"] = {new_state["current_stream"]:state["bookmarks"][old_key]}
    state = new_state
    if not replication_key:
        singer.write_message(activate_version_message)
        state = singer.write_bookmark(
            state, catalog_entry['tap_stream_id'], 'version', None)

    # If pk_chunking is set, only write a bookmark at the end
    if sf.pk_chunking:
        # Write a bookmark with the highest value we've seen
        state = singer.write_bookmark(
            state,
            catalog_entry['tap_stream_id'],
            replication_key,
            singer_utils.strftime(chunked_bookmark))

    if catalog_entry["stream"].startswith("Report_"):
        report_name = catalog_entry["stream"].split("(")[-1][:-1]
        
        headers = sf._get_standard_headers()
        endpoint = "queryAll"
        params = {'q': 'SELECT Id,FolderName,Name FROM Report'}
        url = sf.data_url.format(sf.instance_url, endpoint)
        response = sf._make_request('GET', url, headers=headers, params=params)
        reports = response.json().get("records", [])
        report = [r for r in reports if report_name==r["Name"]][0]
        report_id = report["Id"]

        endpoint = f"analytics/reports/{report_id}"
        url = sf.data_url.format(sf.instance_url, endpoint)
        response = sf._make_request('GET', url, headers=headers)

        with Transformer(pre_hook=transform_bulk_data_hook) as transformer:
            rec = transformer.transform(response.json(), schema)
        rec = fix_record_anytype(rec, schema)
        stream = stream.replace("/","_")
        singer.write_message(
            singer.RecordMessage(
                stream=(
                    stream_alias or stream),
                record=rec,
                version=stream_version,
                time_extracted=start_time))

    else:
        for rec in sf.query(catalog_entry, state):
            counter.increment()
            with Transformer(pre_hook=transform_bulk_data_hook) as transformer:
                rec = transformer.transform(rec, schema)
            rec = fix_record_anytype(rec, schema)

            singer.write_message(
                singer.RecordMessage(
                    stream=(
                        stream_alias or stream),
                    record=rec,
                    version=stream_version,
                    time_extracted=start_time))

            replication_key_value = replication_key and singer_utils.strptime_with_tz(rec[replication_key])

            if sf.pk_chunking:
                if replication_key_value and replication_key_value <= start_time and replication_key_value > chunked_bookmark:
                    # Replace the highest seen bookmark and save the state in case we need to resume later
                    chunked_bookmark = singer_utils.strptime_with_tz(rec[replication_key])
                    state = singer.write_bookmark(
                        state,
                        catalog_entry['tap_stream_id'],
                        'JobHighestBookmarkSeen',
                        singer_utils.strftime(chunked_bookmark))
                    singer.write_state(state)
            # Before writing a bookmark, make sure Salesforce has not given us a
            # record with one outside our range
            elif replication_key_value and replication_key_value <= start_time:
                state = singer.write_bookmark(
                    state,
                    catalog_entry['tap_stream_id'],
                    replication_key,
                    rec[replication_key])
                singer.write_state(state)

            selected = get_selected_streams(catalog)
            if stream == "ListView" and rec.get("SobjectType") in selected and rec["Id"] is not None:
                # Handle listview
                try:
                    sobject = rec["SobjectType"]
                    lv_name = rec["DeveloperName"]
                    lv_catalog = [x for x in catalog["streams"] if x["stream"] == sobject]
                    if len(lv_catalog) > 0:
                        LOGGER.info(f"Syncing {lv_name} with stream {sobject}")
                        # Get the list view query
                        lv = sf.listview(sobject, rec["Id"])
                        lv_query = lv["query"]
                        # Get the matching catalog entry
                        lv_catalog_entry = lv_catalog[0].copy()
                        lv_stream_name = f"ListView_{sobject}_{lv_name}"
                        lv_catalog_entry['stream'] = lv_stream_name
                        lv_stream_version = get_stream_version(lv_catalog_entry, state)
                        # Save the schema
                        lv_schema = lv_catalog_entry['schema']
                        lv_catalog_metadata = metadata.to_map(lv_catalog_entry['metadata'])
                        lv_replication_key = lv_catalog_metadata.get((), {}).get('replication-key')
                        lv_key_properties = lv_catalog_metadata.get((), {}).get('table-key-properties')

                        date_filter = None
                        if input_state.get("bookmarks"):
                            if input_state["bookmarks"].get(sobject):
                                if input_state["bookmarks"][sobject].get(lv_replication_key):
                                    replication_date = input_state['bookmarks'][sobject][lv_replication_key]
                                    date_filter = f"{lv_replication_key} > {replication_date}"

                        if date_filter:
                            if "WHERE" in lv_query:
                                lv_query = lv_query.split("WHERE")
                                lv_query[-1] = f" {date_filter} AND {lv_query[-1]}"
                                lv_query = "WHERE".join(lv_query)
                            elif "ORDER BY" in lv_query:
                                lv_query = lv_query.split("ORDER BY")
                                lv_query[0] = f"{lv_query[0]} WHERE {date_filter} "
                                lv_query = "ORDER BY".join(lv_query)
                            else:
                                lv_query = f"lv_query WHERE {date_filter}"

                        entry = {
                            "schema": {
                                "stream_name": lv_stream_name,
                                "schema": lv_schema,
                                "key_properties": lv_key_properties,
                                "replication_key": lv_replication_key
                            },
                            "records": []
                        }

                        lv_schema = entry["schema"]

                        singer.write_schema(
                            lv_schema["stream_name"],
                            lv_schema["schema"],
                            lv_schema["key_properties"],
                            lv_schema["replication_key"]
                        )

                        # Run the listview query
                        for lv_rec in sf.query(lv_catalog_entry, state, query_override=lv_query):
                            LOGGER.disabled = True
                            with Transformer(pre_hook=transform_bulk_data_hook) as transformer:
                                lv_rec = transformer.transform(lv_rec, lv_schema["schema"])
                            LOGGER.disabled = False

                            lv_rec = fix_record_anytype(lv_rec, lv_schema["schema"])

                            singer.write_message(
                                singer.RecordMessage(
                                    stream=lv_stream_name,
                                    record=lv_rec,
                                    version=lv_stream_version,
                                    time_extracted=start_time))

                except RequestException as e:
                    pass


def fix_record_anytype(rec, schema):
    """Modifies a record when the schema has no 'type' element due to a SF type of 'anyType.'
    Attempts to set the record's value for that element to an int, float, or string."""
    def try_cast(val, coercion):
        try:
            return coercion(val)
        except BaseException:
            return val

    for k, v in rec.items():
        if schema['properties'][k].get("type") is None:
            val = v
            val = try_cast(v, int)
            val = try_cast(v, float)
            if v in ["true", "false"]:
                val = (v == "true")

            if v == "":
                val = None

            rec[k] = val

    return rec

#!/usr/bin/env python3
import json
import sys
import singer
import singer.utils as singer_utils
from singer import metadata, metrics
import tap_salesforce.salesforce
from requests.exceptions import RequestException, HTTPError
from tap_salesforce.sync import (sync_stream, resume_syncing_bulk_query, get_stream_version, ACTIVITY_STREAMS)
from tap_salesforce.salesforce import Salesforce
from tap_salesforce.salesforce.bulk import Bulk
from tap_salesforce.salesforce.exceptions import (
    TapSalesforceException, TapSalesforceQuotaExceededException, TapSalesforceBulkAPIDisabledException)
from concurrent.futures import ThreadPoolExecutor, as_completed

LOGGER = singer.get_logger()

REQUIRED_CONFIG_KEYS = ['refresh_token',
                        'client_id',
                        'client_secret',
                        'start_date',
                        'api_type',
                        'select_fields_by_default']

CONFIG = {
    'refresh_token': None,
    'client_id': None,
    'client_secret': None,
    'start_date': None
}

FORCED_FULL_TABLE = {
    'BackgroundOperationResult', # Does not support ordering by CreatedDate
    'LoginEvent', # Does not support ordering by CreatedDate
}

def get_replication_key(sobject_name, fields):
    if sobject_name in FORCED_FULL_TABLE:
        return None

    fields_list = [f['name'] for f in fields]

    if 'SystemModstamp' in fields_list:
        return 'SystemModstamp'
    elif 'LastModifiedDate' in fields_list:
        return 'LastModifiedDate'
    elif 'CreatedDate' in fields_list:
        return 'CreatedDate'
    elif 'LoginTime' in fields_list and sobject_name == 'LoginHistory':
        return 'LoginTime'
    return None

def stream_is_selected(mdata):
    return mdata.get((), {}).get('selected', False)

def build_state(raw_state, catalog):
    state = {}

    for catalog_entry in catalog['streams']:
        tap_stream_id = catalog_entry['tap_stream_id']
        catalog_metadata = metadata.to_map(catalog_entry['metadata'])
        replication_method = catalog_metadata.get((), {}).get('replication-method')

        version = singer.get_bookmark(raw_state,
                                      tap_stream_id,
                                      'version')

        # Preserve state that deals with resuming an incomplete bulk job
        if singer.get_bookmark(raw_state, tap_stream_id, 'JobID'):
            job_id = singer.get_bookmark(raw_state, tap_stream_id, 'JobID')
            batches = singer.get_bookmark(raw_state, tap_stream_id, 'BatchIDs')
            current_bookmark = singer.get_bookmark(raw_state, tap_stream_id, 'JobHighestBookmarkSeen')
            state = singer.write_bookmark(state, tap_stream_id, 'JobID', job_id)
            state = singer.write_bookmark(state, tap_stream_id, 'BatchIDs', batches)
            state = singer.write_bookmark(state, tap_stream_id, 'JobHighestBookmarkSeen', current_bookmark)

        if replication_method == 'INCREMENTAL':
            replication_key = catalog_metadata.get((), {}).get('replication-key')
            replication_key_value = singer.get_bookmark(raw_state,
                                                        tap_stream_id,
                                                        replication_key)
            if version is not None:
                state = singer.write_bookmark(
                    state, tap_stream_id, 'version', version)
            if replication_key_value is not None:
                state = singer.write_bookmark(
                    state, tap_stream_id, replication_key, replication_key_value)
        elif replication_method == 'FULL_TABLE' and version is None:
            state = singer.write_bookmark(state, tap_stream_id, 'version', version)

    return state

# pylint: disable=undefined-variable
def create_property_schema(field, mdata):
    field_name = field['name']

    if field_name == "Id":
        mdata = metadata.write(
            mdata, ('properties', field_name), 'inclusion', 'automatic')
    else:
        mdata = metadata.write(
            mdata, ('properties', field_name), 'inclusion', 'available')

    property_schema, mdata = tap_salesforce.salesforce.field_to_property_schema(field, mdata)

    return (property_schema, mdata)

def add_synthetic_field(properties, mdata, field_name, field_type, field_format, sf):
    properties[field_name] = {
        'type': ['null', field_type],
        'items': {'type': field_format}
    }
    mdata = metadata.write(
        mdata, ('properties', field_name), 'inclusion', 'available')
    if sf.select_fields_by_default:
        mdata = metadata.write(
            mdata, ('properties', field_name), 'selected-by-default', True)

def generate_schema(fields, sf, sobject_name, replication_key, config=None):
    unsupported_fields = set()
    mdata = metadata.new()
    properties = {}

    # Loop over the object's fields
    for f in fields:
        field_name = f['name']

        property_schema, mdata = create_property_schema(
            f, mdata)

        # Compound Address fields and geolocations cannot be queried by the Bulk API
        if f['type'] in ("address", "location") and sf.api_type == tap_salesforce.salesforce.BULK_API_TYPE:
            unsupported_fields.add(
                (field_name, 'cannot query compound address fields or geolocations with bulk API'))

        # we haven't been able to observe any records with a json field, so we
        # are marking it as unavailable until we have an example to work with
        if f['type'] == "json":
            unsupported_fields.add(
                (field_name, 'do not currently support json fields - please contact support'))

        # Blacklisted fields are dependent on the api_type being used
        field_pair = (sobject_name, field_name)
        if field_pair in sf.get_blacklisted_fields():
            unsupported_fields.add(
                (field_name, sf.get_blacklisted_fields()[field_pair]))

        inclusion = metadata.get(
            mdata, ('properties', field_name), 'inclusion')

        if sf.select_fields_by_default and inclusion != 'unsupported':
            mdata = metadata.write(
                mdata, ('properties', field_name), 'selected-by-default', True)

        properties[field_name] = property_schema

    if config and sobject_name in ['Contact', 'Lead']:
        if config.get("campaign_ids"):
            add_synthetic_field(properties, mdata, 'CampaignMemberships', 'array', 'string', sf)
        elif config.get("list_ids"):
            add_synthetic_field(properties, mdata, 'ListViewMemberships', 'array', 'string', sf)

    if replication_key:
        mdata = metadata.write(
            mdata, ('properties', replication_key), 'inclusion', 'automatic')

    # There are cases where compound fields are referenced by the associated
    # subfields but are not actually present in the field list
    field_name_set = {f['name'] for f in fields}
    filtered_unsupported_fields = [f for f in unsupported_fields if f[0] in field_name_set]
    missing_unsupported_field_names = [f[0] for f in unsupported_fields if f[0] not in field_name_set]

    if missing_unsupported_field_names:
        LOGGER.info("Ignoring the following unsupported fields for object %s as they are missing from the field list: %s",
                    sobject_name,
                    ', '.join(sorted(missing_unsupported_field_names)))

    if filtered_unsupported_fields:
        LOGGER.info("Not syncing the following unsupported fields for object %s: %s",
                    sobject_name,
                    ', '.join(sorted([k for k, _ in filtered_unsupported_fields])))

    # Any property added to unsupported_fields has metadata generated and
    # removed
    for prop, description in filtered_unsupported_fields:
        if metadata.get(mdata, ('properties', prop),
                        'selected-by-default'):
            metadata.delete(
                mdata, ('properties', prop), 'selected-by-default')

        mdata = metadata.write(
            mdata, ('properties', prop), 'unsupported-description', description)
        mdata = metadata.write(
            mdata, ('properties', prop), 'inclusion', 'unsupported')

    if replication_key:
        mdata = metadata.write(
            mdata, (), 'valid-replication-keys', [replication_key])
    else:
        mdata = metadata.write(
            mdata,
            (),
            'forced-replication-method',
            {
                'replication-method': 'FULL_TABLE',
                'reason': 'No replication keys found from the Salesforce API'})

    mdata = metadata.write(mdata, (), 'table-key-properties', ['Id'])

    schema = {
        'type': 'object',
        'additionalProperties': False,
        'properties': properties
    }

    entry = {
        'stream': sobject_name,
        'tap_stream_id': sobject_name,
        'schema': schema,
        'metadata': metadata.to_list(mdata)
    }

    return entry


def get_reports_list(sf):
    output = []
    done = False
    if not sf.list_reports:
        return output
    headers = sf._get_standard_headers()
    endpoint = "queryAll"
    params = {'q': 'SELECT Id,FolderName,Name,DeveloperName,IsDeleted FROM Report'}
    url = sf.data_url.format(sf.instance_url, endpoint)

    while not done:
        try:
            response = sf._make_request('GET', url, headers=headers, params=params)
        except HTTPError as e:
            LOGGER.warning(f"Reports not supported. status_code={e.response.status_code}. response={e.response.json()}. url={e.request.url}")
            return output
        response_json = response.json()
        done = response_json.get("done")
        output.extend(response_json.get("records", []))
        if not done:
            url = sf.instance_url+response_json.get("nextRecordsUrl")
    return output



def get_report_metadata(sf, report_id):
    headers = sf._get_standard_headers()
    endpoint = f"analytics/reports/{report_id}/describe"
    url = sf.data_url.format(sf.instance_url, endpoint)

    response = sf._make_request('GET', url, headers=headers)

    return response.json()


def validate_listview(sf, lv, responses):
    sobject = lv['SobjectType']
    lv_id = lv['Id']
    try: 
        sf.listview(sobject,lv_id)
        responses.append(lv)
    except RequestException as e:
        LOGGER.info(f"No /'results/' endpoint found for Sobject: {sobject}, Id: {lv_id}. response: {e.response.text}. url: {e.request.url}")

def validate_report(sf, report, valid_reports):
    if report.get("IsDeleted"):
        return

    # Describe report
    try:
        report_metadata = get_report_metadata(sf, report["Id"])
    except RequestException as e:
        if e.response.status_code in [403, 501]:
            LOGGER.info(f"Unable to get metadata for report: '{report['Name']}'. response: {e.response.text}. Skipping!")
            return
        else:
            raise e
    
    columns = report_metadata.get('reportMetadata', {}).get('detailColumns', [])
    if len(columns) > 100:
        LOGGER.warning("Skipping report %s, as it has more than 100 columns", report["DeveloperName"])
        return
    
    valid_reports.append(report)


def get_views_list(sf):
    if not sf.list_views:
        return []
    headers = sf._get_standard_headers()
    endpoint = "queryAll"
    params = {'q': 'SELECT Id,Name,SobjectType,DeveloperName FROM ListView'}
    url = sf.data_url.format(sf.instance_url, endpoint)

    response = sf._make_request('GET', url, headers=headers, params=params)

    responses = []
    list_views = response.json().get("records", [])
    start_counter = 0
    concurrency_limit = 25

    while start_counter < len(list_views):
        end_counter = start_counter + concurrency_limit
        if end_counter >= len(list_views):
            end_counter = len(list_views)

        chunk = list_views[start_counter:end_counter]
        chunk_args = [
            [
            sf,
            lv,
            responses
            ]
         for lv in chunk] 
        run_concurrently(validate_listview, chunk_args)
        start_counter = end_counter

    return responses

def discover_stream(
    sf, sobject_name, entries, sf_custom_setting_objects, object_to_tag_references
):
    # Skip blacklisted SF objects depending on the api_type in use
    # ChangeEvent objects are not queryable via Bulk or REST (undocumented)
    if (
        sobject_name in sf.get_blacklisted_objects()
        and sobject_name not in ACTIVITY_STREAMS
    ) or sobject_name.endswith("ChangeEvent"):
        return

    sobject_description = sf.describe(sobject_name)

    if sobject_description is None:
        return

    # Cache customSetting and Tag objects to check for blacklisting after
    # all objects have been described
    if sobject_description.get("customSetting"):
        sf_custom_setting_objects.append(sobject_name)
    elif sobject_name.endswith("__Tag"):
        relationship_field = next(
            (
                f
                for f in sobject_description["fields"]
                if f.get("relationshipName") == "Item"
            ),
            None,
        )
        if relationship_field:
            # Map {"Object":"Object__Tag"}
            object_to_tag_references[
                relationship_field["referenceTo"][0]
            ] = sobject_name

    fields = sobject_description["fields"]
    replication_key = get_replication_key(sobject_name, fields)

    # Salesforce Objects are skipped when they do not have an Id field
    if not [f["name"] for f in fields if f["name"] == "Id"]:
        LOGGER.info(
            "Skipping Salesforce Object %s, as it has no Id field", sobject_name
        )
        return

    entry = generate_schema(fields, sf, sobject_name, replication_key, CONFIG)
    entries.append(entry)


def run_concurrently(fn, fn_args_list):
    all_tasks = []

    # This function is used to garantee the right other of returns.
    # It saves the index of the `fn_args` used to call `fn`.
    def fn_with_index(index, *fn_args):
        result = fn(*fn_args)
        return (index, result)

    with ThreadPoolExecutor(max_workers=5) as executor:
        for (index, fn_args) in enumerate(fn_args_list):
            all_tasks.append(executor.submit(fn_with_index, *[index, *fn_args]))

    results = []

    for future in as_completed(all_tasks):
        (index, result) = future.result()
        # Insert the result in the right index of the list
        results.insert(index, result)

    return results

# pylint: disable=too-many-branches,too-many-statements
def do_discover(sf):
    """Describes a Salesforce instance's objects and generates a JSON schema for each field."""
    global_description = sf.describe()

    objects_to_discover = {o["name"] for o in global_description["sobjects"]}

    sf_custom_setting_objects = []
    object_to_tag_references = {}

    # For each SF Object describe it, loop its fields and build a schema
    entries = []

    # Check if the user has BULK API enabled
    if sf.api_type == "BULK" and not Bulk(sf).has_permissions():
        raise TapSalesforceBulkAPIDisabledException(
            'This client does not have Bulk API permissions, received "API_DISABLED_FOR_ORG" error code'
        )

    objects_list = sorted(objects_to_discover)
    start_counter = 0
    concurrency_limit = 25

    while start_counter < len(objects_list):
        end_counter = start_counter + concurrency_limit
        if end_counter >= len(objects_list):
            end_counter = len(objects_list)

        chunk = objects_list[start_counter:end_counter]
        chunk_args = [
            [
                sf,
                sobject_name,
                entries,
                sf_custom_setting_objects,
                object_to_tag_references,
            ]
            for sobject_name in chunk]
        run_concurrently(discover_stream, chunk_args)
        start_counter = end_counter
    
    # Handle ListViews
    views = get_views_list(sf)

    mdata = metadata.new()

    properties = {f"ListView_{o['SobjectType']}_{o['DeveloperName']}":dict(type=['null','object','string']) for o in views}

    for name in properties.keys():
        mdata = metadata.write(
            mdata,('properties',name),'selected-by-default',True
        )
    
    mdata = metadata.write(
            mdata,
            (),
            'forced-replication-method',
            {'replication-method': 'FULL_TABLE'})

    mdata = metadata.write(mdata, (), 'table-key-properties', [])

    schema = {
        'type': 'object',
        'additionalProperties': False,
        'properties': properties
    }

    entry = {
        'stream': "ListViews",
        'tap_stream_id': "ListViews",
        'schema': schema,
        'metadata': metadata.to_list(mdata)
    }

    entries.append(entry)

    # Handle Reports
    if sf.list_reports is True:
        reports = get_reports_list(sf)

        mdata = metadata.new()
        properties = {}

        if reports:
            start_counter = 0
            concurrency_limit = 25

            valid_reports = []
            # Cull invalid reports
            while start_counter < len(reports):
                end_counter = start_counter + concurrency_limit
                if end_counter >= len(reports):
                    end_counter = len(reports)
                
                chunk = reports[start_counter:end_counter]
                chunk_args = [
                    [
                        sf,
                        report,
                        valid_reports
                    ]
                    for report in chunk]
                run_concurrently(validate_report, chunk_args)
                start_counter = end_counter
            
            for report in valid_reports:
                field_name = f"Report_{report['DeveloperName']}"
                properties[field_name] = dict(type=["null", "object", "string"]) 
                mdata = metadata.write(
                    mdata, ('properties', field_name), 'selected-by-default', False)

            mdata = metadata.write(
                mdata,
                (),
                'forced-replication-method',
                {'replication-method': 'FULL_TABLE'})

            mdata = metadata.write(mdata, (), 'table-key-properties', [])

            schema = {
                'type': 'object',
                'additionalProperties': False,
                'properties': properties
            }

            entry = {
                'stream': "ReportList",
                'tap_stream_id': "ReportList",
                'schema': schema,
                'metadata': metadata.to_list(mdata)
            }

            entries.append(entry)

    # For each custom setting field, remove its associated tag from entries
    # See Blacklisting.md for more information
    unsupported_tag_objects = [object_to_tag_references[f]
                               for f in sf_custom_setting_objects if f in object_to_tag_references]
    if unsupported_tag_objects:
        LOGGER.info( #pylint:disable=logging-not-lazy
            "Skipping the following Tag objects, Tags on Custom Settings Salesforce objects " +
            "are not supported by the Bulk API:")
        LOGGER.info(unsupported_tag_objects)
        entries = [e for e in entries if e['stream']
                   not in unsupported_tag_objects]

    result = {'streams': entries}
    json.dump(result, sys.stdout, indent=4)

def do_sync(sf, catalog, state,config=None):
    input_state = state.copy()
    starting_stream = state.get("current_stream")

    if starting_stream:
        LOGGER.info("Resuming sync from %s", starting_stream)
    else:
        LOGGER.info("Starting sync")
    catalog = prepare_reports_streams(catalog)

    # Set ListView as first stream to sync to avoid issues with replication-keys
    list_view = [c for c in catalog["streams"] if c["stream"]=="ListView"]
    catalog["streams"] = [c for c in catalog["streams"] if c["stream"]!="ListView"]
    catalog["streams"] = list_view + catalog["streams"]

    # Sync Streams
    for catalog_entry in catalog["streams"]:
        stream_version = get_stream_version(catalog_entry, state)
        stream = catalog_entry['stream']
        stream_alias = catalog_entry.get('stream_alias')
        stream_name = catalog_entry["tap_stream_id"].replace("/","_")
        activate_version_message = singer.ActivateVersionMessage(
            stream=(stream_alias or stream.replace("/","_")), version=stream_version)

        catalog_metadata = metadata.to_map(catalog_entry['metadata'])
        replication_key = catalog_metadata.get((), {}).get('replication-key')

        mdata = metadata.to_map(catalog_entry['metadata'])

        if not stream_is_selected(mdata):
            LOGGER.info("%s: Skipping - not selected", stream_name)
            continue

        if starting_stream:
            if starting_stream == stream_name:
                LOGGER.info("%s: Resuming", stream_name)
                starting_stream = None
            else:
                LOGGER.info("%s: Skipping - already synced", stream_name)
                continue
        else:
            LOGGER.info("%s: Starting", stream_name)

        state["current_stream"] = stream_name
        singer.write_state(state)
        key_properties = metadata.to_map(catalog_entry['metadata']).get((), {}).get('table-key-properties')
        singer.write_schema(
            stream.replace("/","_"),
            catalog_entry['schema'],
            key_properties,
            replication_key,
            stream_alias)

        job_id = singer.get_bookmark(state, catalog_entry['tap_stream_id'], 'JobID')
        if job_id:
            with metrics.record_counter(stream) as counter:
                LOGGER.info("Found JobID from previous Bulk Query. Resuming sync for job: %s", job_id)
                # Resuming a sync should clear out the remaining state once finished
                counter = resume_syncing_bulk_query(sf, catalog_entry, job_id, state, counter)
                LOGGER.info("%s: Completed sync (%s rows)", stream_name, counter.value)
                # Remove Job info from state once we complete this resumed query. One of a few cases could have occurred:
                # 1. The job succeeded, in which case make JobHighestBookmarkSeen the new bookmark
                # 2. The job partially completed, in which case make JobHighestBookmarkSeen the new bookmark, or
                #    existing bookmark if no bookmark exists for the Job.
                # 3. The job completely failed, in which case maintain the existing bookmark, or None if no bookmark
                state.get('bookmarks', {}).get(catalog_entry['tap_stream_id'], {}).pop('JobID', None)
                state.get('bookmarks', {}).get(catalog_entry['tap_stream_id'], {}).pop('BatchIDs', None)
                bookmark = state.get('bookmarks', {}).get(catalog_entry['tap_stream_id'], {}) \
                                                     .pop('JobHighestBookmarkSeen', None)
                existing_bookmark = state.get('bookmarks', {}).get(catalog_entry['tap_stream_id'], {}) \
                                                              .pop(replication_key, None)
                state = singer.write_bookmark(
                    state,
                    catalog_entry['tap_stream_id'],
                    replication_key,
                    bookmark or existing_bookmark) # If job is removed, reset to existing bookmark or None
                singer.write_state(state)
        else:
            # Tables with a replication_key or an empty bookmark will emit an
            # activate_version at the beginning of their sync
            bookmark_is_empty = state.get('bookmarks', {}).get(
                catalog_entry['tap_stream_id']) is None

            if "/" in state["current_stream"]:
                # get current name
                old_key = state["current_stream"]
                # get the new key name
                new_key = old_key.replace("/","_")
                state["current_stream"] = new_key

            catalog_entry['tap_stream_id'] = catalog_entry['tap_stream_id'].replace("/","_")
            if replication_key or bookmark_is_empty:
                singer.write_message(activate_version_message)
                state = singer.write_bookmark(state,
                                              catalog_entry['tap_stream_id'],
                                              'version',
                                              stream_version)
            counter = sync_stream(sf, catalog_entry, state, input_state, catalog,config)
            LOGGER.info("%s: Completed sync (%s rows)", stream_name, counter.value)

    state["current_stream"] = None
    singer.write_state(state)
    LOGGER.info("Finished sync")

def main_impl():
    args = singer_utils.parse_args(REQUIRED_CONFIG_KEYS)
    CONFIG.update(args.config)

    sf = None
    is_sandbox = (
        CONFIG.get("base_uri") == "https://test.salesforce.com"
        if CONFIG.get("base_uri")
        else CONFIG.get("is_sandbox")
    )
    try:
        sf = Salesforce(
            refresh_token=CONFIG['refresh_token'],
            sf_client_id=CONFIG['client_id'],
            sf_client_secret=CONFIG['client_secret'],
            quota_percent_total=CONFIG.get('quota_percent_total'),
            quota_percent_per_run=CONFIG.get('quota_percent_per_run'),
            is_sandbox=is_sandbox,
            select_fields_by_default=CONFIG.get('select_fields_by_default'),
            default_start_date=CONFIG.get('start_date'),
            api_type=CONFIG.get('api_type'),
            list_reports=CONFIG.get('list_reports'),
            list_views=CONFIG.get('list_views'),            # config 
            api_version=CONFIG.get('api_version')
            )
        sf.login()

        if args.discover:
            do_discover(sf)
        elif args.properties:
            catalog = args.properties
            state = build_state(args.state, catalog)
            do_sync(sf, catalog, state,CONFIG)
    finally:
        if sf:
            if sf.rest_requests_attempted > 0:
                LOGGER.debug(
                    "This job used %s REST requests towards the Salesforce quota.",
                    sf.rest_requests_attempted)
            if sf.jobs_completed > 0:
                LOGGER.debug(
                    "Replication used %s Bulk API jobs towards the Salesforce quota.",
                    sf.jobs_completed)
            if sf.login_timer:
                sf.login_timer.cancel()

def prepare_reports_streams(catalog):
    streams = catalog["streams"]
    #prepare dynamic schema for selected reports
    for stream in streams:
        report_stream = {}
        if stream["stream"] == "ReportList":
            for meta in stream["metadata"][:-1]:
                if meta["metadata"].get("selected")==True:
                    report_name = meta["breadcrumb"][1]
                    report_stream = create_report_stream(report_name)
                    streams.append(report_stream)
    catalog["streams"] = streams  
    #pop ReportList from list of Streams
    catalog["streams"] = [i for i in catalog["streams"] if not (i['stream'] == "ReportList")]          
    return catalog           

def create_report_stream(report_name):
        mdata = metadata.new()
        properties = {}
        fields = ["attributes", "factMap", "groupingsAcross", "groupingsDown", "picklistColors", "reportExtendedMetadata", "reportMetadata", "allData", "hasDetailRows"]

        # Loop over the object's fields
        for field_name in fields:
            if field_name in ["allData", "hasDetailRows"]:
                property_schema = dict(type=["null", "boolean"])
            else:
                property_schema = dict(type=["null", "object", "string"])
            mdata = metadata.write(
                mdata, ('properties', field_name), 'selected-by-default', True)
            mdata = metadata.write(
                mdata, ('properties', field_name), 'selected', True)    

            properties[field_name] = property_schema

        mdata = metadata.write(
            mdata,
            (),
            'forced-replication-method',
            {'replication-method': 'FULL_TABLE'})

        mdata = metadata.write(mdata, (), 'table-key-properties', [])
        mdata = metadata.write(mdata, (), 'selected', True)
        mdata = metadata.write(mdata, (), 'is-custom-report', True)

        schema = {
            'type': 'object',
            'additionalProperties': False,
            'properties': properties
        }

        entry = {
            'stream': report_name,
            'tap_stream_id': report_name,
            'schema': schema,
            'metadata': metadata.to_list(mdata)
        }
        return entry

def main():
    try:
        main_impl()
    except TapSalesforceQuotaExceededException as e:
        LOGGER.critical(e)
        sys.exit(2)
    except TapSalesforceException as e:
        LOGGER.critical(e)
        sys.exit(1)
    except Exception as e:
        LOGGER.critical(e)
        raise e

if __name__=="__main__":
    main()

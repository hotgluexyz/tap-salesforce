import time
import re
import singer
import singer.utils as singer_utils
from singer import Transformer, metadata, metrics
from requests.exceptions import RequestException
from tap_salesforce.salesforce.bulk import Bulk
import base64

LOGGER = singer.get_logger()

BLACKLISTED_FIELDS = set(['attributes'])

ACTIVITY_STREAMS = ["ActivityHistory", "OpenActivity"]

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

def sync_stream(sf, catalog_entry, state, input_state, catalog,config=None):
    stream = catalog_entry['stream']

    with metrics.record_counter(stream) as counter:
        try:
            sync_records(sf, catalog_entry, state, input_state, counter, catalog,config)
            singer.write_state(state)
        except RequestException as ex:
            raise Exception("Error syncing {}: {} Response: {}".format(
                stream, ex, ex.response.text)) from ex
        except Exception as ex:
            raise Exception("Error syncing {}: {}".format(
                stream, ex)) from ex

        return counter


def get_selected_streams(catalog):
    selected = set()
    for stream in catalog["streams"]:
        if stream["stream"].startswith("Report_"):
            breadcrumb = next(s for s in stream["metadata"] if s.get("breadcrumb")==())
        else:
            breadcrumb = next(s for s in stream["metadata"] if s.get("breadcrumb")==[])     
        
        metadata = breadcrumb.get("metadata")
        if metadata:
            if metadata.get("selected"):
                selected.add(stream["stream"])
                
    return selected

def handle_ListView(sf,rec_id,sobject,lv_name,lv_catalog_entry,state,input_state,start_time):
    LOGGER.info(f"Syncing {lv_name} with stream {sobject}")
    # Get the list view query
    lv = sf.listview(sobject, rec_id)
    lv_query = lv["query"]
    properties = sf._build_query_string(lv_catalog_entry, start_time)
    sel_properties = properties.split("SELECT ")[-1].split(" FROM")[0]
    lv_query = re.sub(r"(?<=SELECT ).*(?= FROM)", sel_properties, lv_query)
    # Get the matching catalog entry
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

def get_campaign_memberships(sf, campaign_ids, stream):
    """
    Fetches campaign memberships for Contact or Lead records.
    
    Args:
        sf: Salesforce client instance
        campaign_ids: List of campaign IDs to fetch memberships for
        stream: Either 'Contact' or 'Lead'
        
    Returns:
        Dictionary mapping Contact/Lead IDs to lists of campaign IDs they belong to
    """
    campaign_memberships = {}
    
    try:
        campaign_ids_str = "'" + "','".join(campaign_ids) + "'"
        headers = sf._get_standard_headers()
        endpoint = "queryAll"
        id_field = "ContactId" if stream == "Contact" else "LeadId"
        
        # Query to get all campaign memberships
        membership_query = f"SELECT CampaignId, {id_field} FROM CampaignMember WHERE CampaignId IN ({campaign_ids_str}) AND {id_field} != null"
        membership_url = sf.data_url.format(sf.instance_url, endpoint)
        params = {'q': membership_query}
        
        # Execute query to get campaign memberships
        response = sf._make_request('GET', membership_url, headers=headers, params=params)
        records = response.json().get('records', [])
        
        # Process the records and build a membership map
        for record in records:
            entity_id = record[id_field]
            campaign_id = record['CampaignId']
            
            if entity_id not in campaign_memberships:
                campaign_memberships[entity_id] = []
            
            campaign_memberships[entity_id].append(campaign_id)
        
        # Handle pagination if there are more results
        next_records_url = response.json().get('nextRecordsUrl')
        while next_records_url:
            paginated_url = sf.instance_url + next_records_url
            response = sf._make_request('GET', paginated_url, headers=headers)
            records = response.json().get('records', [])
            
            for record in records:
                entity_id = record[id_field]
                campaign_id = record['CampaignId']
                
                if entity_id not in campaign_memberships:
                    campaign_memberships[entity_id] = []
                
                campaign_memberships[entity_id].append(campaign_id)
            
            next_records_url = response.json().get('nextRecordsUrl')
        
        LOGGER.info(f"Found {len(campaign_memberships)} {stream} records with campaign memberships")
    except Exception as e:
        LOGGER.error(f"Error retrieving campaign memberships: {str(e)}")
        campaign_memberships = {}
        
    return campaign_memberships


def sync_filtered_accounts(sf, state, stream, catalog_entry, replication_key, config):

    record_ids = set()
    list_view_memberships = {}
    campaign_memberships = {}
    combined_query = config.get("list_ids") and config.get("campaign_ids")
    query = ""
    
    campaign_member_where_clause = lambda entity_name, campaign_ids_str, start_date_str: f"""
        Id IN (
            SELECT {entity_name}Id
            FROM CampaignMember
            WHERE CampaignId IN ({campaign_ids_str})
            AND (SystemModstamp > {start_date_str} OR {entity_name}.SystemModstamp > {start_date_str})
            AND {entity_name}Id != null
        )
    """
    stream_has_lists = False
    if config.get("list_ids"):
        list_ids = config['list_ids']
        quoted_list_ids = "'" + "','".join(list_ids) + "'"
        query = f"""
            SELECT Id FROM ListView WHERE Id IN ({quoted_list_ids})
            AND SobjectType = '{stream}'
        """
        query_response = sf.query(catalog_entry, state, query_override=query)

        for rec in query_response:
            stream_has_lists = True
            list_id = rec["Id"]
            described_list_view = sf.listview(stream, list_id)
            entity_query = described_list_view["query"]
            entity_query_response = sf.query(catalog_entry, state, query_override=entity_query)

            for entity_rec in entity_query_response:
                entity_id = entity_rec["Id"]
                record_ids.add(entity_id)
                
                # Track which list_ids this record belongs to
                if entity_id not in list_view_memberships:
                    list_view_memberships[entity_id] = []
                list_view_memberships[entity_id].append(list_id)
        
            LOGGER.info(f"ListView: {list_id} for {stream}")
        
        LOGGER.info(f"Found {len(record_ids)} {stream} records in the specified list views")
    
    if config.get("campaign_ids"):
        campaign_ids = config['campaign_ids']
        campaign_ids_str = "'" + "','".join(campaign_ids) + "'"
        LOGGER.info(f"Filtering {stream} by campaign membership for campaign IDs: {campaign_ids_str}")
        
        campaign_memberships = get_campaign_memberships(sf, campaign_ids, stream)
        
    
    start_date_str = sf.get_start_date(state, catalog_entry)
    selected_properties = sf._get_selected_properties(catalog_entry)
    
    if 'ListViewMemberships' in selected_properties:
        selected_properties.remove('ListViewMemberships')
        
    if 'CampaignMemberships' in selected_properties:
        selected_properties.remove('CampaignMemberships')
    
    if combined_query and stream_has_lists:
        quoted_ids = "'" + "','".join(record_ids) + "'"
        
        query = f"""
            SELECT {','.join(selected_properties)}
            FROM {stream}
            WHERE (Id IN ({quoted_ids}))
            AND {campaign_member_where_clause(stream, campaign_ids_str, start_date_str).strip()}
        """
    elif config.get("list_ids") and stream_has_lists:
        quote_ids = "'" + "','".join(record_ids) + "'"
        
        query = f"""
            SELECT {','.join(selected_properties)}
            FROM {stream}
            WHERE Id IN ({quote_ids}) AND SystemModstamp > {start_date_str}
        """
    elif config.get("campaign_ids"):
        entity_name = stream  # "Contact" or "Lead"
        
        query = f"""
            SELECT {','.join(selected_properties)}
            FROM {stream}
            WHERE {campaign_member_where_clause(entity_name, campaign_ids_str, start_date_str).strip()}
        """
    else:
        query = f"SELECT {','.join(selected_properties)} FROM {stream} WHERE SystemModstamp > {start_date_str}"
        
    LOGGER.info(f"Generated query: {query}")
        
    if replication_key:
        query += f" ORDER BY {replication_key} ASC"
        
    query_response = sf.query(catalog_entry, state, query_override=query)

    return query_response, campaign_memberships, list_view_memberships

def sync_records(sf, catalog_entry, state, input_state, counter, catalog,config=None):
    download_files = False
    if "download_files" in config:
        if config['download_files']==True:
            download_files = True
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
    campaign_memberships = {}
    list_view_memberships = {}

    LOGGER.info('Syncing Salesforce data for stream %s', stream)
    records_post = []
    
    if "/" in state["current_stream"]:
        # get current name
        old_key = state["current_stream"]
        # get the new key name
        new_key = old_key.replace("/","_")
        # move to new key
        state["bookmarks"][new_key] = state["bookmarks"].pop(old_key)

    
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
        report_name = catalog_entry["stream"].split("Report_", 1)[1]
        
        reports = []
        done = False
        headers = sf._get_standard_headers()
        endpoint = "queryAll"
        params = {'q': 'SELECT Id,DeveloperName FROM Report'}
        url = sf.data_url.format(sf.instance_url, endpoint)

        while not done:
            response = sf._make_request('GET', url, headers=headers, params=params)
            response_json = response.json()
            done = response_json.get("done")
            reports.extend(response_json.get("records", []))
            if not done:
                url = sf.instance_url+response_json.get("nextRecordsUrl")

        report = [r for r in reports if report_name==r["DeveloperName"]][0]
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

    elif "ListViews" == catalog_entry["stream"]:
        headers = sf._get_standard_headers()
        endpoint = "queryAll"

        params = {'q': f'SELECT Name,Id,SobjectType,DeveloperName FROM ListView'}
        url = sf.data_url.format(sf.instance_url, endpoint)
        response = sf._make_request('GET', url, headers=headers, params=params)
    
        Id_Sobject = [{"Id":r["Id"],"SobjectType": r["SobjectType"],"DeveloperName":r["DeveloperName"],"Name":r["Name"]}
                    for r in response.json().get('records',[]) if r["Name"]]

        selected_lists_names = []
        for ln in catalog_entry.get("metadata",[])[:-1]:
            if ln.get("metadata",[])['selected']:
                selected_list = ln.get('breadcrumb',[])[1]
                for isob in Id_Sobject:
                    if selected_list==f"ListView_{isob['SobjectType']}_{isob['DeveloperName']}":
                        selected_lists_names.append(isob)

        replication_key_value = replication_key and singer_utils.strptime_with_tz(rec[replication_key])

        for list_info in selected_lists_names:

            sobject = list_info['SobjectType']
            lv_name = list_info['DeveloperName']
            lv_id = list_info['Id']

            lv_catalog = [x for x in catalog["streams"] if x["stream"] == sobject]

            if lv_catalog:
                lv_catalog_entry = lv_catalog[0].copy()
                try:
                    handle_ListView(sf,lv_id,sobject,lv_name,lv_catalog_entry,state,input_state,start_time)
                except RequestException as e:
                    LOGGER.warning(f"No existing /'results/' endpoint was found for SobjectType:{sobject}, Id:{lv_id}")

    else:
        if stream in ["Contact", "Lead"] and "list_ids" in config or "campaign_ids" in config: 
            query_response, campaign_memberships, list_view_memberships = sync_filtered_accounts(sf, state, stream, catalog_entry, replication_key, config)
        elif catalog_entry["stream"] in ACTIVITY_STREAMS:
            start_date_str = sf.get_start_date(state, catalog_entry)
            start_date = singer_utils.strptime_with_tz(start_date_str)
            start_date = singer_utils.strftime(start_date)

            selected_properties = sf._get_selected_properties(catalog_entry)

            query_map = {
                "ActivityHistory": "ActivityHistories",
                "OpenActivity": "OpenActivities"
            }

            query_field = query_map[catalog_entry['stream']]

            query = "SELECT {} FROM {}".format(",".join(selected_properties), query_field)
            query = f"SELECT ({query}) FROM Contact"

            catalog_metadata = metadata.to_map(catalog_entry['metadata'])
            replication_key = catalog_metadata.get((), {}).get('replication-key')

            order_by = ""
            if replication_key:
                where_clause = " WHERE {} > {} ".format(
                    replication_key,
                    start_date)
                order_by = " ORDER BY {} ASC".format(replication_key)
                query = query + where_clause + order_by

            def unwrap_query(query_response, query_field):
                for q in query_response:
                    if q.get(query_field):
                        for f in q[query_field]["records"]:
                            yield f

            query_response = sf.query(catalog_entry, state, query_override=query)
            query_response = unwrap_query(query_response, query_field)
        else:
            query_response = sf.query(catalog_entry, state)

        selected = (
            get_selected_streams(catalog)
            if stream == "ListView"
            else set()
        )

        for rec in query_response:
            counter.increment()
            with Transformer(pre_hook=transform_bulk_data_hook) as transformer:
                rec = transformer.transform(rec, schema)
            rec = fix_record_anytype(rec, schema)

            if stream in ["Contact", "Lead"]:
                if config.get("campaign_ids"):
                    if rec['Id'] in campaign_memberships:
                        rec['CampaignMemberships'] = campaign_memberships[rec['Id']]
                    else:
                        rec['CampaignMemberships'] = []
                
                if config.get("list_ids"):
                    if rec['Id'] in list_view_memberships:
                        rec['ListViewMemberships'] = list_view_memberships[rec['Id']]
                    else:
                        rec['ListViewMemberships'] = []

            if stream=='ContentVersion':
                if "IsLatest" in rec:
                    if rec['IsLatest']==True and download_files==True:
                        rec['TextPreview'] = base64.b64encode(get_content_document_file(sf,rec['Id'])).decode('utf-8')
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

            if stream == "ListView" and rec.get("SobjectType") in selected and rec.get("Id") is not None:
                # Handle listview
                try:
                    sobject = rec["SobjectType"]
                    lv_name = rec["DeveloperName"]
                    lv_catalog = [x for x in catalog["streams"] if x["stream"] == sobject]
                    rec_id = rec["Id"]
                    lv_catalog_entry = lv_catalog[0].copy()
                    if len(lv_catalog) > 0:
                        handle_ListView(sf,rec_id,sobject,lv_name,lv_catalog_entry,state,input_state,start_time)
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

def get_content_document_file(sf,contentid):
    headers = sf._get_standard_headers()
    endpoint = f"sobjects/ContentVersion/{contentid}/VersionData"
    url = sf.data_url.format(sf.instance_url, endpoint)
    response = sf._make_request('GET', url, headers=headers)
    return response.content

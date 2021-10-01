import json
import boto3
import datetime
import logging
import math
import os
import xmltodict
import threefive
from threefive import Cue
import copy

LOGGER = logging.getLogger()
LOGGER.setLevel(logging.INFO)


def lambda_handler(event, context):
    LOGGER.info(event)

    # Properties supported for SCTE35 binary replace
    threefive_scte_format = dict()
    threefive_scte_format['info_section'] = {
        'table_id':'int',
        'section_syntax_indicator':'bool',
        'private':'bool',
        'sap_type':'int',
        'sap_details':'',
        'protocol_version':'int',
        'pts_adjustment':'float',
        'splice_command_type':'int'
    }
    threefive_scte_format['command'] = {
        'command_type':'int',
        'time_specified_flag':'bool',
        'pts_time':'float',
        'break_auto_return':'bool',
        'break_duration':'float',
        'splice_event_id':'int',
        'splice_event_cancel_indicator':'',
        'out_of_network_indicator':'bool',
        'program_splice_flag':'bool',
        'duration_flag':'bool',
        'splice_immediate_flag':'bool',
        'unique_program_id':'int',
        'avail_num':'int',
        'avail_expected':'int'
    }
    threefive_scte_format['descriptors'] = {
        'tag':'int',
        'descriptor_length':'int',
        'identifier':'str',
        'segmentation_event_id':'int',
        'segmentation_event_cancel_indicator':'bool',
        'program_segmentation_flag':'bool',
        'segmentation_duration_flag':'bool',
        'delivery_not_restricted_flag':'bool',
        'web_delivery_allowed_flag':'bool',
        'no_regional_blackout_flag':'bool',
        'archive_allowed_flag':'bool',
        'device_restrictions':'int',
        'segmentation_upid_type':'int',
        'segmentation_upid':'int',
        'segmentation_type_id':'int',
        'segment_num':'int',
        'segments_expected':'int',
        'provider_avail_id':'int'
    }

    # initialize BOTO3 Client for Dynamodb
    db_client = boto3.client('dynamodb')

    # channels database
    channeldb = os.environ['CHANNELDB']
    scheduledb = os.environ['SCHEDULEDB']

    # Extract SPE from Transcoder
    esam_payload_xml = event['body']

    # Convert to JSON
    esam_payload_json = xmltodict.parse(esam_payload_xml)

    # Extract SPE
    spe = esam_payload_json['SignalProcessingEvent']

    # xml namespace attributes only
    response_main_elements_attributes = dict()
    keys_in_spe = list(spe.keys())
    for key in keys_in_spe:
        if "@" in key:
            response_main_elements_attributes[key] = spe[key]


    # Extract acquired signal parameters
    acq_signal = spe['AcquiredSignal']

    acquisition_point_id = acq_signal['@acquisitionPointIdentity']
    acquisition_signal_id = acq_signal['@acquisitionSignalID']
    acquisition_time = acq_signal['@acquisitionTime']
    zone_identity = acq_signal['@zoneIdentity']

    sig_utc_point = acq_signal['sig:UTCPoint']['@utcPoint']
    sig_binary_data_type = acq_signal['sig:BinaryData']['@signalType']
    sig_binary_data = acq_signal['sig:BinaryData']['#text']

    sig_stream_times = acq_signal['sig:StreamTimes']

    ##
    ## Done Parsing
    ##


    # create exceptions list to catch exceptions
    exceptions = []
    exceptions.clear()

    # DYNAMO DB JSON BUILDER
    dynamodb_to_json = dict()
    keylist = []
    def dict_path(dicttopopulate,my_dict):
        for k,v in my_dict.items():

            value_type = list(my_dict[k].keys())[0]

            if value_type == "M":
                value = my_dict[k][value_type]

                for i in range(0,len(value)):
                    dynamodb_item_m = dict()
                    dict_path(dynamodb_item_m,value)
                    v = dynamodb_item_m

                value.update(dynamodb_item_m)
                dicttopopulate.update({k:value})

            elif value_type == "S":
                value = my_dict[k][value_type]
                dicttopopulate.update({k:value})

            elif value_type == "L": # list
                value = my_dict[k][value_type]

                for i in range(0,len(value)):
                    dynamodb_item_list = dict()
                    dict_path(dynamodb_item_list,value[i])

                    value[i] = dynamodb_item_list

                dicttopopulate.update({k:value})
            elif k == "M":

                dynamodb_item_m = dict()
                dict_path(dynamodb_item_m,v)
                v = dynamodb_item_m
                dicttopopulate.update(v)

    def value_type_validator(rk,rv):

        vartype = ""
        for property_header in threefive_scte_format:
            if rk in threefive_scte_format[property_header]:
                vartype = threefive_scte_format[property_header][rk]

        if vartype == "int":
            return int(rv)
        elif vartype == "float":
            return float(rv)
        elif vartype == "bool":
            if rv.lower() == "true":
                return True
            else:
                return False
        else:
            return str(rv)


    def dbGetSingleChannelInfo(channeldb,channel):
        LOGGER.debug("Doing a call to Dynamo to get channel information for channel : %s" % (channel))
        try:
            response = db_client.get_item(TableName=channeldb,Key={"channelid":{"S":channel}})
        except Exception as e:
            exceptions.append("Unable to get item from DynamoDB, got exception:  %s" % (str(e).upper()))
            return exceptions
        return response

    def scte_rule_checker(rule_condition_property,rule_condition_value,rule_condition_operator,scte_35_dict):
        # ['=','>','<','-','!=']


        scte35_property_value = ""
        for main_key in scte_35_dict:
            for property_key in scte_35_dict[main_key]:
                if property_key == rule_condition_property:
                    scte35_property_value = scte_35_dict[main_key][property_key]

        rule_condition_value = value_type_validator(rule_condition_property,rule_condition_value)


        if rule_condition_value == "false":
            rule_condition_value = False
        elif rule_condition_value == "true":
            rule_condition_value = True

        if rule_condition_operator == "=":
            if scte35_property_value == rule_condition_value:
                return True
            else:
                return False

        elif rule_condition_operator == ">":
            if scte35_property_value > rule_condition_value:
                return True
            else:
                return False

        elif rule_condition_operator == "<":
            if scte35_property_value < rule_condition_value:
                return True
            else:
                return False

        elif rule_condition_operator == "-":
            rule_value_min = rule_condition_value.split("-")[0]
            rule_value_max = rule_condition_value.split("-")[1]

            if scte35_property_value > rule_value_min and scte35_property_value < rule_value_max:
                return True
            else:
                return False

        elif rule_condition_operator == "!=":
            if scte35_property_value != rule_condition_value:
                return True
            else:
                return False

        else:
            return False

    def spn_delete():
        # Build Response Signal
        resp_signal = dict()

        # A Delete example
        resp_signal['@action'] = "delete"
        resp_signal['@acquisitionPointIdentity'] = acquisition_point_id
        resp_signal['@acquisitionSignalID'] = acquisition_signal_id
        resp_signal['@zoneIdentity'] = zone_identity
        #resp_signal['sig:BinaryData'] = {}
        #resp_signal['sig:BinaryData']['@signalType'] = sig_binary_data_type
        #resp_signal['sig:BinaryData']['#text'] = sig_binary_data
        resp_signal['sig:UTCPoint'] = {}
        resp_signal['sig:UTCPoint']['@utcPoint'] = sig_utc_point
        resp_signal['sig:StreamTimes'] = sig_stream_times

        return resp_signal

    def spn_noop():
        # Build Response Signal
        resp_signal = dict()

        # A NOOP Example
        resp_signal['@action'] = "noop"
        resp_signal['@acquisitionPointIdentity'] = acquisition_point_id
        resp_signal['@acquisitionSignalID'] = acquisition_signal_id
        resp_signal['@acquisitionTime'] = acquisition_time
        resp_signal['sig:UTCPoint'] = {}
        resp_signal['sig:UTCPoint']['@utcPoint'] = sig_utc_point
        resp_signal['sig:BinaryData'] = {}
        resp_signal['sig:BinaryData']['@signalType'] = sig_binary_data_type
        resp_signal['sig:BinaryData']['#text'] = sig_binary_data
        return resp_signal

    def spn_replace(sig_binary_data):

        # Build Response Signal
        resp_signal = dict()

        resp_signal['@action'] = "replace"
        resp_signal['@acquisitionPointIdentity'] = acquisition_point_id
        resp_signal['@acquisitionSignalID'] = acquisition_signal_id
        resp_signal['@acquisitionTime'] = acquisition_time
        resp_signal['sig:UTCPoint'] = {}
        resp_signal['sig:UTCPoint']['@utcPoint'] = sig_utc_point
        resp_signal['sig:BinaryData'] = {}
        resp_signal['sig:BinaryData']['@signalType'] = sig_binary_data_type
        resp_signal['sig:BinaryData']['#text'] = sig_binary_data

        return resp_signal

    action = ""
    custom_status_code = dict()
    channel_pois_record = dict()
    # See if channel exists in db

    try:
        get_channel_record = dbGetSingleChannelInfo(channeldb,acquisition_point_id)

        if "Item" in get_channel_record:
            channel_pois_record = get_channel_record['Item']
            dict_path(dynamodb_to_json,channel_pois_record)

        else:
            # channel doesn't exist in POIS, setting behavior to noop back to requestor
            action = "noop"
            custom_status_code['@classCode'] = 2
            custom_status_code['core:Note'] = "Channel not registered with POIS"

    except Exception as e:
        action = "noop"
        custom_status_code['@classCode'] = 2
        custom_status_code['core:Note'] = "Unable to retrieve channel config from POIS"


    # check if rules present
    # iterate through rules
    # process any rule that evaluates to true
    # if nothing evaluates to true, return default behavior

    if "rules" not in dynamodb_to_json:
        LOGGER.debug("Channel has no SCTE rules, sending default behavior")
        action = dynamodb_to_json['default_behavior']
        custom_status_code['@classCode'] = 3
        custom_status_code['core:Note'] = "No conditioning rules at POIS, using default behavior"

    else:
        LOGGER.debug("Channel has SCTE signal rules, iterating through them now")
        LOGGER.debug("Parsing inbound SCTE35")

        # Parse SCTE35 first
        try:
            scte_35_cue = threefive.Cue(sig_binary_data)
            scte_35_cue.decode()
            scte_35_dict = scte_35_cue.get()
        except:
            action = dynamodb_to_json['default_behavior']
            custom_status_code['@classCode'] = 2
            custom_status_code['core:Note'] = "Unable to decode inbound SCTE35, using default behavior"

        # Iterate through rules
        scte35notdeleted = True
        custom_status_code_rule_match = ""

        for r in range(0,len(dynamodb_to_json['rules'])):
            rule = dynamodb_to_json['rules'][r]

            # do a while loop, if we're iterating through the rules but there's alreadby been a delete match, may as well exit for loop
            while scte35notdeleted:
                rule_type = rule['type']
                rule_condition_property = rule['condition']['property']
                rule_condition_value = rule['condition']['value']
                rule_condition_operator = rule['condition']['operator']

                rule_check_result = scte_rule_checker(rule_condition_property,rule_condition_value,rule_condition_operator,scte_35_dict) # true false

                if rule_check_result:
                    if rule_type == "delete":
                        action = "delete"
                        scte35notdeleted = False

                        custom_status_code_rule_match = "matched rule %r" % (str(r))
                        custom_status_code['@classCode'] = 3
                        custom_status_code['core:Note'] = custom_status_code_rule_match

                    else: # replace
                        # iterate through replace_params and modify scte35 dict
                        action = "replace"

                        rule_params = rule['replace_params']
                        descriptors_dict = dict()
                        for replace_param_number in range(0,len(rule_params)):
                            rule_param = rule_params[replace_param_number]
                            r_key = list(rule_param.keys())[0]
                            r_value = value_type_validator(r_key,rule_param[r_key])
                            #r_header = ""


                            for property_header in threefive_scte_format:
                                if r_key in threefive_scte_format[property_header]:
                                    r_header = property_header

                            if r_header == "":
                                custom_status_code_rule_match += "rule %s replace param %s failed ." % (str(r),str(replace_param_number))
                                custom_status_code['@classCode'] = 2

                            else:
                                # replace property in scte35 dict
                                if not isinstance(scte_35_dict[r_header],list):
                                    scte_35_dict[r_header][r_key] = r_value
                                else:
                                    descriptors_dict.update({r_key:r_value})
                                    #scte_35_dict[r_header].append(r_key+":"+str(r_value))
                                custom_status_code_rule_match += "rule %s replace param %s filled ." % (str(r),str(replace_param_number))
                                if '@classCode' in custom_status_code:
                                    if custom_status_code['@classCode'] != 2:
                                        custom_status_code['@classCode'] = 3
                                else:
                                    custom_status_code['@classCode'] = 3
                        # add to descriptors

                    scte_35_dict['descriptors'] = [descriptors_dict]
                    #return scte_35_dict
                    # encode scte35
                    newcue = Cue()
                    newcue.load(scte_35_dict)
                    sig_binary_data = newcue.encode()
                    try:
                        newcue = Cue()
                        newcue.load(scte_35_dict)
                        sig_binary_data = newcue.encode()

                        custom_status_code['core:Note'] = custom_status_code_rule_match
                    except:
                        action = dynamodb_to_json['default_behavior']
                        custom_status_code['@classCode'] = 2
                        custom_status_code['core:Note'] = "Unable to encode new SCTE35, using default behavior"

                else:
                    action = dynamodb_to_json['default_behavior']
                    custom_status_code['@classCode'] = 3
                    custom_status_code['core:Note'] = "No rule match at POIS, using default behavior"

                scte35notdeleted = False


    ##
    ## Build ESAM Response
    ##
    LOGGER.info("action type : %s " % (action))
    if action == "delete":
        resp_signal = spn_delete()
    elif action == "noop":
        resp_signal = spn_noop()
    elif action == "replace":
        resp_signal = spn_replace(sig_binary_data)
    else:
        LOGGER.debug("requested action goes nowhere currently")


    # Create response SPN
    spn = dict()
    spn['SignalProcessingNotification'] = response_main_elements_attributes
    spn['SignalProcessingNotification']['ResponseSignal'] = resp_signal
    if len(custom_status_code) > 0:
        spn['SignalProcessingNotification']['common:StatusCode'] = custom_status_code


    # convert payload to xml for return
    spn_xml = xmltodict.unparse(spn, short_empty_elements=True, pretty=True)

    return {
        'statusCode': 200,
        "headers": {
            "Content-Type": "application/xml",
        },
        'body': spn_xml
    }

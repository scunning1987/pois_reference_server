import json
import boto3
import datetime
import time
import logging
import math
import os
import xmltodict
import threefive
from threefive import Cue
import copy
import binascii

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
        'name':'str',
        'segmentation_message':'str',
        'segmentation_upid_type_name':'str',
        'segmentation_upid_length':'int',
        'sub_segment_num':'int',
        'sub_segments_expected':'int',
        'descriptor_length':'int',
        'identifier':'str',
        'segmentation_event_id':'str',
        'segmentation_duration':'int',
        'segmentation_duration_raw':'int',
        'segmentation_event_cancel_indicator':'bool',
        'program_segmentation_flag':'bool',
        'segmentation_duration_flag':'bool',
        'delivery_not_restricted_flag':'bool',
        'web_delivery_allowed_flag':'bool',
        'no_regional_blackout_flag':'bool',
        'archive_allowed_flag':'bool',
        'device_restrictions':'str',
        'segmentation_upid_type':'int',
        'segmentation_upid':'str',
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

    LOGGER.info("SCTE35 Received in SPE: %s " % (sig_binary_data))

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
            if isinstance(scte_35_dict[main_key],list):
                for desc_number in range(0,len(scte_35_dict[main_key])):
                    for property_key in scte_35_dict[main_key][desc_number]:
                        if property_key == rule_condition_property:
                            scte35_property_value = scte_35_dict[main_key][desc_number][property_key]
            else:
                for property_key in scte_35_dict[main_key]:
                    if property_key == rule_condition_property:
                        scte35_property_value = scte_35_dict[main_key][property_key]


        rule_condition_value = value_type_validator(rule_condition_property,rule_condition_value)

        if scte35_property_value == "":
            scte35_property_value = 0

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
                # if scte35_property_value < rule_condition_value:
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
        try:
            action = dynamodb_to_json['default_behavior']
            custom_status_code['core:Note'] = "No conditioning rules at POIS, using default behavior"
        except:
            custom_status_code['core:Note'] = "Channel not configured in POIS DB"
        custom_status_code['@classCode'] = 0


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

        try:
            LOGGER.info("SCTE35 type : %s , segTypeId: %s " % (scte_35_dict['info_section']['splice_command_type'],scte_35_dict['descriptors'][0]['segmentation_type_id']))
        except:
            LOGGER.debug("Cant obtain type or segtypeid")

        #return scte_35_dict


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
                        custom_status_code['@classCode'] = 0
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

                                    properties_list = scte_35_dict[r_header]

                                    if len(properties_list) == 0:

                                        descriptors_dict[r_key] = r_value

                                    else:

                                        for p_list in range(0,len(properties_list)):
                                            properties_list[p_list][r_key] = r_value


                                    scte_35_dict[r_header] = properties_list

                                    #scte_35_dict[r_header].append(r_key+":"+str(r_value))
                                custom_status_code_rule_match += "rule %s replace param %s filled ." % (str(r),str(replace_param_number))
                                if '@classCode' in custom_status_code:
                                    if custom_status_code['@classCode'] != 2:
                                        custom_status_code['@classCode'] = 0
                                else:
                                    custom_status_code['@classCode'] = 0

                        # add to descriptors
                        if len(descriptors_dict) > 0:

                            scte_35_dict['descriptors'] = [descriptors_dict]

                    # encode scte35

                    #return scte_35_dict

                    cue = threefive.Cue()
                    cmd = threefive.TimeSignal()


                    if scte_35_dict['command']['splice_immediate_flag'] == True:
                        cmd.splice_immediate_flag = True
                    else:
                        cmd.time_specified_flag = True
                        cmd.pts_time = scte_35_dict['command']['pts_time']
                        cmd.pts_ticks = scte_35_dict['command']['pts_ticks']


                    cue.command = cmd

                    cue.info_section.pts_adjustment = scte_35_dict['info_section']['pts_adjustment']


                    #tsdescriptor = threefive.SegmentationDescriptor(bytes.fromhex('021D43554549000003E97FC3000120874E0109313133313433353332220101'))

                    tsdescriptor = threefive.SegmentationDescriptor(None)
                    tsdescriptor.provider_avail_id = 1
                    tsdescriptor.segmentation_event_id = 1
                    tsdescriptor.segmentation_duration_flag = True
                    tsdescriptor.delivery_not_restricted_flag = False
                    tsdescriptor.web_delivery_allowed_flag = False
                    tsdescriptor.no_regional_blackout_flag = False
                    tsdescriptor.archive_allowed_flag = True
                    tsdescriptor.segmentation_duration = 30
                    tsdescriptor.segmentation_duration_ticks = 2700000
                    tsdescriptor.segmentation_upid_type = 9
                    #tsdescriptor.segmentation_upid_length
                    tsdescriptor.segmentation_upid = 00
                    tsdescriptor.segmentation_type_id = 52
                    tsdescriptor.segment_num = 0
                    tsdescriptor.segments_expected = 0
                    tsdescriptor.sub_segment_num = 0
                    tsdescriptor.sub_segments_expected = 0
                    #tsdescriptor.


                    #     "segmentation_event_id":"None",
                    #   "segmentation_event_cancel_indicator":"None",
                    #   "component_count":"None",
                    #   "program_segmentation_flag":"None",
                    #   "segmentation_duration_flag":"None",
                    #   "delivery_not_restricted_flag":"None",
                    #   "web_delivery_allowed_flag":"None",
                    #   "no_regional_blackout_flag":"None",
                    #   "archive_allowed_flag":"None",
                    #   "device_restrictions":"None",
                    #   "segmentation_duration":"None",
                    #   "segmentation_duration_ticks":"None",
                    #   "segmentation_message":"None",
                    #   "segmentation_upid_type":"None",
                    #   "segmentation_upid_type_name":"None",
                    #   "segmentation_upid_length":"None",
                    #   "segmentation_upid":"None",
                    #   "segmentation_type_id":"None",
                    #   "segment_num":"None",
                    #   "segments_expected":"None",
                    #   "sub_segment_num":"None",
                    #   "sub_segments_expected":"None"

                    # # return str(tsdescriptor)
                    # tsdescriptor = threefive.SegmentationDescriptor(None)
                    # tsdescriptordata = {
                    #           "descriptor_length":0,
                    #           "name":"Segmentation Descriptor",
                    #           "identifier":"CUEI",
                    #         #   "bites":"None",
                    #           "provider_avail_id":1,
                    #           "components":[

                    #           ],
                    #           "segmentation_event_id":"100",
                    #           "segmentation_event_cancel_indicator":False,
                    #           "program_segmentation_flag":False,
                    #           "segmentation_duration_flag":True,
                    #           "delivery_not_restricted_flag":False,
                    #           "web_delivery_allowed_flag":False,
                    #           "no_regional_blackout_flag":False,
                    #           "archive_allowed_flag":True,
                    #           "device_restrictions":0,
                    #           "segmentation_duration":30,
                    #           "segmentation_duration_ticks":2700000,
                    #           "segmentation_message":"None",
                    #           "segmentation_upid_type":9,
                    #           "segmentation_upid_type_name":"None",
                    #           "segmentation_upid_length":0,
                    #           "segmentation_upid":0,
                    #           "segmentation_type_id":52,
                    #           "segment_num":0,
                    #           "segments_expected":0,
                    #           "sub_segment_num":0,
                    #           "sub_segments_expected":0
                    #         }

                    # tsdescriptor.load(tsdescriptordata)


                    # cue.descriptors.append(tsdescriptor)

                    cmd=threefive.TimeSignal()

                    if scte_35_dict['command']['splice_immediate_flag'] == True:
                        cmd.splice_immediate_flag = True
                        cmd.time_specified_flag = False
                    else:
                        cmd.time_specified_flag = True
                        cmd.pts_time = scte_35_dict['command']['pts_time']
                        cmd.pts_ticks = scte_35_dict['command']['pts_ticks']
                        cmd.break_auto_return = True



                    #cmd.pts_ticks = scte_35_dict['command']['pts_ticks']
                    cue.info_section.pts_adjustment = scte_35_dict['info_section']['pts_adjustment']
                    cue.command = cmd

                    try:
                        segmentation_event_id_scte = scte_35_dict['descriptors'][0]['segmentation_event_id']

                    except Exception as e:
                        segmentation_event_id_scte = str(int(time.time()/1000))

                    dscrptr = threefive.SegmentationDescriptor(None)
                    dscrptr.tag = 2
                    dscrptr.descriptor_length = 23
                    dscrptr.name = "Segmentation Descriptor"
                    dscrptr.identifier = "CUEI"
                    dscrptr.components = []
                    dscrptr.segmentation_event_id = segmentation_event_id_scte
                    dscrptr.segmentation_event_cancel_indicator = False
                    dscrptr.program_segmentation_flag = True
                    dscrptr.segmentation_duration_flag = True
                    dscrptr.segmentation_duration = 30.0

                    dscrptr.delivery_not_restricted_flag = False
                    dscrptr.web_delivery_allowed_flag = False
                    dscrptr.no_regional_blackout_flag = False
                    dscrptr.archive_allowed_flag = True
                    dscrptr.device_restrictions = "No Restrictions"
                    dscrptr.segmentation_message = "Provider Placement Opportunity Start"
                    dscrptr.segmentation_upid_type = 9
                    dscrptr.segmentation_upid_type_name = "Deprecated"
                    dscrptr.segmentation_upid_length = 0
                    dscrptr.segmentation_upid = ""
                    dscrptr.segmentation_type_id = 52
                    dscrptr.segment_num = 0
                    dscrptr.sub_segments_expected = 0
                    dscrptr.sub_segment_num = 0
                    dscrptr.segments_expected = 1

                    #dscrptr.load(descriptors)



                    cue.descriptors.append(dscrptr)



                    #return str(cue.show())
                    # binary_data = cue.encode()

                    # sig_binary_data = binary_data

                    scte_start = True
                    try:
                        if int(scte_35_dict['descriptors'][0]['segmentation_type_id']) == 53:
                            scte_start = False
                    except:
                        LOGGER.debug("Nothing")

                    if scte_start:
                        try:
                            #newcue = Cue()
                            #newcue.load(scte_35_dict)
                            sig_binary_data = cue.encode()
                            #sig_binary_data = newcue.encode()
                            LOGGER.info("SCTE35 encoded: %s " % (sig_binary_data))

                            custom_status_code['core:Note'] = custom_status_code_rule_match
                        except Exception as e:
                            LOGGER.warning("SCTE35 encode exception : %s " % (e))
                            action = dynamodb_to_json['default_behavior']
                            custom_status_code['@classCode'] = 2
                            custom_status_code['core:Note'] = "Unable to encode new SCTE35, using default behavior"
                    else:
                        LOGGER.info("SCTE35 encoded: %s " % (sig_binary_data))


                else:
                    action = dynamodb_to_json['default_behavior']
                    custom_status_code['@classCode'] = 0
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
    response_main_elements_attributes = {
        "@xmlns:xsi": "http://www.w3.org/2001/XMLSchema-instance",
        "@xmlns:sig": "urn:cablelabs:md:xsd:signaling:3.0",
        "@xmlns:core":"urn:cablelabs:md:xsd:core:3.0",
        "@xsi:schemaLocation": "urn:cablelabs:iptvservices:esam:xsd:common:1 OC-SP-ESAM- API-I0x-Common.xsd",
        "@xmlns": "urn:cablelabs:iptvservices:esam:xsd:common:1"
    }

    spn['SignalProcessingNotification'] = response_main_elements_attributes
    spn['SignalProcessingNotification']['ResponseSignal'] = resp_signal
    if len(custom_status_code) > 0:
        spn['SignalProcessingNotification']['StatusCode'] = custom_status_code


    # convert payload to xml for return
    spn_xml = xmltodict.unparse(spn, short_empty_elements=True, pretty=True)

    return {
        'statusCode': 200,
        "headers": {
            "Content-Type": "application/xml",
        },
        'body': spn_xml
    }
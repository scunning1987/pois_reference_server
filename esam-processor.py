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


    ''' common namespace headers
    xsi:schemaLocation="urn:cablelabs:iptvservices:esam:xsd:signal:1 OC-SP-ESAM- API-I0x-Signal.xsd"
    xmlns="urn:cablelabs:iptvservices:esam:xsd:signal:1"
    xmlns:sig="urn:cablelabs:md:xsd:signaling:3.0"
    xmlns:core="urn:cablelabs:md:xsd:core:3.0
    xmlns:content="urn:cablelabs:md:xsd:content:3.0"
    xmlns:common="urn:cablelabs:iptvservices:esam:xsd:common:1"
    xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
    '''

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

    # valid actions : replace , noop , delete
    action = "replace"


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

    def spn_replace(sig_new_binary_data):

        # parse scte

        '''
        scte_35_cue = threefive.Cue(sig_binary_data)
        scte_35_cue.decode()


        cue_dict = scte_35_cue.get()

        newcue = Cue()

        newcue.load(cue_dict)

        #newcue.encode()
        '''


        # Build Response Signal
        resp_signal = dict()

        # A REPLACE example
        resp_signal['@action'] = "replace"
        resp_signal['@acquisitionPointIdentity'] = acquisition_point_id
        resp_signal['@acquisitionSignalID'] = acquisition_signal_id
        resp_signal['@acquisitionTime'] = acquisition_time
        resp_signal['sig:UTCPoint'] = {}
        resp_signal['sig:UTCPoint']['@utcPoint'] = sig_utc_point
        resp_signal['sig:SCTE35PointDescriptor'] = {}
        resp_signal['sig:SCTE35PointDescriptor']['@spliceCommandType'] = "06"
        resp_signal['sig:SCTE35PointDescriptor']['sig:SegmentationDescriptorInfo'] = {}
        resp_signal['sig:SCTE35PointDescriptor']['sig:SegmentationDescriptorInfo']['@segmentEventId'] = "999"
        resp_signal['sig:SCTE35PointDescriptor']['sig:SegmentationDescriptorInfo']['@upidType'] = "9"
        resp_signal['sig:SCTE35PointDescriptor']['sig:SegmentationDescriptorInfo']['@upid'] = "SCOTT"
        resp_signal['sig:SCTE35PointDescriptor']['sig:SegmentationDescriptorInfo']['@segmentTypeId'] = "52"
        resp_signal['sig:SCTE35PointDescriptor']['sig:SegmentationDescriptorInfo']['@segmentNum'] = "0"
        resp_signal['sig:SCTE35PointDescriptor']['sig:SegmentationDescriptorInfo']['@segmentsExpected'] = "1"
        resp_signal['sig:SCTE35PointDescriptor']['sig:SegmentationDescriptorInfo']['@duration'] = "PT0M30S"
        resp_signal['sig:StreamTimes'] = sig_stream_times

        return resp_signal

    if action == "delete":
        resp_signal = spn_delete()
    elif action == "noop":
        resp_signal = spn_noop()
    elif action == "replace":
        sig_new_binary_data = ""
        resp_signal = spn_replace(sig_binary_data)
    else:
        LOGGER.info("requested action goes nowhere currently")


    # Create response SPN
    spn = dict()
    spn['SignalProcessingNotification'] = response_main_elements_attributes
    spn['SignalProcessingNotification']['ResponseSignal'] = resp_signal
    spn['SignalProcessingNotification']['ConditioningInfo'] = {}
    spn['SignalProcessingNotification']['ConditioningInfo']['@duration'] = "PT0M30S"


    # convert payload to xml for return
    spn_xml = xmltodict.unparse(spn, short_empty_elements=True, pretty=True)

    return {
        'statusCode': 200,
        "headers": {
            "Content-Type": "application/xml",
        },
        'body': spn_xml
    }

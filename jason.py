#!/usr/bin/env python3
import os
import sys
from simplified_scrapy import SimplifiedDoc
import xmltodict
from flask import Flask, make_response, request
import threefive
import bitn
from threefive import Cue
​
# The cheap_esam_server
# Here we will:
# 1. listen for ESAM requests
# 2. Parse the XML request for some key info:
#    - acquisition_point_identity
#    - acquisition_signal_id
#    - acquisition_time
#    - zone_identity
#    - utc_point
#    - scte35 data
# 3. We then parse the scte35 data
# 4. If we have a CONTENT_IDENTIFICATION segmentation_type_id we will:
#      build up a 'input switch' response to the specified alt_content_identity channel
#    else
#      build up a 'normal' response
​
help = ["-h", "--help"]
if len(sys.argv) != 2 or sys.argv[1] in help:
    print("usage: %s <server port_num>" % sys.argv[0])
    print("   ex: %s 5023" % sys.argv[0])
    sys.exit(1)
​
port_num = int(sys.argv[1])
esam_header = """<signal:SignalProcessingNotification xmlns:adi3="urn:cablelabs:md:xsd:core:3.0" xmlns:signaling="urn:cablelabs:md:xsd:signaling:3.0" xmlns:signal="urn:cablelabs:iptvservices:esam:xsd:signal:1" xmlns:manifest="urn:cablelabs:iptvservices:esam:xsd:manifest:1" xmlns:ns5="http://www.cablelabs.com/namespaces/metadata/xsd/confirmation/2" xmlns:common="urn:cablelabs:iptvservices:esam:xsd:common:1" xmlns:content="urn:cablelabs:md:xsd:content:3.0" xmlns:offer="urn:cablelabs:md:xsd:offer:3.0" xmlns:po="urn:cablelabs:md:xsd:placementopportunity:3.0" xmlns:terms="urn:cablelabs:md:xsd:terms:3.0" xmlns:title="urn:cablelabs:md:xsd:title:3.0">"""
​
api = Flask(__name__)
​
last_input = ""
​
​
​
@api.route('/lightweight_esam', methods=['POST', 'GET'])
def lightweight_esam():
    if request.method == 'POST':
        last_input=(request.get_data(as_text=True))
        print("last_input = {0}".format(last_input))

        xml_parse = SimplifiedDoc(last_input)

        tmp = xml_parse.selects('SignalProcessingEvent>AcquiredSignal')
        acquisition_point_identity = tmp[0]['acquisitionPointIdentity']
        acquisition_signal_id = tmp[0]['acquisitionSignalID']
        acquisition_time = tmp[0]['acquisitionTime']
        zone_identity = tmp[0]['zoneIdentity']

        tmp = xml_parse.selects('SignalProcessingEvent>AcquiredSignal>sig:UTCPoint')
        utc_point = tmp[0]['utcPoint']

        tmp = xml_parse.selects('SignalProcessingEvent>AcquiredSignal>sig:BinaryData')
        scte35_data = tmp[0]['html']

        pts_value = xml_parse.selects('SignalProcessingEvent>AcquiredSignal>sig:StreamTimes>sig:StreamTime')[0]['timeValue']

        cue = threefive.Cue(scte35_data)
        cue.decode()
        cue_dict = cue.get()

        newcue = Cue()

cue_dict["descriptors"][0]["delivery_not_restricted_flag"] = False
cue_dict["descriptors"][0]["web_delivery_allowed_flag"] = True
cue_dict["descriptors"][0]["no_regional_blackout_flag"] = True
cue_dict["descriptors"][0]["archive_allowed_flag"] = False
cue_dict["descriptors"][0]["device_restrictions"] = "No Restrictions"

newcue.load(cue_dict)
#cue.command.splice_event_id = 12
#cue.command.break_duration = float(65)

return_string = esam_header
return_string += '  <common:StatusCode classCode="0"/>'
return_string += "  <signal:ResponseSignal action=\"replace\" acquisitionPointIdentity=\"{0}\" acquisitionSignalID=\"{1}\" acquisitionTime=\"{2}\">".format(acquisition_point_identity, acquisition_signal_id, acquisition_time)
return_string += "    <signaling:UTCPoint utcPoint=\"{0}\"/>".format(utc_point)
return_string += "    <signaling:BinaryData signalType=\"SCTE35\">{0}</signaling:BinaryData>".format(str(newcue.encode())[2:-1])
return_string += "    <StreamTimes>"
return_string += "      <StreamTime timeType=\"HLS\" timeValue=\"{0}\"/>".format(str(pts_value))
return_string += "      <StreamTime timeType=\"PTS\" timeValue=\"{0}\"/>".format(str(pts_value))
return_string += "    </StreamTimes>"
return_string += "  </signal:ResponseSignal>"
return_string += "</signal:SignalProcessingNotification>"

print("sending out: {0}".format(return_string))
return return_string
else:
print("NOTE: Got http get")
answer = 'uh, this is really for responding to post messages'
response = make_response(answer)
response.headers['Content-Type'] = 'text/xml; charset=utf-8'
return response

if __name__ == '__main__':
    api.run(host="0.0.0.0",port=port_num)
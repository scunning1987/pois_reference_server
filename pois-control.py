import json
import boto3
import datetime
import logging
import math
import os
import xmltodict


LOGGER = logging.getLogger()
LOGGER.setLevel(logging.INFO)
'''
GET
../channels/get = get all channels
../channels/channel1 = get specific channel

PUT
../channels/create/channel1
../channels/update/channel

DELETE
../channels/delete/channel1
'''


def lambda_handler(event, context):
    LOGGER.info(event)


    # initialize BOTO3 Client for Dynamodb
    db_client = boto3.client('dynamodb')

    # Databases
    channeldb = os.environ['CHANNELDB']
    scheduledb = os.environ['SCHEDULEDB']

    # initialize a list to capture exceptions
    exceptions = []
    exceptions.clear()

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
        'segmentation_event_id':'',
        'segmentation_event_cancel_indicator':'',
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
        'segments_expected':'int'
    }

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

    # Response Structure
    def clientResponse(status_code,response_message):
        response_json = {
            'statusCode': status_code,
            "headers": {
                "Content-Type": "application/json",
            },
            'body': json.dumps(response_message)
        }
        return response_json

    # DynamoDB Get Item
    def dbGetSingleChannelInfo(channeldb,channel):
        LOGGER.debug("Doing a call to Dynamo to get channel information for channel : %s" % (channel))
        try:
            response = db_client.get_item(TableName=channeldb,Key={"channelid":{"S":channel}})
        except Exception as e:
            exceptions.append("Unable to get item from DynamoDB, got exception:  %s" % (str(e).upper()))
            return exceptions
        return response

    # DynamoDB Scan DB // get all items
    def dbGetAllChannelInfo(channeldb):
        LOGGER.debug("Doing a call to Dynamo to get all channels")
        try:
            response = db_client.scan(TableName=channeldb)
        except Exception as e:
            exceptions.append("error getting channel list from DynamoDB, got exception: %s" %  (e))
            return exceptions
        return response['Items']

    # DynamoDB Put Item // Create and update item
    def dbCreateUpdateSingleChannel(channeldb,item,channel):
        LOGGER.debug("Doing a call to Dynamo to get channel information for channel : %s" % (channel))
        try:
            response = db_client.put_item(TableName=channeldb,Item=item)
        except Exception as e:
            exceptions.append("Unable to create/update item in DynamoDB, got exception:  %s" % (str(e).upper()))
            return exceptions
        return response

    # DynamoDB Delete Item
    def dbDeleteSingleChannel(channeldb,channel):
        LOGGER.debug("Doing a call to Dynamo to delete channel record : %s" % (channel))
        try:
            response = db_client.delete_item(TableName=channeldb,Key={"channelid":{"S":channel}})
        except Exception as e:
            exceptions.append("Unable to delete item from DynamoDB, got exception:  %s" % (str(e).upper()))
            return exceptions
        return response


    if event['httpMethod'] == "GET":
        if event['path'] == "/pois/channels":

            LOGGER.info("The inbound request is to get a list of all channels configured in the POIS")
            # This is a request to return a list of all channels

            channels_information = dbGetAllChannelInfo(channeldb)
            if len(exceptions) > 0:
                LOGGER.error("Something went wrong")
                return clientResponse(502,exceptions)
            else:
                LOGGER.info("Returning response to client containing channnels")
                for i in range(0,len(channels_information)):
                    channels_information[i]
                    channel_information_json = dict()
                    dict_path(channel_information_json,channels_information[i])

                    channels_information[i] = channel_information_json
                return clientResponse(200,channels_information)

        elif "/pois/channels/" in event['path']:

            LOGGER.info("The inbound request is to get information on a specific channel")

            # Need to do some validation on the request url first
            path = event['path'].split("/")

            if len(path) != 4:
                exceptions.append({"Status":"Error performing task, expected url in format /pois/channels/[channel], got something different"})
                return clientResponse(502,exceptions)
            elif len(path[3]) < 2:
                exceptions.append({"Status":"Error performing task, not a suitable channel name, expecting more than a single character"})
                return clientResponse(502,exceptions)
            else:
                # lookup channel from DynamoDB
                channel = path[3]
                channel_information = dbGetSingleChannelInfo(channeldb,channel)
                if len(exceptions) > 0:
                    LOGGER.error("Something went wrong")
                    return clientResponse(502,exceptions)
                else:
                    LOGGER.info("Returning response to client containing channnels")

                    if "Item" not in channel_information:
                        channel_information = {"Status":"Channel does not exist"}
                        return clientResponse(200,channel_information)
                    else:

                        channel_information_json = dict()
                        dict_path(channel_information_json,channel_information['Item'])

                        #return clientResponse(200,channel_information['Item'])
                        return clientResponse(200,channel_information_json)

        else:
            exceptions.append({"Status":"The path you sent is not a supported api, please see the documentation for list of supported api paths"})
            return clientResponse(502,exceptions)

    if event['httpMethod'] == "PUT":


        # Need to do some validation on the request url first
        path = event['path'].split("/")

        if "/pois/channels/" in event['path']:
            if len(path) != 4:
                exceptions.append({"Status":"Error performing task, expected url in format /pois/channels/[channel], got something different"})
                return clientResponse(502,exceptions)
            else:

                ###
                ### Create or put new Channel, lots of validation required here
                ###
                payload = json.loads(event['body'])


                ##### VALIDATION START

                # check required top level keys first
                required_keys = ["default_behavior","esam_version"]


                for rkey in required_keys:
                    if rkey not in list(payload.keys()):
                        return clientResponse(502,{"status":"malformed request body, please refer to the template: required keys - default_behavior,esam_version"})


                default_behaviors = ["noop","delete"]

                valid_properties = []
                for key,value in threefive_scte_format.items():
                    for value in threefive_scte_format[key]:
                        valid_properties.append(value)

                if payload['default_behavior'] not in default_behaviors:
                    return clientResponse(502,{"status":"default_behavior value must be one of - noop , delete"})

                if "rules" in list(payload.keys()):
                    for esamrule in payload['rules']:
                        # check type = delete or replace
                        if esamrule['type'] not in ['replace','delete']:
                            return clientResponse(502,{"status":"malformed request body, for esam rule, type must be one of - delete, replace"})

                        if "condition" not in list(esamrule.keys()):
                            return clientResponse(502,{"status":"malformed request body, esam rule must have a condition key"})

                        if esamrule['type'] == "replace":
                            if "replace_params" not in list(esamrule.keys()):
                                return clientResponse(502,{"status":"malformed request body, esam rule type replace, you must have a key replace_params to indicate the result if the condition evaluates to true"})

                        if "operator" not in list(esamrule['condition'].keys()):

                            return clientResponse(502,{"status":"malformed request body, esam rule must have a operator key"})

                        if "value" not in list(esamrule['condition'].keys()):
                            return clientResponse(502,{"status":"malformed request body, esam rule must have a value key"})

                        if esamrule['condition']['operator'] not in ['=','>','<','-','!=']:
                            return clientResponse(502,{"status":"malformed request body, esam rule operator must be one of = , > , < , - , != "})

                        #if esamrule['condition']['operator'] not in supported_properties:
                        #    clientResponse(502,{"status":"malformed request body, esam rule property must be one of: %s " % (supported_properties)})

                        if esamrule['type'] == "replace":
                            if not isinstance(esamrule['replace_params'], list):
                                return clientResponse(502,{"status":"malformed request body, esam rule replace_params must be of type list"})

                            if esamrule['condition']['property'] not in valid_properties:
                                return clientResponse(502,{"status":"malformed request body, esam condition property is %s, must be one of %s " % (esamrule['condition']['property'],valid_properties)})

                            for replace_param in esamrule['replace_params']:

                                replace_property = list(replace_param.keys())[0]

                                if replace_property not in valid_properties:

                                    return clientResponse(502,{"status":"malformed request body, esam rule replace_param is %s, must be one of %s " % (replace_property,valid_properties)})

                        if esamrule['type'] == "delete":
                            if esamrule['condition']['property'] not in valid_properties:
                                return clientResponse(502,{"status":"malformed request body, esam condition property is %s, must be one of %s " % (esamrule['condition']['property'],valid_properties)})

                ##### VALIDATION END
                # If we get here then we can write the item to the Db
                LOGGER.info("Passed Validation, now proceeding to Create/update record in DynamoDB")

                channel = event['path'].split("/")[-1]
                payload['channelid'] = channel
                item = payload


                # DYNAMO DB JSON BUILDER
                dynamodb_item = dict()
                keylist = []
                def dict_path(dicttopopulate,my_dict):
                    for k,v in my_dict.items():

                        if isinstance(v,dict):
                            dynamodb_item_subdict = dict()
                            dict_path(dynamodb_item_subdict,v)

                            v = dynamodb_item_subdict
                            dicttopopulate.update({k:{"M":v}})

                        elif isinstance(v,str):
                            dicttopopulate.update({k:{"S":v}})
                        elif isinstance(v,list):
                            for i in range(0,len(v)):
                                dynamodb_item_list = dict()
                                dict_path(dynamodb_item_list,v[i])

                                v[i] = {"M":dynamodb_item_list}

                            dicttopopulate.update({k:{"L":v}})

                dict_path(dynamodb_item,item)

                # PUT DB Item
                channel_create_update = dbCreateUpdateSingleChannel(channeldb,dynamodb_item,channel)

                if len(exceptions) > 0:
                    LOGGER.error("Something went wrong")
                    return clientResponse(502,exceptions)
                else:
                    success_message = "Channel created/updated successfully"
                    return clientResponse(200,{"status":success_message})
                    LOGGER.info("Returning response to client containing channnels")


                ###
                ### End of create/put channel section
                ###

        else:
            exceptions.append({"Status":"The path you sent is not a supported api, please see the documentation for list of supported api paths"})
            return clientResponse(502,exceptions)

    if event['httpMethod'] == "DELETE":

        # need to do some validation on teh request url first
        path = event['path'].split("/")

        if "/pois/channels/" in event['path']:

            if len(path) != 4:
                exceptions.append({"Status":"Error performing task, expected url in format /pois/channels/[channel], got something different"})
                return clientResponse(502,exceptions)
            else:

                # DELETE Channel
                channel = path[-1]

                channel_information = dbDeleteSingleChannel(channeldb,channel)

                if len(exceptions) > 0:
                    LOGGER.error("Something went wrong")
                    return clientResponse(502,exceptions)
                else:
                    LOGGER.info("Successfully deleted the channel")
                    channel_information = {"status":"success"}
                    return clientResponse(200,{"status":"Channel deleted successfully"})



        else:
            exceptions.append({"Status":"The path you sent is not a supported api, please see the documentation for list of supported api paths"})
            return clientResponse(502,exceptions)



    return clientResponse(502,{"status":"method not supported"})
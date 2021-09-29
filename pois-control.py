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
                        return clientResponse(200,channel_information['Item'])

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

                        supported_properties = [
                            "pts_adjustment",
                            "splice_command_type",
                            "name",
                            "time_specified_flag",
                            "pts_time",
                            "break_auto_return",
                            "break_duration",
                            "splice_event_id",
                            "splice_event_cancel_indicator",
                            "out_of_network_indicator",
                            "program_splice_flag",
                            "duration_flag",
                            "splice_immediate_flag",
                            "unique_program_id",
                            "avail_num",
                            "avail_expected",
                            "delivery_not_restricted_flag",
                            "web_delivery_allowed_flag",
                            "no_regional_blackout_flag",
                            "device_restrictions",
                            "segmentation_duration",
                            "segmentation_upid_type",
                            "segmentation_type_id",
                            "segmentation_upid"
                        ]


                        if esamrule['condition']['operator'] not in supported_properties:
                            clientResponse(502,{"status":"malformed request body, esam rule property must be one of: %s " % (supported_properties)})

                        if esamrule['type'] == "replace":
                            if not isinstance(esamrule['replace_params'], list):
                                clientResponse(502,{"status":"malformed request body, esam rule replace_params must be of type list"})

                            for replace_param in esamrule['replace_params']:

                                replace_property = list(replace_param.keys())[0]

                                if replace_property not in supported_properties:
                                    clientResponse(502,{"status":"malformed request body, esam rule replace_param properties must be one of %s " % (supported_properties)})


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
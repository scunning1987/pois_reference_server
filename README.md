# POIS Reference Server
## Release Notes
| Date         | Version | Update Notes |
|--------------|---------|--------------|
| 2020-06-08   | 1.0.27  | Initial release containing support for ad rules only. API configuration only - See API section for details

## Overview
This is a POIS reference server capable of interacting with a transcoder using ESAM protocol.

It is intended to be used for testing purposes only.

## Functionality
The functionality includes:
* SCTE35 filtering (deleting unwanted,, passing through noop):
    * UPID value
    * segmentation type id (program,network, provider advertisement id,chapter start/stop)
    * splice command type (5,6)
    * UPID type


* Modify / Replace (free text entry with parameter validation)
    * splice command type 5 → 6
    * modify certain characteristics:
        * example: web_delivery_not_allowed
            * be aware that this may add more metadata to payload that needs to be taken into consideration
        * Adding/modifying segmentation_upid value
* SCTE35 driven archives
    * Utilizing the Elemental Live API (requires public facing Elemental Live IP or NAT proxy)
* SCTE35 input switching
    * Synchronous using inbound SCTE35 messages (Source switch logged in schedule stored in database)

## Architecture
The architecture consists of several AWS services and is deployed using a CloudFormation template.

![](Architecture-pois-ref-server.png?width=60pc&classes=border,shadow)

For OTT workflows, AWS Elemental Live needs to contribute the ESAM conditioned stream to AWS. Then through MediaConnect, MediaLive and MediaPackage/MediaStore can the OTT package be generated.

![](Architecture-pois-ref-server-aws-video.png?width=60pc&classes=border,shadow)

## How to deploy
Use [this CloudFormation template](pois-ref-server.yaml) to deploy the solution.. Please note, the only resources deployed in this solution pertain to the POIS. Deployed Resources include:

* AWS Lambda Functions x 2
* AWS Lambda Layers * 2
* Amazon API Gateway REST API
* DynamoDB Channels Database
* DynamoDB Schedule Database
* Amazon S3 ****bucket deployed to host UI ... later phase*
* AWS IAM Role & Policy creation for Lambda Functions

## API Control
At initial release, the only option to configure the POIS is via API. Once you've deployed the CloudFormation stack, check the **Outputs** tab. This will contain your API Endpoints.

![](cloudformation-output.png?width=60pc&classes=border,shadow)

The two most important output values are the **ESAMEndpoint** which you will configure in your encoder, and the **ChannelConfigurationAPI** value.  This is the API endpoint you need to use to start setting up the POIS.

| Method  | Path             | Description |
|---------|------------------|-------------|
| GET     | /pois/channnels  | Returns configuration of all channels in the POIS |
| GET     | /pois/channnels/{channel-name} | Where {channel-name} is the name of your channel, this will return the configuration of the channel |
| PUT     | /pois/channels/{channel-name} | Where {channel-name} is the name of your channel, this will create/update the channel configuration in the POIS |
| DELETE  | /pois/channels/{channel-name} | Where {channel-name} is the name of your channel, this will delete the channel from the POIS database

You can download the POSTman collection for this API set here: https://www.getpostman.com/collections/8b0ebbb7d238132034ae

## Creating SCTE35 Rules
The POIS offers some rudimentary capabilities for SCTE35 signal conditioning and deleting. Here are some tips when configuring your rules:
* The rules are processed in order, starting with the first rule in the list submitted via the API
* The rule format is as follows:
  - You first specify an action type. What do you want to happen if the rule evaluates to TRUE (supported actions: replace, delete)
  - Then comes the condition; what do you want to evaluate. Currently the application supports simple checks, ie. splice_command_type = 5 (splice_command_type is the **property**, 5 is the **value**, and the operator is **=**)
  - For **replace** actions, you also need to submit with the json a **replace_params** list. If the evaluation of the rule is True, then the SCTE properties listed in the **replace_params** list will all be used to modify the SCTE35 binary  
* You can mix and match action types in your rules. IE. you can have a DELETE rule to delete all splice_command_type 5 signals, then have a subsequent rule to modify duration  
* It's recommended that any DELETE rules should be at earlier indexes in list than REPLACE rules
* There is currently no checking to see if rules you submit contradict each other, so please validate this before you configure the channel or you may not get the desired results
* For major replace actions, ie. if splice_command_type = 5, modify to splice_command_type = 6, the code does not fill in any blanks, so make sure your **replace_params** list contains all the new properties you want included in the new SCTE35 binary
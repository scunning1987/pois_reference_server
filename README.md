# POIS Reference Server
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
Use [this CloudFormation template](pois-ref-server.yaml) to deploy the solution
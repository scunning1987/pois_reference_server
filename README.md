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
        * Adding/modifying upid value
* SCTE35 driven archives
    * cool
* SCTE35 input switching
    * Synchronous (schedule checked)

## Architecture
The architecture consists of several AWS services and is deployed using a CloudFormation template.

![](Architecture-pois-ref-server.png?width=60pc&classes=border,shadow)


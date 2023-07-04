# Live Ad Pre-Roll with broadpeak.io and Bitmovin

## Description

The set of scripts in this folder generate a broadpeak.io Live Pre-Roll service for monetisation of Live content transcoded by the Bitmovin Live Encoder. 

Features:
- Assets are transcoded into ABR ladders with DASH and/or HLS. 
- The broadpeak.io service is configured to transcode ads served by a VAST-compliant ad server. 
- Ads are inserted before the live content

Optionally, the script can generate a dummy live contribution feed and push it to the Bitmovin Live encoder, for ease of testing.


## Pre-Requisites

1. An active broadpeak.io account. You can sign up for a trial at https://app.broadpeak.io/signup
2. An active Bitmovin account. You can sign up for a trial at https://dashboard.bitmovin.com/signup
3. A local installation of FFmpeg if using the dummy live contribution feed functionality in this script

## How to run this code

1. Modify the config.py file with the appropriate information (read the comments in that file for guidance)
2. Define environment variables as appropriate for secret information (eg. API tokens and S3 credentials) - see the config.py file to determine what variables are required
3. Execute the script

```python3 main.py```

### Notes
- The script can be used to generate one-off resourced in broadpeak.io and Bitmovin (such as Ad Server, S3 Output, etc), allowing the script to be used with virgin accounts. It is recommended however that after initial execution, or configuration of those resources in the service UIs, the identifiers of these resources are collected and added to the config.py file, to prevent exceptions being raised due to duplication of resources
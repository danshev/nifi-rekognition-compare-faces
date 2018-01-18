# RekognitionCompareFaces #

Apache NiFi processor for calling Amazon's "Compare Faces" Rekognition service.

## Installation ##

 1. Download complied NAR into NiFi's `/lib/` directory.
 2. Change permissions on the file (>`chmod 755 nifi-rekognitioncomparefaces-nar-1.0-SNAPSHOT.nar)
 3. Restart NiFi (`/bin/nifi.sh restart`)

## Configuration ##

After NiFi restarts and you've added the processor to your canvas ...

 1. Add your S3 credentials, just as you would with NiFi's FetchS3Object processor.
 2. Define `Source` and `Target` image sources:
  - S3Object <> S3Object
  - S3Object <> Base64-encoded string
  - Base64-encoded string <> S3Object
  - Base64-encoded string <> Base64-encoded string
  
  
  ### Notes ###
  This processor was developed for a limited, go / no-go use-case.  
  Currently, the processor simply checks whether the length of the FaceMatches
  response is non-zero. If if is, then the FlowFile is routed to the `Matched`
  relationship.  If no faces are found (throwing an InvalidParameters exception) 
  or the FaceMatches response is empty, the FlowFile is routed to the `No Matches`
  relationship.
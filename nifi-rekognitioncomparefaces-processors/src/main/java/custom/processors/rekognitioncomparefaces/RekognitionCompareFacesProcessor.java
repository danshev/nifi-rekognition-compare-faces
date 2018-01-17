/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package custom.processors.rekognitioncomparefaces;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.util.StandardValidators;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.rekognition.AmazonRekognition;
import com.amazonaws.services.rekognition.AmazonRekognitionClientBuilder;
import com.amazonaws.services.rekognition.model.CompareFacesMatch;
import com.amazonaws.services.rekognition.model.CompareFacesRequest;
import com.amazonaws.services.rekognition.model.CompareFacesResult;
import com.amazonaws.services.rekognition.model.Image;
import com.amazonaws.services.rekognition.model.S3Object;
import com.amazonaws.util.Base64;
import com.amazonaws.util.StringUtils;

import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

@Tags({"example"})
@CapabilityDescription("Provide a description")
@SeeAlso({})
@ReadsAttributes({@ReadsAttribute(attribute="", description="")})
@WritesAttributes({@WritesAttribute(attribute="", description="")})
public class RekognitionCompareFacesProcessor extends AbstractProcessor {

    public static final PropertyDescriptor S3_KEY = new PropertyDescriptor
            .Builder().name("S3_KEY")
            .displayName("S3 Access Key")
            .description("S3 Access Key for S3 Bucket")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor S3_SECRET = new PropertyDescriptor
            .Builder().name("S3_SECRET")
            .displayName("S3 Secret")
            .description("S3 Secret for S3 Bucket")
            .required(true)
            .sensitive(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor REGION = new PropertyDescriptor
            .Builder().name("REGION")
            .displayName("Region")
            .description("S3 Region")
            .required(true)
            .allowableValues(getRegions())
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor SOURCE_BUCKET = new PropertyDescriptor
            .Builder().name("SOURCE_BUCKET")
            .displayName("S3 Source Bucket")
            .description("S3 Source Bucket")
            .required(false)
            .expressionLanguageSupported(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor SOURCE_BUCKET_OBJECT_KEY = new PropertyDescriptor
            .Builder().name("SOURCE_BUCKET_OBJECT_KEY")
            .displayName("S3 Source Bucket Object Key")
            .description("S3 Source Bucket Object Key")
            .required(false)
            .expressionLanguageSupported(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    
    public static final PropertyDescriptor TARGET_BUCKET = new PropertyDescriptor
            .Builder().name("TARGET_BUCKET")
            .displayName("S3 Target Bucket")
            .description("S3 Target Bucket")
            .required(false)
            .expressionLanguageSupported(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor TARGET_BUCKET_OBJECT_KEY = new PropertyDescriptor
            .Builder().name("TARGET_BUCKET_OBJECT_KEY")
            .displayName("S3 Target Bucket Object Key")
            .description("S3 Target Bucket Object Key")
            .required(false)
            .expressionLanguageSupported(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();


    public static final PropertyDescriptor SOURCE_BASE64 = new PropertyDescriptor
            .Builder().name("SOURCE_BASE64")
            .displayName("Source Base64")
            .description("Source Base64 from Attribute")
            .expressionLanguageSupported(true)
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor TARGET_BASE64 = new PropertyDescriptor
            .Builder().name("TARGET_BASE64")
            .displayName("Target Base64")
            .description("Target Base64 from Attribute")
            .expressionLanguageSupported(true)
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final Relationship MATCH = new Relationship.Builder()
            .name("MATCH")
            .description("Match")
            .build();
    
    public static final Relationship NO_MATCH = new Relationship.Builder()
            .name("NO_MATCH")
            .description("No Match")
            .build();

    private List<PropertyDescriptor> descriptors;
    
    private static AmazonRekognition client = null;

    private Set<Relationship> relationships;
    
    private static Set<String> getRegions() {
    	Set<String> regions = new TreeSet<String>();
    	for (Regions r : Regions.values()) {
    		regions.add(r.getName());
    	}
    	
    	return regions;
    }

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> descriptors = new ArrayList<PropertyDescriptor>();
        descriptors.add(S3_KEY);
        descriptors.add(S3_SECRET);
        descriptors.add(REGION);
        descriptors.add(SOURCE_BUCKET);
        descriptors.add(SOURCE_BUCKET_OBJECT_KEY);
        descriptors.add(TARGET_BUCKET);
        descriptors.add(TARGET_BUCKET_OBJECT_KEY);
        descriptors.add(SOURCE_BASE64);
        descriptors.add(TARGET_BASE64);
        this.descriptors = Collections.unmodifiableList(descriptors);

        final Set<Relationship> relationships = new HashSet<Relationship>();
        relationships.add(MATCH);
        relationships.add(NO_MATCH);
        this.relationships = Collections.unmodifiableSet(relationships);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return this.relationships;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {
        String key = context.getProperty(S3_KEY).getValue();
        String secret = context.getProperty(S3_SECRET).getValue();
        String region = context.getProperty(REGION).getValue();
                
        BasicAWSCredentials awsCreds = new BasicAWSCredentials(key, secret);
        client = AmazonRekognitionClientBuilder.standard().withRegion(region).withCredentials(new AWSStaticCredentialsProvider(awsCreds)).build();
        
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if ( flowFile == null ) {
            return;
        }
        
        String source_bucket = context.getProperty(SOURCE_BUCKET).evaluateAttributeExpressions(flowFile).getValue();
        String source_bucket_key = context.getProperty(SOURCE_BUCKET_OBJECT_KEY).evaluateAttributeExpressions(flowFile).getValue();
        String target_bucket = context.getProperty(TARGET_BUCKET).evaluateAttributeExpressions(flowFile).getValue();
        String target_bucket_key = context.getProperty(TARGET_BUCKET_OBJECT_KEY).evaluateAttributeExpressions(flowFile).getValue();

        String source_b64 = context.getProperty(SOURCE_BASE64).evaluateAttributeExpressions(flowFile).getValue();
        String target_b64 = context.getProperty(SOURCE_BASE64).evaluateAttributeExpressions(flowFile).getValue();
        
        CompareFacesRequest request = null;
        
        if (StringUtils.isNullOrEmpty(source_b64) && StringUtils.isNullOrEmpty(target_b64)) {
            request = new CompareFacesRequest()
                    .withSourceImage(new Image().withS3Object(new S3Object().withBucket(source_bucket).withName(source_bucket_key)))
                    .withTargetImage(new Image().withS3Object(new S3Object().withBucket(target_bucket).withName(target_bucket_key))).withSimilarityThreshold(80f);

        }
        else if (StringUtils.isNullOrEmpty(source_b64)) {
        	byte[] decoded = Base64.decode(source_b64);
        	ByteBuffer buf = ByteBuffer.wrap(decoded);
        	
            request = new CompareFacesRequest()
                    .withSourceImage(new Image().withBytes(buf))
                    .withTargetImage(new Image().withS3Object(new S3Object().withBucket(target_bucket).withName(target_bucket_key))).withSimilarityThreshold(80f);
        }
        else if (StringUtils.isNullOrEmpty(target_b64)) {
        	byte[] decoded = Base64.decode(target_b64);
        	ByteBuffer buf = ByteBuffer.wrap(decoded);
        	
            request = new CompareFacesRequest()
                    .withSourceImage(new Image().withS3Object(new S3Object().withBucket(source_bucket).withName(source_bucket_key)))
                    .withTargetImage(new Image().withBytes(buf)).withSimilarityThreshold(80f);
        }
        
        CompareFacesResult response = client.compareFaces(request);
        
        List<CompareFacesMatch> faces = response.getFaceMatches();
        
        for (CompareFacesMatch f : faces) {
        	
        }
        if (faces.size() > 0) {
        	session.transfer(flowFile, MATCH);
        }
        else {
        	session.transfer(flowFile, NO_MATCH);
        }
    }
}

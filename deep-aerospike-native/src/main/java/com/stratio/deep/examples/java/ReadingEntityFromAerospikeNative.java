/*
 * Copyright 2014 Aerospike, Inc.
 *
 * Portions may be licensed to Aerospike, Inc. under one or more
 * contributor license agreements.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */
package com.stratio.deep.examples.java;

import com.stratio.deep.aerospike_native.config.AerospikeConfigFactory;
import com.stratio.deep.aerospike_native.config.AerospikeDeepJobConfig;
import com.stratio.deep.aerospike_native.extractor.AerospikeNativeEntityExtractor;
import com.stratio.deep.core.context.DeepSparkContext;
import com.stratio.deep.examples.utils.ContextProperties;
import com.stratio.deep.examples.utils.MessageTestEntity;

import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;

/**
 * Example for reading an IDeepEntity with Aerospike Native extractor.
 */
public class ReadingEntityFromAerospikeNative {

    private static final Logger LOG = Logger.getLogger(ReadingEntityFromAerospikeNative.class);

    public static void main(String[] args) {
        doMain(args);
    }

    public static void doMain(String[] args) {
        String job = "java:readingEntityFromAerospikeNative";

        String host = "localhost";
        Integer port = 3000;
        String namespace = "test";
        String set = "message";

        // Creating the Deep Context where args are Spark Master and Job Name
        ContextProperties p = new ContextProperties(args);
        DeepSparkContext deepContext = new DeepSparkContext(p.getCluster(), job, p.getSparkHome(),
                p.getJars());

        AerospikeDeepJobConfig<MessageTestEntity> inputConfigEntity =
                AerospikeConfigFactory.createAerospike(MessageTestEntity.class).host(host).port(port).namespace(namespace)
                        .set(set).initialize();
        inputConfigEntity.setExtractorImplClass(AerospikeNativeEntityExtractor.class);

        JavaRDD<MessageTestEntity> inputRDDEntity = deepContext.createJavaRDD(inputConfigEntity);

        LOG.info("count : " + inputRDDEntity.cache().count());

        deepContext.stop();

    }
}

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
package com.stratio.deep.examples.scala

import com.stratio.deep.aerospike.direct.config.{AerospikeConfigFactory, AerospikeDeepJobConfig}
import com.stratio.deep.aerospike.direct.extractor.AerospikeNativeEntityExtractor
import com.stratio.deep.core.context.DeepSparkContext
import com.stratio.deep.examples.utils.{MessageTestEntity, ContextProperties}
import org.apache.spark.rdd.RDD

/**
 * Example of writing an Entity to Aerospike using the native extractor.
 */
object WritingEntityToAerospikeNative {

  def main(args: Array[String]) {
    doMain(args)
  }

  def doMain(args: Array[String]) {
    val job: String = "scala:writingEntityToAerospikeNative"
    val host: String = "localhost"
    val port: Integer = 3000
    val namespace: String = "test"
    val inputSet: String = "message"
    val outputSet: String = "messageOutput"

    val p: ContextProperties = new ContextProperties(args)

    val deepContext: DeepSparkContext = new DeepSparkContext(p.getCluster, job, p.getSparkHome, p.getJars)

    val inputConfigEntity: AerospikeDeepJobConfig[MessageTestEntity] = AerospikeConfigFactory.createAerospike(classOf[MessageTestEntity]).host(host).port(port).namespace(namespace).set(inputSet)
    inputConfigEntity.setExtractorImplClass(classOf[AerospikeNativeEntityExtractor[MessageTestEntity]])
    inputConfigEntity.initialize

    val inputRDDEntity: RDD[MessageTestEntity] = deepContext.createJavaRDD(inputConfigEntity)

    val outputConfigEntity: AerospikeDeepJobConfig[MessageTestEntity] = AerospikeConfigFactory.createAerospike(classOf[MessageTestEntity]).host(host).port(port).namespace(namespace).set(outputSet)
    outputConfigEntity.setExtractorImplClass(classOf[AerospikeNativeEntityExtractor[MessageTestEntity]])
    outputConfigEntity.initialize

    DeepSparkContext.saveRDD(inputRDDEntity, outputConfigEntity)

    deepContext.stop

  }

}

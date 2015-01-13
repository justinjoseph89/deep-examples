package com.stratio.deep.examples.java;

import com.stratio.deep.aerospike.direct.config.AerospikeConfigFactory;
import com.stratio.deep.aerospike.direct.config.AerospikeDeepJobConfig;
import com.stratio.deep.core.context.DeepSparkContext;
import com.stratio.deep.examples.utils.ContextProperties;
import com.stratio.deep.examples.utils.MessageTestEntity;
import org.apache.log4j.Logger;
import org.apache.spark.rdd.RDD;
import scala.Tuple2;

import java.util.List;

/**
 * Example of writing an Entity to Aerospike using the native extractor.
 */
public class WritingEntityToAerospikeNative {

    private static final Logger LOG = Logger.getLogger(WritingEntityToAerospikeNative.class);
    public static List<Tuple2<String, Integer>> results;

    public static void main(String[] args) {
        doMain(args);
    }

    public static void doMain(String[] args) {
        String job = "java:writingEntityToAerospikeNative";

        String host = "localhost";
        Integer port = 3000;

        String namespace = "test";
        String inputCollection = "message";
        String outputCollection = "messageOutput";

        // Creating the Deep Context where args are Spark Master and Job Name
        ContextProperties p = new ContextProperties(args);
        DeepSparkContext deepContext = new DeepSparkContext(p.getCluster(), job, p.getSparkHome(),
                p.getJars());

        AerospikeDeepJobConfig<MessageTestEntity> inputConfigEntity =
                AerospikeConfigFactory.createAerospike(MessageTestEntity.class).host(host).port(port).namespace(namespace)
                        .set(inputCollection);

        RDD<MessageTestEntity> inputRDDEntity = deepContext.createRDD(inputConfigEntity);

        AerospikeDeepJobConfig<MessageTestEntity> outputConfigEntity =
                AerospikeConfigFactory.createAerospike(MessageTestEntity.class).host(host).port(port).namespace(namespace)
                        .set(outputCollection);

        deepContext.saveRDD(inputRDDEntity, outputConfigEntity);

        deepContext.stop();
    }
}

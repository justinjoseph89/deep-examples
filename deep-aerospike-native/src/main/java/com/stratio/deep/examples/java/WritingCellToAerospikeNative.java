package com.stratio.deep.examples.java;

import com.stratio.deep.aerospike.direct.config.AerospikeConfigFactory;
import com.stratio.deep.aerospike.direct.config.AerospikeDeepJobConfig;
import com.stratio.deep.aerospike.direct.extractor.AerospikeNativeEntityExtractor;
import com.stratio.deep.core.context.DeepSparkContext;
import com.stratio.deep.examples.utils.ContextProperties;
import org.apache.log4j.Logger;
import org.apache.spark.rdd.RDD;

/**
 * Simple example for writing a Cell to Aerospike using the native extractor.
 */
public class WritingCellToAerospikeNative {

    private static final Logger LOG = Logger.getLogger(WritingCellToAerospikeNative.class);

    private WritingCellToAerospikeNative() {
    }

    public static void main(String[] args) {
        doMain(args);
    }

    public static void doMain(String[] args) {
        String job = "java:writingCellToAerospikeNative";

        String host = "127.0.0.1";
        int port = 3000;

        String namespace = "book";
        String set = "input";

        // Creating the Deep Context where args are Spark Master and Job Name
        ContextProperties p = new ContextProperties(args);
        DeepSparkContext deepContext = new DeepSparkContext(p.getCluster(), job, p.getSparkHome(),
                p.getJars());

        AerospikeDeepJobConfig inputConfigCell = AerospikeConfigFactory.createAerospike().host(host).port(port)
                .namespace(namespace)
                .set(set);
        inputConfigCell.setExtractorImplClass(AerospikeNativeEntityExtractor.class);

        RDD inputRDDCell = deepContext.createRDD(inputConfigCell);

        LOG.info("count : " + inputRDDCell.count());
        LOG.info("prints first cell : " + inputRDDCell.first());

        AerospikeDeepJobConfig outputConfigCell = AerospikeConfigFactory.createAerospike().host(host).port(port)
                .namespace(namespace)
                .set(set).initialize();

        deepContext.saveRDD(inputRDDCell, outputConfigCell);

        RDD outputRDDCell = deepContext.createRDD(outputConfigCell);

        LOG.info("count output : " + outputRDDCell.count());
        LOG.info("prints first output cell: " + outputRDDCell.first());

        deepContext.stop();

    }
}

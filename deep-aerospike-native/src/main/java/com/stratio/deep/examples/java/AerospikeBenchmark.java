package com.stratio.deep.examples.java;

import com.stratio.deep.aerospike.direct.config.AerospikeConfigFactory;
import com.stratio.deep.aerospike.direct.config.AerospikeDeepJobConfig;
import com.stratio.deep.aerospike.direct.extractor.AerospikeCellExtractor;
import com.stratio.deep.aerospike.direct.extractor.AerospikeNativeCellExtractor;
import com.stratio.deep.commons.entity.Cell;
import com.stratio.deep.commons.entity.Cells;
import com.stratio.deep.core.context.DeepSparkContext;
import com.stratio.deep.examples.utils.ContextProperties;
import org.apache.spark.rdd.RDD;

import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by mariomgal on 05/01/15.
 */
public class AerospikeBenchmark {

    private AerospikeBenchmark() {
    }

    public static void main(String[] args) {
        doMain(args);
    }

    public static void doMain(String[] args) {

        for(int i=0; i<=6; i++) {
            Runtime runtime = Runtime.getRuntime();

            NumberFormat format = NumberFormat.getInstance();

            StringBuilder sb = new StringBuilder();
            long maxMemory = runtime.maxMemory();
            long allocatedMemory = runtime.totalMemory();
            long freeMemory = runtime.freeMemory();

            System.out.println("free memory: " + format.format(freeMemory / 1024));
            System.out.println("allocated memory: " + format.format(allocatedMemory / 1024));
            System.out.println("max memory: " + format.format(maxMemory / 1024));
            System.out.println("total free memory: " + format.format((freeMemory + (maxMemory - allocatedMemory)) / 1024));

            Double cells = Math.pow(10, i);
            System.out.println("STARTING ITERATION WITH " + i + " CELLS");
            String job = "scala:writing" + cells + "CellsToAerospikeNative";
            String host = "localhost";
            int port = 3000;
            String namespace = "test";
            String inputSet = "input";
            String outputSet = "output_" + cells;

            ContextProperties p = new ContextProperties(args);
            System.out.println("Cluster:" + p.getCluster());
            System.out.println("Spark home:" + p.getSparkHome());
            System.out.println("Jars: " + p.getJars());
            DeepSparkContext deepContext = new DeepSparkContext(p.getCluster(), job, p.getSparkHome(), p.getJars());

            AerospikeDeepJobConfig<Cells> outputConfigEntity = AerospikeConfigFactory.createAerospike().host(host).port(port).namespace(namespace).set(outputSet);

            // COMMENT/UNCOMMENT THE EXTRACTOR CLASS YOU WANT TO TEST
            outputConfigEntity.setExtractorImplClass(AerospikeCellExtractor.class);
            //outputConfigEntity.setExtractorImplClass(AerospikeNativeCellExtractor.class);
            outputConfigEntity.initialize();

            // UNCOMMENT THIS BLOCK FOR TESTING WRITE OPERATIONS

//            List<Cells> cellsToWrite = new ArrayList<>(cells.intValue());
//            for(int j=0; j < cells.intValue(); j++) {
//                Cells row = new Cells(outputSet);
//                Cell  cell1 = Cell.create("cell1", j, true);
//                row.add(cell1);
//                Cell cell2 = Cell.create("cell2", "cell2", false);
//                row.add(cell2);
//                Cell cell3 = Cell.create("cell3", System.currentTimeMillis(), false);
//                row.add(cell3);
//                Cell cell4 = Cell.create("cell4", "cell4", false);
//                row.add(cell4);
//                cellsToWrite.add(row);
//            }
//
//            System.out.println("List of " + cells + " created");
//
//            RDD<Cells> rddCells = deepContext.parallelize(cellsToWrite, 4).rdd();
//            System.out.println("Going to write " + cells + " to " + outputSet);
//
//            long startTime = System.nanoTime();
//            deepContext.saveRDD(rddCells, outputConfigEntity);
//            long endTime = System.nanoTime();
//
//            System.out.println(cells + " written in " + (endTime - startTime) + " nanoseconds");
//            cellsToWrite = null;


            // UNCOMMENT THIS BLOCK FOR TESTING READ OPERATIONS

            System.out.println("--------------------------------------------");

            System.out.println("Going to read " + cells + " to " + outputSet);

            long startTime = System.nanoTime();
            RDD<Cells> readCells = deepContext.createRDD(outputConfigEntity);
            System.out.println(readCells.count());
            long endTime = System.nanoTime();

            System.out.println(cells + " read in " + (endTime - startTime) + "nanoseconds");
            System.out.println("--------------------------------------------");


            // LEAVE THIS LINES UNCOMMENTED
            System.gc();
            deepContext.stop();
            System.out.println("SPARK CONTEXT STOPPED");


        }

    }


}

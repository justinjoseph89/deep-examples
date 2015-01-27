/*
 * Copyright 2014, Stratio.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.stratio.deep.examples.java.factory;

import com.stratio.deep.commons.filter.Filter;
import com.stratio.deep.core.context.DeepSparkContext;
import com.stratio.deep.examples.utils.ContextProperties;
import com.stratio.deep.jdbc.config.JdbcNeo4JConfigFactory;
import com.stratio.deep.jdbc.config.JdbcNeo4JDeepJobConfig;
import org.apache.log4j.Logger;
import org.apache.spark.rdd.RDD;

/**
 * Usage of JDBC extractor for reading from a Neo4J database into Cells.
 */
public class ReadingCellWithJdbcNeo4J {

    private static final Logger LOG = Logger.getLogger(ReadingCellWithJdbc.class);

    private ReadingCellWithJdbcNeo4J() {
    }

    public static void main(String[] args) {
        doMain(args);
    }

    public static void doMain(String[] args) {
        String job = "java:readingCellWithJdbcNeo4J";

        String host = "127.0.0.1";
        int port = 7474;

        // Creating the Deep Context where args are Spark Master and Job Name
        ContextProperties p = new ContextProperties(args);
        DeepSparkContext deepContext = new DeepSparkContext(p.getCluster(), job, p.getSparkHome(),
                p.getJars());

        Filter [] filters = new Filter[1];
        JdbcNeo4JDeepJobConfig inputConfigCell = JdbcNeo4JConfigFactory.createJdbcNeo4J().host(host).port(port)
                .connectionUrl("jdbc:neo4j://127.0.0.1:7474")
                .cypherQuery("MATCH (a)-[:`ACTED_IN`]->(b) RETURN a,b LIMIT 25");

        inputConfigCell.initialize();

        RDD inputRDDCell = deepContext.createRDD(inputConfigCell);
        LOG.info("count : " + inputRDDCell.count());
        LOG.info("prints first cell  : " + inputRDDCell.first());

        deepContext.stop();

    }
}

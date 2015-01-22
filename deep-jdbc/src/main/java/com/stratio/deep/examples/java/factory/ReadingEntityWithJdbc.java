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

import com.mysql.jdbc.Driver;
import com.stratio.deep.core.context.DeepSparkContext;
import com.stratio.deep.core.entity.MessageTestEntity;
import com.stratio.deep.jdbc.config.JdbcConfigFactory;
import com.stratio.deep.jdbc.config.JdbcDeepJobConfig;
import com.stratio.deep.examples.utils.ContextProperties;
import com.stratio.deep.jdbc.config.JdbcDeepJobConfig;
import org.apache.log4j.Logger;
import org.apache.spark.rdd.RDD;

/**
 * Usage of JDBC extractor for reading from a MySQL database into Deep Entities.
 */
public class ReadingEntityWithJdbc {

    private static final Logger LOG = Logger.getLogger(ReadingEntityWithJdbc.class);

    private ReadingEntityWithJdbc() {
    }

    public static void main(String[] args) {
        doMain(args);
    }

    public static void doMain(String[] args) {
        String job = "java:readingEntityWithJdbc";

        String host = "127.0.0.1";
        int port = 3306;
        Class driverClass = Driver.class;
        String user = "root";
        String password = "root";

        String database = "test";
        String table = "test_table";

        // Creating the Deep Context where args are Spark Master and Job Name
        ContextProperties p = new ContextProperties(args);
        DeepSparkContext deepContext = new DeepSparkContext(p.getCluster(), job, p.getSparkHome(),
                p.getJars());

        JdbcDeepJobConfig inputConfigCell = JdbcConfigFactory.createJdbc(MessageTestEntity.class).host(host).port(port)
                .username(user)
                .password(password)
                .driverClass(driverClass)
                .database(database)
                .table(table);
        inputConfigCell.initialize();

        RDD<MessageTestEntity> inputRDDEntity = deepContext.createRDD(inputConfigCell);
        LOG.info("count : " + inputRDDEntity.count());
        LOG.info("prints first entity  : " + inputRDDEntity.first());

        deepContext.stop();

    }

}

################################################################################
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
# limitations under the License.
################################################################################

from pyflink.datastream import StreamExecutionEnvironment, TimeCharacteristic
from pyflink.table import StreamTableEnvironment, DataTypes, EnvironmentSettings


def log_processing():
    env = StreamExecutionEnvironment.get_execution_environment()
    t_env = StreamTableEnvironment.create(stream_execution_environment=env)
    t_env.get_config().get_configuration().set_boolean("python.fn-execution.memory.managed", True)

    source_ddl = """
            CREATE TABLE source_table(
                createTime VARCHAR,
                orderId BIGINT,
                payAmount DOUBLE,
                payPlatform INT,
                provinceId INT
            ) WITH (
              'connector.type' = 'kafka',
              'connector.version' = 'universal',
              'connector.topic' = 'payment_msg',
              'connector.properties.bootstrap.servers' = 'kafka:9092',
              'connector.properties.group.id' = 'test_4',
              'connector.startup-mode' = 'earliest-offset',
              'format.type' = 'json'
            )
            """

    sink_ddl = """
                CREATE TABLE sink_table(
                    orderId BIGINT
                ) WITH (
                  'connector' = 'kafka',
                  'topic' = 'sink_topic',
                  'properties.bootstrap.servers' = 'kafka:9092',
                  'format' = 'json'
                )
                """

    t_env.execute_sql(source_ddl)
    t_env.execute_sql(sink_ddl)

    t_env.sql_query("SELECT orderId FROM source_table") \
        .execute_insert("sink_table").wait()

#qili@ip-192-168-1-11 pyflink-walkthrough % docker-compose exec kafka kafka-console-consumer.sh --bootstrap-server kafka:9092 --topic sink_topic

if __name__ == '__main__':
    log_processing()

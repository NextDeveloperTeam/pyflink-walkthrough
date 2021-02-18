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
from pyflink.table.expressions import lit, col
from pyflink.table.window import Tumble


def log_processing():
    env = StreamExecutionEnvironment.get_execution_environment()
    t_env = StreamTableEnvironment.create(stream_execution_environment=env)
    t_env.get_config().get_configuration().set_boolean("python.fn-execution.memory.managed", True)

    create_kafka_source_ddl = """
            CREATE TABLE source_msg(
                id VARCHAR,
                application VARCHAR,
                rowTime TIMESTAMP(3),
                WATERMARK FOR rowTime AS rowTime - INTERVAL '1' SECOND
            ) WITH (
              'connector.type' = 'kafka',
              'connector.version' = 'universal',
              'connector.topic' = 'dev_filtered_topic',
              'connector.properties.bootstrap.servers' = 'kafka-headless.kafka.svc.cluster.local:9092',
              'connector.properties.group.id' = 'test_13',
              'connector.startup-mode' = 'earliest-offset',
              'format.type' = 'csv'
            )
            """


    aggr_ddl = """
                CREATE TABLE aggr_table(
                    application VARCHAR,
                    startTime TIMESTAMP(3),
                    endTime TIMESTAMP(3),
                    cnt BIGINT
                ) WITH (
                  'connector' = 'kafka',
                  'topic' = 'exception-flag-created-cnt',
                  'properties.bootstrap.servers' = 'kafka-headless.kafka.svc.cluster.local:9092',
                  'format' = 'csv'
                )
                """

    t_env.execute_sql(create_kafka_source_ddl)
    t_env.execute_sql(aggr_ddl)

    print(t_env.list_databases())
    print(t_env.list_tables())

    source = t_env.from_path("source_msg")

    source.window(Tumble.over(lit(1).hours).on(source.rowTime).alias("w")) \
        .group_by(source.application, col('w')) \
        .select(source.application, col('w').start.alias('startTime'), col('w').end.alias('endTime'),
                source.id.count.alias('cnt')) \
        .execute_insert("aggr_table").wait()

    # t_env.from_path("test_msg") \
    #     .select("application, id") \
    #     .group_by("application") \
    #     .select("application, count(id) as cnt") \
    #     .execute_insert("aggr_table")


if __name__ == '__main__':
    log_processing()

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
from pyflink.table.udf import udf
from pyflink.table.window import Tumble
from datetime import datetime
import time


@udf(input_types=[DataTypes.STRING()], result_type=DataTypes.STRING())
def string_to_timestamp(date_str):
    print(date_str)
    return time.strftime("%Y-%m-%d %H:%M:%S", datetime.strptime(date_str, "%b %d, %Y %I:%M:%S %p").timetuple())

# https://stackoverflow.com/questions/1759455/how-can-i-account-for-period-am-pm-using-strftime
# https://www.kite.com/python/answers/how-to-convert-between-month-name-and-month-number-in-python

def log_processing():
    env = StreamExecutionEnvironment.get_execution_environment()
    t_env = StreamTableEnvironment.create(stream_execution_environment=env)
    t_env.get_config().get_configuration().set_boolean("python.fn-execution.memory.managed", True)


    create_kafka_source_ddl = """
            CREATE TABLE test_msg(
                payload VARCHAR,
                id VARCHAR,
                application VARCHAR,
                createdAt VARCHAR
            ) WITH (
              'connector.type' = 'kafka',
              'connector.version' = 'universal',
              'connector.topic' = 'exception-flag-created',
              'connector.properties.bootstrap.servers' = 'kafka-headless.kafka.svc.cluster.local:9092',
              'connector.properties.group.id' = 'test_11',
              'connector.startup-mode' = 'earliest-offset',
              'format.type' = 'json'
            )
            """

    sink_ddl = """
                CREATE TABLE filtered_table(
                    id VARCHAR,
                    application VARCHAR,
                    rowTime TIMESTAMP(3)
                ) WITH (
                  'connector' = 'kafka',
                  'topic' = 'dev_filtered_topic',
                  'properties.bootstrap.servers' = 'kafka-headless.kafka.svc.cluster.local:9092',
                  'format' = 'csv'
                )
                """

    t_env.execute_sql(create_kafka_source_ddl)
    t_env.execute_sql(sink_ddl)
    t_env.register_function('string_to_timestamp', string_to_timestamp)


    t_env.sql_query("SELECT id, application, CAST(string_to_timestamp(createdAt) AS TIMESTAMP) as rowTime FROM test_msg") \
        .execute_insert("filtered_table").wait()


if __name__ == '__main__':
    log_processing()

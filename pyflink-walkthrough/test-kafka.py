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

    create_kafka_source_ddl = """
            CREATE TABLE payment_msg(
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
              'connector.properties.group.id' = 'test_3',
              'connector.startup-mode' = 'earliest-offset',
              'format.type' = 'json'
            )
            """

    create_print_sink_ddl = """
            CREATE TABLE print_sink (
                provinceId INT,
                pay_amount DOUBLE
            ) with (
                'connector' = 'print'
            )
    """

    t_env.execute_sql(create_kafka_source_ddl)
    t_env.execute_sql(create_print_sink_ddl)

    t_env.from_path("payment_msg") \
        .select("provinceId, payAmount") \
        .group_by("provinceId") \
        .select("provinceId, sum(payAmount) as pay_amount")\
        .execute_insert("print_sink") \


if __name__ == '__main__':
    log_processing()

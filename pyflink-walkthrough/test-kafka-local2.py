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
from pyflink.table.window import Tumble
from pyflink.table.expressions import lit, col


def log_processing():
    env = StreamExecutionEnvironment.get_execution_environment()
    t_env = StreamTableEnvironment.create(stream_execution_environment=env)
    t_env.get_config().get_configuration().set_boolean("python.fn-execution.memory.managed", True)

    input_ddl = """
                   CREATE TABLE input_table(
                       provinceId INT,
                       payAmount DOUBLE,
                       rowTime TIMESTAMP(3),
                       WATERMARK FOR rowTime AS rowTime - INTERVAL '1' SECOND
                   ) WITH (
                     'connector.type' = 'kafka',
                     'connector.version' = 'universal',
                     'connector.topic' = 'filtered_topic',
                     'connector.properties.bootstrap.servers' = 'kafka:9092',
                     'connector.properties.group.id' = 'test_9',
                     'connector.startup-mode' = 'earliest-offset',
                     'format.type' = 'csv'
                   )
                   """

 # have to add  WATERMARK FOR rowTime AS rowTime - INTERVAL '1' SECOND
 # otherwise it will throw : org.apache.flink.table.api.ValidationException: A group window expects a time attribute for grouping in a stream environment.
    aggr_ddl = """
                   CREATE TABLE aggr_table(
                       provinceId INT,
                       startTime TIMESTAMP(3),
                       endTime TIMESTAMP(3),
                       aggPayAmount DOUBLE
                   ) WITH (
                     'connector' = 'kafka',
                     'topic' = 'aggr_topic',
                     'properties.bootstrap.servers' = 'kafka:9092',
                     'format' = 'csv'
                   )
                   """

    t_env.execute_sql(input_ddl)
    t_env.execute_sql(aggr_ddl)

    source = t_env.from_path("input_table")

    source.window(Tumble.over(lit(1).minutes).on(source.rowTime).alias("w")) \
        .group_by(source.provinceId, col('w')) \
        .select(source.provinceId, col('w').start.alias('startTime'), col('w').end.alias('endTime'), source.payAmount.sum.alias('aggPayAmount'))\
        .execute_insert("aggr_table").wait()

    # t_env.sql_query("SELECT provinceId, payAmount as aggPayAmount FROM source2_table") \
    #     .execute_insert("sink4_table").wait()

    # https://ci.apache.org/projects/flink/flink-docs-stable/dev/table/tableApi.html

if __name__ == '__main__':
    log_processing()

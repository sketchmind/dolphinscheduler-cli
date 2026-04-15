from __future__ import annotations

from enum import Enum, IntEnum, StrEnum

class DbType(StrEnum):
    code: int
    name_field: str
    descp: str

    def __new__(cls, wire_value: str, code: int, name_arg: str, descp: str) -> DbType:
        obj = str.__new__(cls, wire_value)
        obj._value_ = wire_value
        obj.code = code
        obj.name_field = name_arg
        obj.descp = descp
        return obj
    MYSQL = ('MYSQL', 0, 'mysql', 'mysql')
    POSTGRESQL = ('POSTGRESQL', 1, 'postgresql', 'postgresql')
    HIVE = ('HIVE', 2, 'hive', 'hive')
    SPARK = ('SPARK', 3, 'spark', 'spark')
    CLICKHOUSE = ('CLICKHOUSE', 4, 'clickhouse', 'clickhouse')
    ORACLE = ('ORACLE', 5, 'oracle', 'oracle')
    SQLSERVER = ('SQLSERVER', 6, 'sqlserver', 'sqlserver')
    DB2 = ('DB2', 7, 'db2', 'db2')
    PRESTO = ('PRESTO', 8, 'presto', 'presto')
    H2 = ('H2', 9, 'h2', 'h2')
    REDSHIFT = ('REDSHIFT', 10, 'redshift', 'redshift')
    ATHENA = ('ATHENA', 11, 'athena', 'athena')
    TRINO = ('TRINO', 12, 'trino', 'trino')
    STARROCKS = ('STARROCKS', 13, 'starrocks', 'starrocks')
    AZURESQL = ('AZURESQL', 14, 'azuresql', 'azuresql')
    DAMENG = ('DAMENG', 15, 'dameng', 'dameng')
    OCEANBASE = ('OCEANBASE', 16, 'oceanbase', 'oceanbase')
    SSH = ('SSH', 17, 'ssh', 'ssh')
    KYUUBI = ('KYUUBI', 18, 'kyuubi', 'kyuubi')
    DATABEND = ('DATABEND', 19, 'databend', 'databend')
    SNOWFLAKE = ('SNOWFLAKE', 20, 'snowflake', 'snowflake')
    VERTICA = ('VERTICA', 21, 'vertica', 'vertica')
    HANA = ('HANA', 22, 'hana', 'hana')
    DORIS = ('DORIS', 23, 'doris', 'doris')
    ZEPPELIN = ('ZEPPELIN', 24, 'zeppelin', 'zeppelin')
    SAGEMAKER = ('SAGEMAKER', 25, 'sagemaker', 'sagemaker')
    K8S = ('K8S', 26, 'k8s', 'k8s')
    ALIYUN_SERVERLESS_SPARK = ('ALIYUN_SERVERLESS_SPARK', 27, 'aliyun_serverless_spark', 'aliyun serverless spark')
    DOLPHINDB = ('DOLPHINDB', 28, 'dolphindb', 'dolphindb')

    @classmethod
    def from_code(cls, code: int) -> "DbType":
        for member in cls:
            if member.code == code:
                return member
        raise ValueError(f"Unknown DbType code: {code}")

__all__ = ["DbType"]

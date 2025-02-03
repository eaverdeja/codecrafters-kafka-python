class KafkaMessage:
    API_KEY: int
    MIN_VERSION: int
    MAX_VERSION: int


class ApiVersions(KafkaMessage):
    """
    # https://kafka.apache.org/protocol.html#The_Messages_ApiVersions

    ApiVersions Response (Version: 3/4) => error_code [api_keys] throttle_time_ms TAG_BUFFER
    error_code => INT16
    api_keys => api_key min_version max_version TAG_BUFFER
        api_key => INT16
        min_version => INT16
        max_version => INT16
    throttle_time_ms => INT32
    """

    API_KEY = 18
    MIN_VERSION = 0
    MAX_VERSION = 4


class DescribeTopicPartitions(KafkaMessage):
    """
    https://kafka.apache.org/protocol.html#The_Messages_DescribeTopicPartitions

    DescribeTopicPartitions Response (Version: 0) => throttle_time_ms [topics] next_cursor TAG_BUFFER
        throttle_time_ms => INT32
        topics => error_code name topic_id is_internal [partitions] topic_authorized_operations TAG_BUFFER
            error_code => INT16
            name => COMPACT_NULLABLE_STRING
            topic_id => UUID
            is_internal => BOOLEAN
            partitions => error_code partition_index leader_id leader_epoch [replica_nodes] [isr_nodes] [eligible_leader_replicas] [last_known_elr] [offline_replicas] TAG_BUFFER
            error_code => INT16
            partition_index => INT32
            leader_id => INT32
            leader_epoch => INT32
            replica_nodes => INT32
            isr_nodes => INT32
            eligible_leader_replicas => INT32
            last_known_elr => INT32
            offline_replicas => INT32
            topic_authorized_operations => INT32
        next_cursor => topic_name partition_index TAG_BUFFER
            topic_name => COMPACT_STRING
            partition_index => INT32
    """

    API_KEY = 75
    MIN_VERSION = 0
    MAX_VERSION = 0

class AbstractProducer:
    """
    Abstract Kafka Producer doing nothing.
    """

    def __init__(self, bootstrap_servers: list, schema: dict) -> None:
        """
        Create the Producer instance.
        """
        self.bootstrap_servers = bootstrap_servers
        self.schema = schema

    def send(self, topic: str, key: str, value: dict) -> None:
        """
        Send the specified Value to a Kafka Topic.
        """


class AbstractConsumer:
    """
    Abstract Kafka Consumer doing nothing.
    """

    def __init__(
        self, bootstrap_servers: list, group_id: str, topics: list, schema: dict
    ) -> None:
        """
        Create the Consumer instance.
        """
        self.bootstrap_servers = bootstrap_servers
        self.group_id = group_id
        self.topics = topics
        self.schema = schema

    def create_topics(
        self, topics: list, num_partitions: int = 2, replication_factor: int = 1
    ):
        """
        Create the specified list of Topics.
        """
        return []

    def read(self):
        """
        Read messages from the configured Kafka Topics.
        """
        return []

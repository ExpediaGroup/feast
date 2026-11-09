from typing import Any, Dict

from testcontainers.elasticsearch import ElasticSearchContainer

from tests.integration.feature_repos.universal.online_store_creator import (
    OnlineStoreCreator,
)

# Elasticsearch 8 sizes its JVM heap from the host's memory (about half of it)
# when the container has no limit. Two of these starting together in the
# parallel integration run used up a CI runner's memory and got the runner shut
# down, so cap the heap and the container.
ES_TEST_JAVA_OPTS = "-Xms512m -Xmx512m"
ES_TEST_MEM_LIMIT = "1g"


class ElasticSearchOnlineStoreCreator(OnlineStoreCreator):
    def __init__(self, project_name: str, **kwargs):
        super().__init__(project_name)
        self.container = (
            ElasticSearchContainer("elasticsearch:8.18.0", mem_limit=ES_TEST_MEM_LIMIT)
            .with_env("ES_JAVA_OPTS", ES_TEST_JAVA_OPTS)
            .with_exposed_ports(9200)
        )

    def create_online_store(self) -> Dict[str, Any]:
        self.container.start()
        return {
            "host": "localhost",
            "type": "elasticsearch",
            "port": self.container.get_exposed_port(9200),
            "similarity": "cosine",
        }

    def teardown(self):
        self.container.stop()

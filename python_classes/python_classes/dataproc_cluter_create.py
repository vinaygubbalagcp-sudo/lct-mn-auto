# can you please covnvert the following code into a class based code and create an object of that class in another file and call the method to create dataproc cluster

from google.cloud import dataproc_v1 as dataproc


class DataprocClusterManager:
    def __init__(self, project_id, region):
        self.project_id = project_id
        self.region = region

    def create_cluster(self, cluster_name):
        # Create a client with the endpoint set to the desired cluster region.
        cluster_client = dataproc.ClusterControllerClient(
            client_options={"api_endpoint": f"{self.region}-dataproc.googleapis.com:443"}
        )

        # Create the cluster config.
        cluster = {
            "project_id": self.project_id,
            "cluster_name": cluster_name,
            "config": {
                "master_config": {"num_instances": 1, "machine_type_uri": "n1-standard-2"},
                "worker_config": {"num_instances": 2, "machine_type_uri": "n1-standard-2"},
            },
        }
    # Create the cluster config.
    cluster = {
        "project_id": project_id,
        "cluster_name": cluster_name,
        "config": {
            "master_config": {"num_instances": 1, "machine_type_uri": "n1-standard-2"},
            "worker_config": {"num_instances": 2, "machine_type_uri": "n1-standard-2"},
        },
    }
 
from dataproc_cluter_create import create_cluster

project_id = "lvc-tc-mn-d"
region = "us-east1"
cluster_name = "example-dataproc-cluster"
create_cluster(project_id, region, cluster_name)

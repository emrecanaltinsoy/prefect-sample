"""Register the orders-pipeline flow as a deployment on the Docker work pool.

Run once after the Prefect server is up (the `deployer` service in docker-compose does this).
The deployment points at a prebuilt image, so nothing is built or pushed here.
"""

import os

from flow import orders_pipeline

IMAGE = os.getenv("PIPELINE_IMAGE", "prefect-test-pipeline:latest")
WORK_POOL = os.getenv("WORK_POOL", "docker-pool")

if __name__ == "__main__":
    deployment_id = orders_pipeline.deploy(
        name="orders-pipeline-docker",
        work_pool_name=WORK_POOL,
        image=IMAGE,
        build=False,
        push=False,
        parameters={"num_orders": 100},
    )
    print(f"Deployment ready: orders-pipeline/orders-pipeline-docker ({deployment_id})")

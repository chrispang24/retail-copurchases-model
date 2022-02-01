"""
Run Baseline Recommender Model
"""

import sys
import time
import coiled
from dask.distributed import Client, LocalCluster
from recommender.config import cfg
from recommender.builder import RecommenderBuilder
from recommender.server import RecommenderServer


if __name__ == "__main__":

    print("\nBaseline Recommender Model...")

    # if rebuild parameter specified, build baseline recommendation using computing cluster
    if len(sys.argv) > 1 and sys.argv[1] == 'build':
        print("\nBuilding Model...")
        start = time.time()

        #cloud_cluster = coiled.Cluster(name="aws", n_workers=20, worker_cpu=2, worker_memory="16 GiB")
        local_cluster = LocalCluster(n_workers=cfg.num_workers)
        client = Client(local_cluster)

        builder = RecommenderBuilder(config=cfg)
        builder.run()

        print(f"Model Build Time: {time.time() - start}")

    # build key-value recommendations server and return results for item ID
    server = RecommenderServer(config=cfg)
    server.run()

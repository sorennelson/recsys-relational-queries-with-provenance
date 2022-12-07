from typing import List, Dict
import logging
from .workers import LandCoverWorker
from collections import defaultdict
import ray
from .aws_util import S3Manager
from opentelemetry import trace
from opentelemetry.trace import NonRecordingSpan, SpanContext, SpanKind, TraceFlags, Link
tracer = trace.get_tracer('RSR')


class BoundBox:
    """
    Holds information of the bound box area
    """

    def __init__(self, start_path: int, start_row: int, end_path: int, end_row: int):
        self.start_path = start_path
        self.start_row = start_row
        self.end_path = end_path
        self.end_row = end_row

    def get_start_path(self) -> int:
        return self.start_path

    def get_start_row(self) -> int:
        return self.start_row

    def get_end_path(self) -> int:
        return self.end_path

    def get_end_row(self) -> int:
        return self.end_row


class TimeWindow:
    """
    Holds information of the time window
    """

    def __init__(self, years: int):
        self.years = years

    def get_years(self) -> int:
        return self.years

# TODO: Trace this class
class ClusterConfig:
    """
    Holds the configuration regarding the clusters and workers in the cluster
    """
    def __init__(self, node_count: int, core_count: int, num_workers: int, s3_client: S3Manager = None):
        self.node_count = node_count
        self.core_count = core_count
        self.num_workers = None
        if num_workers is not None:
            self.num_workers = num_workers
        else:
            self.num_workers = self.core_count
        self.workers = []
        self.assignments = defaultdict(list)
        self.s3_client = s3_client

    def get_node_count(self) -> int:
        return self.node_count

    def get_core_count(self) -> int:
        return self.core_count

    def get_num_workers(self) -> int:
        return self.num_workers

    def get_workers(self) -> List[LandCoverWorker]:
        return self.workers

    def get_assignments(self) -> Dict[str, list]:
        return self.assignments

    # TODO: Trace this function
    def initialize_workers(self):
        """
        Spawn workers for the cluster configuration
        """
        # Spawn all workers
        # num_workers = [80]  # Running on just one cluster => 10 workers
        # num_workers.reverse()
        with tracer.start_as_current_span("ClusterConfig.initialize_workers"):
            logging.info("  Found {} nodes and {} cores.".format(self.node_count, self.core_count))
            node_id_mapping = {}
            node_id = 0
            for i in range(self.num_workers):
                print("Before creating worker")
                worker = LandCoverWorker.remote()
                print("ID: ", ray.get(worker.id.remote()))
                print("IP: ", ray.get(worker.ip.remote()))
                print("Host: ", ray.get(worker.hostname.remote()))
                host = ray.get(worker.hostname.remote())
                #Storing node id of the host
                if host not in node_id_mapping:
                    node_id_mapping[host] = node_id
                    node_id += 1
                print("Created worker")
                # add worker to mapping
                self.assignments[host].append(worker)
                worker.set_id.remote(len(self.assignments[host]) - 1)
                worker.set_node_id.remote(node_id_mapping[host])
                worker.set_total_nodes.remote(self.node_count)
                self.workers.append(worker)
            # store assignments to worker
            for worker in self.workers:
                worker.set_assignments.remote(self.assignments)
            for (h, c) in self.assignments.items():
                print("{} -> {}".format(h, c))

    # TODO: Trace this function
    def reset_workers(self):
        # Clearing Raw Folder
        with tracer.start_as_current_span("ClusterConfig.reset_workers"):
            for host in self.assignments:
                ray.get(self.assignments[host][0].delete_raw_folder.remote(trace.get_current_span().get_span_context()))
                for worker in self.assignments[host]:
                    worker.reset_worker.remote()

    # TODO: Trace this function
    def set_aws_config(self, cluster_row, cluster_path, zone):
        with tracer.start_as_current_span("ClusterConfig.set_aws_config"):
            if self.s3_client is not None:
                bucket_name = "{}-rsr-analysis-data".format(zone)
                for worker in self.workers:
                    worker.set_bucket_name.remote(bucket_name)
                    worker.set_aws_client.remote(self.s3_client)
                    worker.set_path_row.remote(cluster_path,cluster_row)

    # TODO: Trace this function
    def populate_workers(self, file_names: List, time_file_names: List):
        with tracer.start_as_current_span("ClusterConfig.populate_workers"):
            # Get all the files from the bucket of the path
            res = []
            for i in range(len(self.workers)):
                res.append(self.workers[i].populate_data.remote(file_names[i], time_file_names[i], 
                                                                trace.get_current_span().get_span_context()))
            # Waiting for all workers to have the data
            ray.get(res)




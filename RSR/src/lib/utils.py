import itertools
from typing import List, Dict
from geowombat.util import GeoDownloads
import time as clock
import logging
import math
from lib.convertor import Convertor
import ray
from .config import ClusterConfig, BoundBox, TimeWindow
from .workers import LandCoverWorker
from .models import Model
import os
import requests
import multiprocessing
import collections
import json
from opentelemetry import trace
tracer = trace.get_tracer('RSR')


class Data(object):
    """
    A handle to the distributed input dataset
    """
    def __init__(self, workers: List[LandCoverWorker]):
        # The worker handles that store partitions of the dataset
        self.workers = workers
        # A list of pointers to time series data
        self.time_series = []
        # A list of pointers to Dan's model results
        self.model_results = []
        pass

    # Creates time series from the raw data
    def create_time_series(self):
        if len(self.time_series) == 0:  # Preprocess data if not done already
            for w in self.workers:
                self.time_series.append(w.create_time_series.remote())
        pass

    # Applies Dan's model to the preprocessed time series
    def apply_model(self, model: Model):
        if len(self.model_results) == 0:  # Apply the model if not already
            for w in self.workers:
                self.model_results.append(w.apply_model.remote(model, 
                                                            trace.get_current_span().get_span_context()))
            print(ray.get(self.model_results))
        pass

    def collect_data(self,convertor:Convertor=None):
        output_map_futures = []
        for w in self.workers:
            output_map_futures.append(w.get_output_files.remote(convertor, 
                                                            trace.get_current_span().get_span_context()))
        output_map_results = ray.get(output_map_futures)
        print(len(output_map_results))
        # os.makedirs('/home/ubuntu/raw/results', exist_ok=True)
        # os.chdir('/home/ubuntu/raw/results/')
        # for future in output_map_futures:
        #     result = ray.get(future)
        #     print(result.keys())
        #     for file in result:
        #         print("Saving {} to disk".format(file))
        #         if convertor == None:
        #             result[file].tofile(file)
        #         else:
        #             convertor.save(file,result[file])





class Archive:
    """
    Interface for accessing an archive of data
    """
    def __init__(self, cluster_config: ClusterConfig):
        self.cluster_config = cluster_config
        self.bound_box = None
        self.time_window = None

    def query(self, bound_box: BoundBox, time_window: TimeWindow):
        self.bound_box = bound_box
        self.time_window = time_window
        return self

    def fetch_urls(self, limit=None):
        pass

    def download(self):
        pass

    def get_total_size(self):
        pass

#TODO: Trace this class
class GoogleArchive(Archive):
    """
    Class for accessing Google archive of images
    """
    def __init__(self, cluster_config: ClusterConfig):
        Archive.__init__(self, cluster_config)
        self.unique_urls = []
        self.true_urls_per_worker = []
        self.tile_mapping = collections.defaultdict(dict)

    # TODO: Trace this function
    def get_urls_for_tile(self, data):
        """
            Fetch urls for downloading data
            Parameters: path, row, bands
            """
        with tracer.start_as_current_span("GoogleArchive.get_urls_for_tile"):
            ########################################################################################################
            # I changed this from the gw tutorial to match a cert available in SCC. May need to do the same on MOC
            ########################################################################################################
            # os.environ['CURL_CA_BUNDLE'] = '/etc/ssl/certs/ca-bundle.crt'
            # os.chdir('/projectnb2/measures/users/danc')
            os.environ['CURL_CA_BUNDLE'] = '/etc/ssl/certs/ca-certificates.crt'

            ########################################################################################################
            # Downloading data
            ########################################################################################################

            gdl_l5 = GeoDownloads()
            gdl_l7 = GeoDownloads()
            gdl_l8 = GeoDownloads()

            row = data[1]
            path = data[0]
            print("Getting urls for Path: {} Row: {}".format(path, row))
            logging.info("  Retrieving all years and months for tile...")
            start = clock.time()

            # Query all years and months for tile(path&row)
            gdl_l5.list_gcp('l5', f'{path:03d}/{row:03d}/*{path:03d}{row:03d}_*_T1*')
            gdl_l7.list_gcp('l7', f'{path:03d}/{row:03d}/*{path:03d}{row:03d}_*_T1*')
            gdl_l8.list_gcp('l8', f'{path:03d}/{row:03d}/*{path:03d}{row:03d}_*_T1*')

            logging.info("  Done in {} secs.".format(clock.time() - start))

            logging.info("  Fetching scene list and URLs...")
            start = clock.time()

            # Get a scene list of all Landsat images and bands
            scene_list_l5 = gdl_l5.get_gcp_results
            scene_list_l7 = gdl_l7.get_gcp_results
            scene_list_l8 = gdl_l8.get_gcp_results

            # Isolate scene id from the full path
            scene_keys_split_l5 = [key.split('/') for key in scene_list_l5.keys()]
            scene_keys_split_l7 = [key.split('/') for key in scene_list_l7.keys()]
            scene_keys_split_l8 = [key.split('/') for key in scene_list_l8.keys()]

            # Make list of scene ids
            scene_list_l5_final = []
            for i in range(len(scene_keys_split_l5)):
                scene_ids_l5 = scene_keys_split_l5[i][4]
                scene_list_l5_final.append(scene_ids_l5)

            scene_list_l7_final = []
            for i in range(len(scene_keys_split_l7)):
                scene_ids_l7 = scene_keys_split_l7[i][4]
                scene_list_l7_final.append(scene_ids_l7)

            scene_list_l8_final = []
            for i in range(len(scene_keys_split_l8)):
                scene_ids_l8 = scene_keys_split_l8[i][4]
                scene_list_l8_final.append(scene_ids_l8)
            bands = ['nir']

            # URLs for each of the scene ids
            urls_final_l5 = []
            for i in range(len(scene_list_l5_final)):
                urls_l5, meta_url_l5 = gdl_l5.get_landsat_urls(scene_list_l5_final[i], bands=bands)
                urls_final_l5.append(urls_l5)
            urls_final_l7 = []
            for i in range(len(scene_list_l7_final)):
                urls_l7, meta_url_l7 = gdl_l7.get_landsat_urls(scene_list_l7_final[i], bands=bands)
                urls_final_l7.append(urls_l7)

            urls_final_l8 = []
            for i in range(len(scene_list_l8_final)):
                urls_l8, meta_url_l8 = gdl_l8.get_landsat_urls(scene_list_l8_final[i], bands=bands)
                urls_final_l8.append(urls_l8)
            # Flatten the list of urls
            flat_list_l5 = [item for sublist in urls_final_l5 for item in sublist]
            flat_list_l7 = [item for sublist in urls_final_l7 for item in sublist]
            flat_list_l8 = [item for sublist in urls_final_l8 for item in sublist]

            flat_list = flat_list_l5 + flat_list_l7 + flat_list_l8  # COmmented out l5 and l7
            # flat_list = flat_list_l8  # Use Landsat 8 only
            # flat_list=flat_list+flat_list_l5 #1.33 times data size ??
            logging.info("  Done in {} secs. Found {} TIF files.".format(clock.time() - start, len(flat_list)))
            # flat_list=flat_list_l5 #Subset
            return list(set(flat_list))

    # TODO: Trace this function
    def fetch_urls(self, limit=None, parallel=False, per_tile=False):
        with tracer.start_as_current_span("GoogleArchive.fetch_urls"):
            self.unique_urls = []
            if parallel:
                path_range = range(self.bound_box.get_start_path(), self.bound_box.get_end_path()-1, -1)
                row_range = range(self.bound_box.get_start_row(), self.bound_box.get_end_row()+1)
                path_row_combinations = list(itertools.product(path_range, row_range))
                fetch_urls_pool = multiprocessing.Pool()
                response = fetch_urls_pool.map(self.get_urls_for_tile, path_row_combinations)
                for url_list in response:
                    self.unique_urls.extend(url_list)
                for url in self.unique_urls:
                    path, row = self.get_url_components(url)
                    if "{}-{}".format(path,row) not in self.tile_mapping:
                        self.tile_mapping["{}-{}".format(path,row)]['urls'] = 0
                        self.tile_mapping["{}-{}".format(path,row)]['size'] = 0
                    self.tile_mapping["{}-{}".format(path,row)]['urls'] += 1
            else:
                # Code for iterative
                for path in range(self.bound_box.get_start_path(), self.bound_box.get_end_path()-1, -1):
                    for row in range(self.bound_box.get_start_row(), self.bound_box.get_end_row()+1):
                        print("Getting urls for Path: {} Row: {}".format(path, row))
                        self.unique_urls.extend(self.get_urls_for_tile([path, row]))
            if limit:
                self.unique_urls = self.unique_urls[:limit]
            return self

    # TODO: Trace this function
    def redistribute(self, compression=False) -> Data:
        with tracer.start_as_current_span("GoogleArchive.redistribute"):
            urls_per_worker = self.true_urls_per_worker
            res = []
            timers = []
            # # redistribution
            logging.info("Start redistributing data")
            start_time = clock.time()
            # abs_worker_id = 0
            # Parallelization change happens here
            # for node_id, node_workers in zip(range(len(self.cluster_config.get_assignments().values())), self.cluster_config.get_assignments().values()):
            #     start = 0  # We want every worker to handle different files based under the range
            #     for worker in node_workers:  # For each worker inside node
            #         # Grab the "true" url per worker value received from the parent function.
            #         # THis is a list of core ids (across all nodes) to the number of URLs assigned to them
            #         rslt = worker.redistribute.remote(start, urls_per_worker[abs_worker_id], compression)
            #         start += urls_per_worker[abs_worker_id]
            #         abs_worker_id += 1
            #         res.append(rslt)
            # Calling redistribute on all workers
            for worker in self.cluster_config.get_workers():
                res.append(worker.redistribute.remote(compression, trace.get_current_span().get_span_context()))
            futures = ray.get(res)
            end_time = clock.time()
            logging.info("Initial Get done in {} secs.".format(end_time - start_time))
            start_time = clock.time()
            idx = 0
            for future in futures:
                if future:
                    ray.get(future)
                    idx += 1
            end_time = clock.time()
            logging.info("Future Get done in {} secs.".format(end_time - start_time))
            return Data(self.cluster_config.get_workers())

    # TODO: Trace this function
    def download(self) -> Data:
        with tracer.start_as_current_span("GoogleArchive.download"):
            print("Distributing {} to {} workers".format(len(self.unique_urls),self.cluster_config.get_num_workers()))
            # This allows us to handle URL counts that don't perfectly match our worker count
            urls_per_worker = math.floor(len(self.unique_urls) / self.cluster_config.get_num_workers())
            true_url_per_worker = [urls_per_worker] * self.cluster_config.get_num_workers()
            remaining_urls = len(self.unique_urls) - (urls_per_worker * self.cluster_config.get_num_workers())
            # Distributed the remaining URLs to workers
            for i in range(remaining_urls):
                true_url_per_worker[i] += 1
                remaining_urls -= 1

            logging.info("  Will assign {} URLs to each worker.".format(urls_per_worker))
            start = 0
            res = []
            start_time = clock.time()

            for i in range(self.cluster_config.get_num_workers()):
                iter_urls_per_worker = min(urls_per_worker, self.cluster_config.get_num_workers() - start)
                logging.info("  Pulling tiles in [{},{})".format(start,
                                                                start + true_url_per_worker[i]))
                res.append(self.cluster_config.get_workers()[i].pull_data.remote(
                    self.unique_urls[start:start + true_url_per_worker[i]],
                    trace.get_current_span().get_span_context()
                ))
                start += true_url_per_worker[i]
            ray.get(res)
            end_time = clock.time()
            logging.info(" Downloading Done in {} secs.".format(end_time - start_time))
            print("{}, {}".format(self.cluster_config.get_num_workers(), end_time - start_time))
            # TODO: Figure out why this code was being used. Don't we need to keep it?
            # res.clear()
            # for i in range(n):
            #     res.append(workers[i].delete_raw_data.remote())
            # ray.get(res)
            print(true_url_per_worker)
            self.true_urls_per_worker = true_url_per_worker
            return self

    def get_size_of_url(self, url:str):
        path, row = self.get_url_components(url)
        response = requests.head(url, allow_redirects=True)
        size = response.headers.get('content-length', -1)
        size_mb = int(size) / float(1 << 20)
        return ("{}-{}".format(path,row),size_mb)

    def get_total_size(self):
        url_size_pool = multiprocessing.Pool()
        print("Urls to complete: {}".format(len(self.unique_urls)))
        response = url_size_pool.map(self.get_size_of_url, self.unique_urls)
        final_size = 0
        for data in response:
            self.tile_mapping[data[0]]['size'] += data[1]
            final_size += data[1]
        return final_size
    
    def get_tile_mapping(self):
        return self.tile_mapping

    def get_url_components(self, url:str):
        data = url.split("/")
        return int(data[6]), int(data[7])

    def get_worker_timers(self):
        futures = []
        for node_id, node_workers in zip(range(len(self.cluster_config.get_assignments().values())), self.cluster_config.get_assignments().values()):
            for worker_id, worker in zip(range(len(node_workers)), node_workers):
                futures.append(worker.get_global_timers.remote())
        data = ray.get(futures)
        final_timer_data = collections.defaultdict(list)
        for d in data:
            final_timer_data.update(d)
        return final_timer_data
    
    def get_total_uncompressed_size(self):
        futures = []
        for worker in self.cluster_config.get_workers():
            futures.append(worker.get_total_uncompressed_size.remote())
        data = ray.get(futures)
        return sum(data)/math.pow(1024,3)



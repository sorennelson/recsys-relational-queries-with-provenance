from lib.config import ClusterConfig, BoundBox, TimeWindow
from lib.utils import GoogleArchive, Data
from lib.models import MockModel
import ray
from collections import defaultdict
import time as clock
import argparse
from opentelemetry import trace
from opentelemetry.exporter.jaeger.thrift import JaegerExporter
from opentelemetry.sdk.resources import SERVICE_NAME, Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Sample application for running models")
    parser.add_argument("--urls", dest="urlCount", help="Number of urls to download", type=int, required=True)
    args = parser.parse_args()
    trace.set_tracer_provider(
        TracerProvider(
            resource=Resource.create({SERVICE_NAME: "RSR"})
        )
    )
    jaeger_exporter = JaegerExporter(
        agent_host_name="localhost",
        agent_port=6831,
    )
    trace.get_tracer_provider().add_span_processor(
        BatchSpanProcessor(jaeger_exporter)
    )
    runtime_env = {"working_dir": "."}
    tracer = trace.get_tracer('RSR')
    with tracer.start_as_current_span("main-start"):
        ray.init(address="auto", runtime_env =runtime_env)
        global_timers = defaultdict(list)
        global_timers['e2e'].append(clock.time())
        global_timers['clusterCreation'].append(clock.time())
        c = ClusterConfig(len(ray.nodes()), ray.cluster_resources()['CPU'], int(ray.cluster_resources()['CPU']))
        c.initialize_workers()
        global_timers['clusterCreation'].append(clock.time())
        bound_box = BoundBox(12, 31, 12, 31)
        time_window = TimeWindow(1)
        global_timers['archiveDownload'].append(clock.time())
        archive = GoogleArchive(c)
        archive.query(bound_box, time_window).fetch_urls(args.urlCount).download()
        global_timers['archiveDownload'].append(clock.time())
        global_timers['redistribute'].append(clock.time())
        imageData = archive.redistribute()
        global_timers['redistribute'].append(clock.time())
        global_timers['applyModel'].append(clock.time())
        r_model = MockModel()
        imageData.apply_model(r_model)
        global_timers['applyModel'].append(clock.time())
        global_timers['collectFiles'].append(clock.time())
        imageData.collect_data()
        global_timers['collectFiles'].append(clock.time())
        global_timers['e2e'].append(clock.time())
        for key in global_timers.keys():
            print(f'{key}: {global_timers[key][1] - global_timers[key][0]}')

"""
Adopted from https://github.com/IBM/Drain3
"""
# Standard Library
import base64
import logging
import re
import pathlib
import time
import zlib

# Third Party
import jsonpickle
from cachetools import LRUCache
from drain3.drain import Drain, LogCluster
from drain3.persistence_handler import PersistenceHandler
from drain3.simple_profiler import NullProfiler, Profiler, SimpleProfiler
from drain3.template_miner_config import TemplateMinerConfig

logger = logging.getLogger(__name__)

config_filename = "drain3.ini"


class TemplateMiner:
    def __init__(
        self,
        persistence_handler: PersistenceHandler = None,
        config: TemplateMinerConfig = None,
        clusters_counter: int = 0
    ):
        """
        Wrapper for Drain with persistence and masking support
        :param persistence_handler: The type of persistence to use. When None, no persistence is applied.
        :param config: Configuration object. When none, configuration is loaded from default .ini file (if exist)
        """
        logger.info("Starting Drain3 template miner")

        if config is None:
            logger.info(f"Loading configuration from {config_filename}")
            config = TemplateMinerConfig()
            config.load(config_filename)

        self.config = config

        self.profiler: Profiler = NullProfiler()
        if self.config.profiling_enabled:
            self.profiler = SimpleProfiler()

        self.persistence_handler = persistence_handler

        param_str = self.config.mask_prefix + "*" + self.config.mask_suffix
        self.drain = Drain(
            sim_th=self.config.drain_sim_th,
            depth=self.config.drain_depth,
            max_children=self.config.drain_max_children,
            max_clusters=self.config.drain_max_clusters,
            extra_delimiters=self.config.drain_extra_delimiters,
            profiler=self.profiler,
            param_str=param_str,
            clusters_counter=clusters_counter
        )
        self.last_save_time = time.time()
        if persistence_handler is not None:
            self.load_state()

    def load_state(self, control_plane_binary_path=None):
        logger.info("Checking for saved state")
        state = None
        if control_plane_binary_path:
            state = pathlib.Path(control_plane_binary_path).read_bytes()
        else:
            state = self.persistence_handler.load_state()
        if state is None:
            logger.info("Saved state not found")
            return

        if self.config.snapshot_compress_state:
            state = zlib.decompress(base64.b64decode(state))

        drain: Drain = jsonpickle.loads(state, keys=True)

        # json-pickle encoded keys as string by default, so we have to convert those back to int
        # this is only relevant for backwards compatibility when loading a snapshot of drain <= v0.9.1
        # which did not use json-pickle's keys=true
        if len(drain.id_to_cluster) > 0 and isinstance(
                next(iter(drain.id_to_cluster.keys())), str
        ):
            drain.id_to_cluster = {
                int(k): v for k, v in list(drain.id_to_cluster.items())
            }
            if self.config.drain_max_clusters:
                cache = LRUCache(maxsize=self.config.drain_max_clusters)
                cache.update(drain.id_to_cluster)
                drain.id_to_cluster = cache

        drain.profiler = self.profiler

        self.drain = drain
        logger.info(
            "Restored {} clusters with {} messages".format(
                len(drain.clusters), drain.get_total_cluster_size()
            )
        )
    def save_state(self):
        state = jsonpickle.dumps(self.drain, keys=True).encode("utf-8")
        if self.config.snapshot_compress_state:
            state = base64.b64encode(zlib.compress(state))

        num_drain_clusters = len(self.drain.clusters)

        logger.info(
            f"Saving state of {num_drain_clusters} clusters "
            f"with {self.drain.get_total_cluster_size()} messages, {len(state)} bytes"
        )
        self.persistence_handler.save_state(state)

    def save_state_local(self, snapshot_reason, file_path):
        state = jsonpickle.dumps(self.drain, keys=True).encode("utf-8")
        if self.config.snapshot_compress_state:
            state = base64.b64encode(zlib.compress(state))

        num_drain_clusters = len(self.drain.clusters)

        logger.info(
            f"Saving state of {num_drain_clusters} clusters "
            f"with {self.drain.get_total_cluster_size()} messages, {len(state)} bytes, "
            f"reason: {snapshot_reason}"
        )
        pathlib.Path(file_path).write_bytes(state)


    def get_snapshot_reason(self, change_type, cluster_id):
        if change_type != "none":
            return f"{change_type} ({cluster_id})"

        diff_time_sec = time.time() - self.last_save_time
        if diff_time_sec >= self.config.snapshot_interval_minutes * 60:
            return "periodic"

        return None

    def add_log_message(self, masked_log_message: str, original_log_message: str, anomaly_level: str) -> dict:
        self.profiler.start_section("total")

        self.profiler.start_section("drain")
        cluster, change_type = self.drain.add_log_message(masked_log_message, original_log_message, anomaly_level)
        self.profiler.end_section("drain")
        result = {
            "change_type": change_type,
            "cluster_id": cluster.cluster_id,
            "cluster_size": cluster.size,
            "template_mined": cluster.get_template(),
            "cluster_count": len(self.drain.clusters),
        }
        self.profiler.end_section("total")
        self.profiler.report(self.config.profiling_report_sec)
        return result

    def add_log_template(self, log_template: str, pretrained: bool, anomaly_level: str) -> dict:
        self.profiler.start_section("total")

        self.profiler.start_section("drain")
        cluster = self.drain.add_log_template(log_template, pretrained, anomaly_level)
        self.profiler.end_section("drain")
        result = {
            "cluster_id": cluster.cluster_id,
            "cluster_size": cluster.size,
            "template_mined": cluster.get_template(),
            "cluster_count": len(self.drain.clusters),
        }

        self.profiler.end_section("total")
        self.profiler.report(self.config.profiling_report_sec)
        return result

    def match(self, masked_log_message: str, original_log_message: str) -> LogCluster:

        """
        Match against an already existing cluster. Match shall be perfect (sim_th=1.0).
        New cluster will not be created as a result of this call, nor any cluster modifications.
        :param log_message: log message to match
        :return: Matched cluster or None of no match found.
        """
        return self.drain.match(masked_log_message, original_log_message)

    def get_parameter_list(self, log_template: str, content: str):
        escaped_prefix = re.escape(self.config.mask_prefix)
        escaped_suffix = re.escape(self.config.mask_suffix)
        template_regex = re.sub(
            escaped_prefix + r".+?" + escaped_suffix, self.drain.param_str, log_template
        )
        if self.drain.param_str not in template_regex:
            return []
        template_regex = re.escape(template_regex)
        template_regex = re.sub(r"\\ +", r"\\s+", template_regex)
        template_regex = (
            "^"
            + template_regex.replace(escaped_prefix + r"\*" + escaped_suffix, "(.*?)")
            + "$"
        )

        for delimiter in self.config.drain_extra_delimiters:
            content = re.sub(delimiter, " ", content)
        parameter_list = re.findall(template_regex, content)
        parameter_list = parameter_list[0] if parameter_list else ()
        parameter_list = (
            list(parameter_list)
            if isinstance(parameter_list, tuple)
            else [parameter_list]
        )

        def is_mask(p: str):
            return p.startswith(self.config.mask_prefix) and p.endswith(
                self.config.mask_suffix
            )

        parameter_list = [p for p in list(parameter_list) if not is_mask(p)]
        return parameter_list
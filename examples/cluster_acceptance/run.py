#!/usr/bin/env python3
"""Run the three-node Docker cluster acceptance example using Python's standard library."""

import argparse
import collections
import datetime
import hashlib
import html
import json
import os
import re
import subprocess
import sys
import time
import traceback
import unicodedata
import urllib.request
import uuid
from pathlib import Path


BASE = Path(__file__).resolve().parent
REPO = BASE.parent.parent
API_KEY = "mocra-local-acceptance"
NODES = (1, 2, 3)
TOPICS = {f"{stage}-{priority}" for stage in
          ("task", "request", "response", "parser_task", "error_task")
          for priority in ("high", "normal", "low")}


def integer(value, label):
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError(f"{label}: expected a nonnegative integer, got {value!r}")
    return value


def read_jsonl(paths):
    records = []
    for path in paths:
        if not path.exists():
            continue
        text = path.read_text(encoding="utf-8")
        lines = text.splitlines()
        # A running sink can be midway through its final write.
        if text and not text.endswith("\n"):
            lines = lines[:-1]
        for number, line in enumerate(lines, 1):
            if line.strip():
                record = json.loads(line)
                if not isinstance(record, dict):
                    raise ValueError(f"{path}:{number}: JSONL record must be an object")
                records.append(record)
    return records


def normalize(value):
    return unicodedata.normalize("NFC", " ".join(value.split()))


class Acceptance:
    def __init__(self, args):
        self.args = args
        self.token = uuid.uuid4().hex[:12]
        self.project = f"mocra-06-{self.token}"
        self.namespace = f"mocra_06_{self.token}"
        self.run_dir = REPO / "data" / "cluster_acceptance" / self.token
        self.image = args.image or f"{self.project}:local"
        cargo = (REPO / "Cargo.toml").read_text(encoding="utf-8")
        package = re.search(r"(?ms)^\[package\]\s*\n(.*?)(?=^\[|\Z)", cargo)
        version = re.search(r'^version\s*=\s*"([^"]+)"', package.group(1), re.M) if package else None
        if not version:
            raise ValueError("Cannot determine the current mocra package version")
        self.package_version = version.group(1)
        self.env = dict(os.environ, MOCRA_ACCEPTANCE_PROJECT=self.project,
                        MOCRA_ACCEPTANCE_RUN_DIR=str(self.run_dir),
                        MOCRA_ACCEPTANCE_IMAGE=self.image)
        self.compose_prefix = ["docker", "compose", "-p", self.project,
                               "-f", str(BASE / "compose.yml")]
        self.ip_history = {}
        self.started = False
        self.command_number = 0
        self.summary = {
            "status": "running", "project": self.project, "namespace": self.namespace,
            "run_dir": str(self.run_dir), "image": self.image, "package_version": self.package_version,
            "started_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
            "topology": {"crawler_nodes": 3, "raft_voters": 3, "nats_instances": 1},
            "scenarios": [], "public_site": "skipped" if args.skip_public_site else "pending",
            "boundaries": [
                "All containers use one Docker host; host and network failures are not covered.",
                "NATS is one JetStream instance, not a replicated broker deployment.",
                "These observations do not establish exactly-once delivery or eliminate stage-handoff crash-loss windows.",
            ],
        }

    def save(self, name, document):
        path = self.run_dir / f"{name}.json"
        temporary = path.with_suffix(".json.tmp")
        temporary.write_text(json.dumps(document, indent=2, ensure_ascii=False) + "\n",
                             encoding="utf-8")
        temporary.replace(path)

    def command(self, args, timeout=120, check=True):
        self.command_number += 1
        path = self.run_dir / "logs" / f"command-{self.command_number:03d}.log"
        with path.open("w", encoding="utf-8") as log:
            log.write(json.dumps(args) + "\n")
            log.flush()
            result = subprocess.run(args, cwd=REPO, env=self.env, stdout=log,
                                    stderr=subprocess.STDOUT, text=True, timeout=timeout)
        output = path.read_text(encoding="utf-8")
        if check and result.returncode:
            raise RuntimeError(f"Command exited {result.returncode}; see {path}:\n{output[-3000:]}")
        return output.split("\n", 1)[1]

    def compose(self, *args, override=None, **kwargs):
        command = self.compose_prefix + (["-f", str(override)] if override else []) + list(args)
        return self.command(command, **kwargs)

    def http(self, url, payload=None):
        headers = {"x-api-key": API_KEY}
        data = None
        if payload is not None:
            data = json.dumps(payload).encode("utf-8")
            headers["Content-Type"] = "application/json"
        request = urllib.request.Request(url, data=data, headers=headers)
        with urllib.request.urlopen(request, timeout=5) as response:
            body = response.read()
            return json.loads(body) if body else {"http_status": response.status}

    def discover(self):
        ids = self.compose("ps", "-aq").split()
        if not ids:
            raise RuntimeError("No containers found for this acceptance project")
        inspected = json.loads(self.command(["docker", "inspect", *ids]))
        discovered = {}
        for container in inspected:
            labels = container["Config"]["Labels"]
            if labels.get("com.docker.compose.project") != self.project:
                raise ValueError("Refusing to use a container from another Compose project")
            if labels.get("com.docker.compose.oneoff") == "True":
                continue
            service = labels["com.docker.compose.service"]
            port = "12800/tcp" if service.startswith("node") else {
                "nats": "8222/tcp", "site": "8000/tcp"}.get(service)
            bindings = container["NetworkSettings"]["Ports"].get(port) if port else None
            running = container["State"]["Running"]
            if running and (not bindings or len(bindings) != 1 or bindings[0]["HostIp"] != "127.0.0.1"):
                raise ValueError(f"{service}: expected exactly one loopback port mapping")
            ips = [network["IPAddress"] for network in container["NetworkSettings"]["Networks"].values()
                   if network.get("IPAddress")]
            discovered[service] = {
                "id": container["Id"], "running": running,
                "exit_code": container["State"]["ExitCode"], "ips": ips,
                "api": f"http://127.0.0.1:{bindings[0]['HostPort']}" if bindings else None,
            }
            if service in {f"node{node}" for node in NODES}:
                for ip in ips:
                    self.ip_history[ip] = int(service[4:])
        return discovered

    def snapshot(self, ids=NODES):
        containers = self.discover()
        snapshots = {}
        for node_id in ids:
            node = containers[f"node{node_id}"]
            if not node["running"] or not node["api"]:
                raise RuntimeError(f"node{node_id} is not running")
            cluster = self.http(node["api"] + "/observability/cluster")
            engine = self.http(node["api"] + "/observability/engine")
            if not isinstance(cluster, dict) or cluster.get("node_id") != node_id:
                raise ValueError(f"node{node_id}: invalid cluster status")
            for key in ("term", "member_count", "voter_count"):
                integer(cluster[key], f"node{node_id}.{key}")
            if not isinstance(cluster["is_leader"], bool):
                raise ValueError(f"node{node_id}: is_leader must be a boolean")
            if not isinstance(engine, dict) or engine.get("namespace") != self.namespace or engine.get("clustered") is not True:
                raise ValueError(f"node{node_id}: engine is not using this cluster namespace")
            integer(engine["outstanding"]["total"], f"node{node_id}.outstanding.total")
            snapshots[node_id] = {"cluster": cluster, "engine": engine}
        return snapshots

    def poll(self, label, check, timeout=None, interval=0.5):
        deadline = time.monotonic() + (timeout or self.args.timeout)
        last_error = "condition not met"
        while time.monotonic() < deadline:
            try:
                result = check()
                if result:
                    return result
            except (OSError, RuntimeError) as error:
                last_error = str(error)
            time.sleep(interval)
        raise TimeoutError(f"{label}: {last_error}")

    def ready(self, ids=NODES, minimum_term=0, minimum_index=0):
        def check():
            states = self.snapshot(ids)
            leaders = {state["cluster"]["current_leader"] for state in states.values()}
            if len(leaders) != 1 or not leaders.issubset(set(ids)):
                return None
            leader = next(iter(leaders))
            for node_id, state in states.items():
                cluster = state["cluster"]
                if (cluster["member_count"] != 3 or cluster["voter_count"] != 3 or
                        cluster["is_leader"] != (node_id == leader) or
                        cluster["term"] < minimum_term or
                        cluster["last_applied_index"] is None or
                        integer(cluster["last_applied_index"], "last_applied_index") < minimum_index):
                    return None
            return states
        states = self.poll("waiting for a ready three-voter cluster", check)
        print(f"Cluster ready: live nodes {list(ids)}, leader {next(iter(states.values()))['cluster']['current_leader']}", flush=True)
        return states

    def broker(self):
        node = self.discover()["nats"]
        document = self.http(node["api"] + "/jsz?accounts=true&streams=true&consumers=true&config=true")
        name = self.namespace + "_stream"
        streams = [stream for account in document["account_details"]
                   for stream in account.get("stream_detail", []) if stream["name"] == name]
        if len(streams) != 1:
            raise RuntimeError(f"Expected stream {name}, found {len(streams)}")
        consumers = streams[0].get("consumer_detail")
        if not isinstance(consumers, list) or len(consumers) != 15:
            raise RuntimeError(f"{name}: expected 15 topic consumers")
        topics = set()
        for consumer in consumers:
            subject = consumer["config"]["filter_subject"]
            if not isinstance(subject, str) or not subject.startswith(self.namespace + "."):
                raise ValueError(f"{name}: invalid consumer filter subject")
            topics.add(subject[len(self.namespace) + 1:])
            for key in ("num_pending", "num_ack_pending", "num_redelivered"):
                integer(consumer[key], f"consumer.{key}")
        if topics != TOPICS:
            raise ValueError(f"{name}: unexpected topic consumer set {sorted(topics)}")
        return {
            "stream": name, "consumer_count": len(consumers), "consumers": consumers,
            "num_pending": sum(c["num_pending"] for c in consumers),
            "num_ack_pending": sum(c["num_ack_pending"] for c in consumers),
            "num_redelivered": sum(c["num_redelivered"] for c in consumers),
            "request_num_redelivered": sum(c["num_redelivered"] for c in consumers
                                           if c["config"]["filter_subject"].endswith(".request-normal")),
        }

    def rows(self):
        return read_jsonl(sorted((self.run_dir / "output").glob("node-*.jsonl")))

    def audit(self):
        return read_jsonl([self.run_dir / "http" / "http-audit.jsonl"])

    def submit(self, module, phase, node_id, run_id):
        node = self.discover()[f"node{node_id}"]
        response = self.http(node["api"] + "/start_work", {
            "platform": phase, "account": "acceptance", "module": [module],
            "priority": "normal", "run_id": run_id,
        })
        self.save(f"submission-{phase}", {"run_id": run_id, "node_id": node_id, "response": response})

    def manifest(self, phase, count, delay=0.04):
        path = self.run_dir / "control" / "manifest.json"
        temporary = path.with_suffix(".json.tmp")
        temporary.write_text(json.dumps({"phase": phase, "count": count, "delay_seconds": delay}),
                             encoding="utf-8")
        temporary.replace(path)

    def distribution(self, records, phase):
        parsed = collections.Counter(str(record["node_id"]) for record in records)
        downloaded = collections.Counter()
        for record in self.audit():
            if record.get("phase") == phase and record.get("event") == "end" and record.get("status") == 200:
                node_id = self.ip_history.get(record["remote_ip"])
                if node_id is None:
                    raise ValueError(f"Unrecognized crawler IP in HTTP audit: {record['remote_ip']}")
                downloaded[str(node_id)] += 1
        return dict(parsed), dict(downloaded)

    def wait_output(self, phase, run_id, ids, summarize):
        stable = 0
        peak_redelivered = 0
        peak_request_redelivered = 0
        started = time.monotonic()
        next_progress = 0

        def check():
            nonlocal stable, peak_redelivered, peak_request_redelivered, next_progress
            previous_stable = stable
            stable = 0
            all_records = self.rows()
            if any(record.get("phase") == phase and record.get("run_id") != run_id
                   for record in all_records):
                raise ValueError(f"{phase}: output contains an unexpected run_id")
            records = [record for record in all_records if record.get("run_id") == run_id]
            result, complete = summarize(records)
            states = self.snapshot(ids)
            broker = self.broker()
            peak_redelivered = max(peak_redelivered, broker["num_redelivered"])
            peak_request_redelivered = max(peak_request_redelivered, broker["request_num_redelivered"])
            idle = all(state["engine"]["outstanding"]["total"] == 0 for state in states.values())
            drained = broker["num_pending"] == 0 and broker["num_ack_pending"] == 0
            stable = previous_stable + 1 if complete and idle and drained else 0
            elapsed = time.monotonic() - started
            result.update({"phase": phase, "run_id": run_id, "elapsed_seconds": elapsed,
                           "status": "passed" if stable >= 2 else "waiting",
                           "snapshot": states, "broker": broker, "broker_drained": drained,
                           "stable_samples": stable, "peak_num_redelivered": peak_redelivered,
                           "peak_request_num_redelivered": peak_request_redelivered})
            self.save(f"phase-{phase}", result)
            if elapsed >= next_progress:
                print(f"{phase}: {result['raw']} records, {result['unique']} unique, drained={drained}", flush=True)
                next_progress = elapsed + 10
            return result if stable >= 2 else None

        result = self.poll(f"{phase}: complete output and two idle/drained samples", check)
        self.summary["scenarios"].append({key: value for key, value in result.items()
                                          if key not in ("snapshot", "broker")})
        self.save("summary", self.summary)
        return result

    def wait_phase(self, phase, count, run_id, ids=NODES, require_distribution=False):
        expected = {(kind, item_id) for kind in ("page", "detail") for item_id in range(count)}

        def summarize(records):
            identities = set()
            for record in records:
                kind, item_id = record["kind"], record["id"]
                integer(item_id, "corpus.id")
                integer(record["node_id"], "corpus.node_id")
                identity = kind, item_id
                if (identity not in expected or record["phase"] != phase or
                        record["node_id"] not in NODES or record["value"] != f"value-{item_id}" or
                        record["source_url"] != f"http://site:8000/{kind}/{phase}/{item_id}" or
                        record["http_status"] != 200):
                    raise ValueError(f"Unexpected corpus record: {record}")
                if identity in identities:
                    raise ValueError(f"Duplicate corpus output for run {run_id}: {identity}")
                identities.add(identity)
            parsed, downloaded = self.distribution(records, phase)
            complete = identities == expected
            if complete and require_distribution:
                wanted = {str(node_id) for node_id in ids}
                if set(parsed) != wanted or set(downloaded) != wanted:
                    raise ValueError(f"{phase}: every live node must download and parse, got {parsed}/{downloaded}")
            return {"raw": len(records), "unique": len(identities), "duplicates": 0,
                    "missing": [{"kind": kind, "id": item_id} for kind, item_id in sorted(expected - identities)],
                    "per_parse_node": parsed, "per_download_node": downloaded}, complete

        return self.wait_output(phase, run_id, ids, summarize)

    def phase(self, phase, count, ids=NODES, node_id=None, delay=0.04, require_distribution=False):
        self.manifest(phase, count, delay)
        run_id = str(uuid.uuid4())
        self.submit("corpus", phase, node_id or ids[0], run_id)
        return self.wait_phase(phase, count, run_id, ids, require_distribution)

    def quotes(self):
        expected = set()
        expected_rows = []
        urls = {"https://quotes.toscrape.com/"} | {
            f"https://quotes.toscrape.com/page/{page}/" for page in range(2, 11)}
        for page in range(1, 11):
            url = "https://quotes.toscrape.com/" if page == 1 else f"https://quotes.toscrape.com/page/{page}/"
            request = urllib.request.Request(url, headers={"User-Agent": "mocra-cluster-acceptance/0.6.0"})
            with urllib.request.urlopen(request, timeout=30) as response:
                document = response.read().decode("utf-8")
            texts = re.findall(r'<span class="text"[^>]*>(.*?)</span>', document, re.S)
            authors = re.findall(r'<small class="author"[^>]*>(.*?)</small>', document, re.S)
            if len(texts) != 10 or len(authors) != 10:
                raise ValueError(f"Expected ten public quotes and authors at {url}")
            for text, author in zip(texts, authors):
                pair = normalize(html.unescape(text)), normalize(html.unescape(author))
                expected.add(pair)
                expected_rows.append({"text": pair[0], "author": pair[1]})
        if len(expected_rows) != 100 or len(expected) != 100:
            raise ValueError("Public quote baseline must contain exactly 100 unique text/author pairs")
        self.save("expected-quotes", expected_rows)
        run_id = str(uuid.uuid4())
        self.submit("quotes", "quotes", 1, run_id)

        def summarize(records):
            identities, sources = set(), set()
            for record in records:
                pair = normalize(record["text"]), normalize(record["author"])
                if (record["kind"] != "quote" or pair not in expected or
                        record["node_id"] not in NODES or record["source_url"] not in urls or
                        record["http_status"] != 200):
                    raise ValueError(f"Unexpected quote record: {record}")
                if pair in identities:
                    raise ValueError(f"Duplicate public quote output: {pair}")
                identities.add(pair)
                sources.add(record["source_url"])
            return {"raw": len(records), "unique": len(identities), "duplicates": 0,
                    "missing": len(expected - identities), "source_urls": sorted(sources),
                    "per_parse_node": dict(collections.Counter(str(record["node_id"]) for record in records))}, identities == expected and sources == urls

        self.wait_output("quotes", run_id, NODES, summarize)
        self.summary["public_site"] = "passed"

    def initialize(self):
        override = self.run_dir / "init.compose.json"
        override.write_text(json.dumps({"services": {
            f"node{node_id}": {"command": ["init"]} for node_id in NODES}}), encoding="utf-8")
        self.compose("up", "-d", "--force-recreate", "node1", "node2", "node3", override=override)

        def initialized():
            events = []
            for node_id in NODES:
                output = self.compose("logs", "--no-color", "--no-log-prefix", f"node{node_id}", override=override)
                records = []
                for line in output.splitlines():
                    try:
                        record = json.loads(line)
                    except ValueError:
                        continue
                    if isinstance(record, dict) and record.get("event") == "INITIALIZED":
                        records.append(record)
                if not records:
                    return None
                record = records[-1]
                if (record.get("node_id") != node_id or record.get("member_count") != 3 or
                        record.get("voter_count") != 3 or record.get("package_version") != self.package_version):
                    raise ValueError(f"Invalid initialized Raft node: {record}")
                events.append(record)
            return events

        self.save("initialization", self.poll("Raft initialization", initialized))
        self.compose("stop", "-t", "30", "node1", "node2", "node3", override=override)
        self.assert_stopped(NODES)
        self.compose("up", "-d", "--force-recreate", "node1", "node2", "node3")
        self.save("initial-ready", self.ready())

    def assert_stopped(self, ids, graceful=True):
        containers = self.discover()
        states = {f"node{node_id}": containers[f"node{node_id}"] for node_id in ids}
        for service, node in states.items():
            if node["running"] or (graceful and node["exit_code"] != 0):
                raise ValueError(f"{service}: expected stopped{' with exit 0' if graceful else ''}, got {node}")
        return states

    def leader_failure(self):
        before = self.ready()
        leader = next(iter(before.values()))["cluster"]["current_leader"]
        term = max(state["cluster"]["term"] for state in before.values())
        survivors = tuple(node_id for node_id in NODES if node_id != leader)
        started = time.monotonic()
        self.compose("kill", "-s", "SIGKILL", f"node{leader}")
        self.assert_stopped((leader,), graceful=False)
        after = self.ready(survivors, minimum_term=term + 1)
        elected = next(iter(after.values()))["cluster"]["current_leader"]
        follower = next(node_id for node_id in survivors if node_id != elected)
        api = self.discover()[f"node{follower}"]["api"]
        controls = {action: self.http(api + f"/control/{action}", {}) for action in ("pause", "resume")}
        if any(response != {"http_status": 200} for response in controls.values()):
            raise ValueError(f"Follower pause/resume failed: {controls}")
        self.save("leader-failure", {
            "killed_node": leader, "previous_term": term, "new_leader": elected,
            "election_seconds": time.monotonic() - started, "follower_write_node": follower,
            "controls": controls, "snapshot": after,
        })
        self.phase("after_leader_failure", 400, survivors, node_id=follower, require_distribution=True)
        self.compose("up", "-d", f"node{leader}")
        self.save("leader-restored", self.ready())

    def worker_failure(self):
        states = self.ready()
        leader = next(iter(states.values()))["cluster"]["current_leader"]
        victim = next(node_id for node_id in NODES if node_id != leader)
        survivors = tuple(node_id for node_id in NODES if node_id != victim)
        victim_ips = set(self.discover()[f"node{victim}"]["ips"])
        before = self.broker()
        phase = "worker_replay"
        self.manifest(phase, 30, delay=3.0)
        run_id = str(uuid.uuid4())
        self.submit("corpus", phase, leader, run_id)

        def active_request():
            records = [record for record in self.audit() if record.get("phase") == phase]
            completed = {record["request_token"] for record in records if record["event"] == "end"}
            return next((record for record in reversed(records)
                         if record["event"] == "begin" and record["remote_ip"] in victim_ips
                         and record["request_token"] not in completed), None)

        interrupted = self.poll("worker must have an HTTP request in flight before kill", active_request,
                                timeout=30, interval=0.05)
        kill_started_ns = time.time_ns()
        self.compose("kill", "-s", "SIGKILL", f"node{victim}")
        kill_completed_ns = time.time_ns()
        self.assert_stopped((victim,), graceful=False)
        if any(record["event"] == "end" and record["request_token"] == interrupted["request_token"]
               and record["time_ns"] < kill_completed_ns for record in self.audit()):
            raise ValueError("The selected HTTP request completed before the worker kill; no in-flight fault was proven")
        self.ready(survivors)
        result = self.wait_phase(phase, 30, run_id, survivors)
        replay = [record for record in self.audit() if record.get("phase") == phase
                  and record["path"] == interrupted["path"]]
        remote_replays = [record for record in replay if record["event"] == "begin"
                          and record["remote_ip"] not in victim_ips
                          and record["time_ns"] >= kill_completed_ns]
        successful_tokens = {record["request_token"] for record in replay
                             if record["event"] == "end" and record["status"] == 200}
        remote_replays = [record for record in remote_replays if record["request_token"] in successful_tokens]
        output = [record for record in self.rows() if record.get("run_id") == run_id
                  and record.get("source_url") == "http://site:8000" + interrupted["path"]]
        if (not remote_replays or len(output) != 1 or output[0]["node_id"] not in survivors or
                result["peak_request_num_redelivered"] <= before["request_num_redelivered"]):
            raise ValueError("Worker replay needs another node's successful HTTP retry, unique output, and observed NATS request redelivery")
        self.save("worker-failure", {
            "run_id": run_id, "killed_node": victim, "victim_ips": sorted(victim_ips),
            "kill_started_ns": kill_started_ns, "kill_completed_ns": kill_completed_ns,
            "interrupted_request": interrupted, "replay_audit": replay, "replayed_output": output[0],
            "peak_request_num_redelivered": result["peak_request_num_redelivered"],
            "redownload_seconds": (remote_replays[0]["time_ns"] - interrupted["time_ns"]) / 1e9,
        })
        self.compose("up", "-d", f"node{victim}")
        self.ready()
        self.phase("after_worker_restart", 150, require_distribution=True)

    def blobs(self):
        self.phase("blob", 120, require_distribution=True)
        self.compose("stop", "-t", "30", "node1")
        self.assert_stopped((1,))
        self.ready((2, 3))
        output = self.compose("run", "--rm", "--no-deps", "node1", "blob-probe")
        producers = []
        for line in output.splitlines():
            try:
                record = json.loads(line)
            except ValueError:
                continue
            if isinstance(record, dict) and record.get("event") == "BLOB_PROBE_PUBLISHED":
                producers.append(record)
        if len(producers) != 1:
            raise ValueError(f"Expected one shared-blob producer record, found {len(producers)}")
        producer = producers[0]
        if (producer["storage_root"] != "/blobs" or not producer["storage_path"] or
                integer(producer["content_bytes"], "blob.content_bytes") < 70 * 1024 or
                integer(producer["queue_payload_bytes"], "blob.queue_payload_bytes") >= producer["content_bytes"]):
            raise ValueError(f"Blob probe did not offload its content: {producer}")
        run_id = str(uuid.UUID(producer["run_id"]))

        def summarize(records):
            if len(records) > 1:
                raise ValueError("Shared blob probe produced duplicate records")
            if records:
                record = records[0]
                if (record["node_id"] not in (2, 3) or record["kind"] != "detail" or
                        record["phase"] != "blob_remote" or record["id"] != 42 or
                        record["value"] != "value-42" or record["http_status"] != 200 or
                        record["request_id"] != producer["request_id"] or
                        record["source_url"] != "http://site:8000/detail/blob_remote/42"):
                    raise ValueError(f"Incorrect shared blob probe output: {record}")
            return {"raw": len(records), "unique": len(records), "duplicates": 0,
                    "producer": producer, "output": records}, len(records) == 1

        self.wait_output("blob_remote", run_id, (2, 3), summarize)
        self.compose("up", "-d", "node1")
        self.ready()

    def graceful_restart(self):
        before = self.ready()
        term = max(state["cluster"]["term"] for state in before.values())
        index = min(state["cluster"]["last_applied_index"] for state in before.values())
        self.save("before-graceful-restart", before)
        self.compose("stop", "-t", "30", "node1", "node2", "node3")
        self.save("graceful-stop", self.assert_stopped(NODES))
        self.compose("up", "-d", "node1", "node2", "node3")
        self.save("graceful-restored", self.ready(minimum_term=term, minimum_index=index))
        self.phase("graceful_restore", 90, require_distribution=True)

    def prepare(self):
        for name in ("config", "control", "output", "http", "logs"):
            (self.run_dir / name).mkdir(parents=True, exist_ok=False)
        template = (BASE / "node.toml").read_text(encoding="utf-8")
        if "{{NAMESPACE}}" not in template or "{{NODE_ID}}" not in template:
            raise ValueError("node.toml must contain {{NAMESPACE}} and {{NODE_ID}} placeholders")
        for node_id in NODES:
            (self.run_dir / "config" / f"node{node_id}.toml").write_text(
                template.replace("{{NAMESPACE}}", self.namespace).replace("{{NODE_ID}}", str(node_id)),
                encoding="utf-8")
        self.manifest("preflight", 1)
        self.save("summary", self.summary)
        self.command(["docker", "info", "--format", "{{.ServerVersion}}"])
        self.compose("version")
        self.summary["source"] = {
            "commit": self.command(["git", "rev-parse", "HEAD"]).strip(),
            "tracked_diff_sha256": hashlib.sha256(self.command(["git", "diff", "HEAD"]).encode()).hexdigest(),
        }
        source_files = [REPO / "Cargo.toml", REPO / "examples" / "cluster_acceptance.rs"]
        if (REPO / "Cargo.lock").exists():
            source_files.append(REPO / "Cargo.lock")
        source_files.extend((REPO / "src").rglob("*.rs"))
        source_files.extend(path for path in BASE.iterdir() if path.is_file())
        self.summary["source"]["file_sha256"] = {
            str(path.relative_to(REPO)): hashlib.sha256(path.read_bytes()).hexdigest()
            for path in sorted(source_files)
        }
        self.save("summary", self.summary)
        if not self.args.skip_build:
            print(f"Building {self.image}; logs: {self.run_dir / 'logs'}", flush=True)
            self.compose("build", "node1", timeout=3600)
        images = json.loads(self.command(["docker", "image", "inspect", self.image]))
        self.summary["image_id"] = images[0]["Id"]
        self.summary["docker_platform"] = images[0]["Os"] + "/" + images[0]["Architecture"]

    def run(self):
        self.prepare()
        print(f"Acceptance project: {self.project}\nResults: {self.run_dir}", flush=True)
        self.started = True
        self.compose("up", "-d", "nats", "site")

        def dependencies_ready():
            containers = self.discover()
            if self.http(containers["nats"]["api"] + "/healthz").get("status") != "ok":
                return None
            with urllib.request.urlopen(containers["site"]["api"] + "/health", timeout=5) as response:
                if response.read() != b"ok\n":
                    return None
            return True

        self.poll("NATS and local HTTP corpus readiness", dependencies_ready)
        self.initialize()
        if not self.args.skip_public_site:
            self.quotes()
        self.phase("baseline", 600, require_distribution=True)
        self.leader_failure()
        self.worker_failure()
        self.blobs()
        self.graceful_restart()
        self.summary["final_snapshot"] = self.ready()
        self.summary["final_broker"] = self.broker()
        self.summary["status"] = "passed"

    def finish(self):
        if self.started:
            try:
                self.compose("logs", "--no-color", timeout=60)
                self.save("final-containers", self.discover())
            except (OSError, RuntimeError, subprocess.TimeoutExpired) as error:
                self.summary["log_collection_error"] = str(error)
            if self.args.keep:
                self.summary["cleanup"] = "kept"
            else:
                try:
                    self.compose("down", "--volumes", "--remove-orphans", timeout=120)
                    self.summary["cleanup"] = "removed this run's containers, network and volumes"
                except (OSError, RuntimeError, subprocess.TimeoutExpired) as error:
                    self.summary["cleanup"] = "failed: " + str(error)
                    self.summary["status"] = "failed"
        self.summary["finished_at"] = datetime.datetime.now(datetime.timezone.utc).isoformat()
        if self.run_dir.exists():
            self.save("summary", self.summary)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--skip-build", action="store_true", help="Use an already built --image")
    parser.add_argument("--image", help="Crawler image tag; default is a unique tag for this run")
    parser.add_argument("--skip-public-site", action="store_true", help="Skip the external 100-quote test")
    parser.add_argument("--keep", action="store_true", help="Keep this project's containers and volumes")
    parser.add_argument("--timeout", type=float, default=240, help="Per-scenario wait limit in seconds")
    args = parser.parse_args()
    if args.timeout <= 0:
        parser.error("--timeout must be positive")
    if args.skip_build and not args.image:
        parser.error("--skip-build requires --image pointing to the current example build")
    acceptance = Acceptance(args)
    try:
        acceptance.run()
    except (Exception, KeyboardInterrupt) as error:
        acceptance.summary["status"] = "failed"
        acceptance.summary["error"] = f"{type(error).__name__}: {error}"
        if acceptance.run_dir.exists():
            (acceptance.run_dir / "logs" / "failure.log").write_text(traceback.format_exc(), encoding="utf-8")
        print(acceptance.summary["error"], file=sys.stderr, flush=True)
    finally:
        acceptance.finish()
    print(f"Acceptance {acceptance.summary['status']}; report: {acceptance.run_dir / 'summary.json'}", flush=True)
    return 0 if acceptance.summary["status"] == "passed" else 1


if __name__ == "__main__":
    sys.exit(main())

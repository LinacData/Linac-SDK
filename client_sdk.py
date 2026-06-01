"""
switchboard-ingress Layer 3 External Integration SDK
Decoupled background queue thread worker with native urllib HTTPS POST integration.
"""

import threading
import queue
import urllib.request
import json
from engine_core import IngressEngine
from tracker import IngressTracker

class Ingress:
    """
    User-facing Developer SDK client.
    Integrates Layer 1 Normalization and Layer 2 Metric Tracking.
    Decouples ledger synchronization asynchronously to a single, persistent
    daemon background worker thread running a Queue. Emits native HTTPS POST calls
    using urllib to maintain zero dependencies and absolute memory stability.
    """
    def __init__(self, api_key, delimiter=".", pulse_limit=10000, pulse_interval_sec=5.0, endpoint="http://127.0.0.1:8787/v1/pulse"):
        if not isinstance(api_key, str) or not api_key.startswith("sb_"):
            raise ValueError("Invalid API key format. API key must start with 'sb_'")
            
        self.api_key = api_key
        self.endpoint = endpoint
        self.engine = IngressEngine(delimiter=delimiter)
        self.tracker = IngressTracker(pulse_limit=pulse_limit, pulse_interval_sec=pulse_interval_sec)
        
        # High-performance persistent background queue and thread worker
        self._pulse_queue = queue.Queue()
        self._sync_thread = threading.Thread(target=self._sync_pulse_worker, daemon=True)
        self._sync_thread.start()

    def process(self, payload):
        """
        Ingests, flattens, and records metrics for a telemetry payload.
        Pushes Pulse Frames to the queue asynchronously when a flush occurs.
        """
        # 1. Layer 1 Ingestion and Flattening
        flat_data = self.engine.normalize(payload)
        
        # 2. Layer 2 Approximate Metric Recording
        t_payload = type(payload)
        if t_payload is str:
            byte_size = len(payload)
        elif t_payload is dict:
            # Fast, memory-conservative estimation for pre-parsed dictionaries
            byte_size = len(payload) * 32
        else:
            byte_size = 0
            
        pulse = self.tracker.track(byte_size)
        
        # 3. Layer 3 Non-Blocking Queue Push
        if pulse is not None:
            self._pulse_queue.put(pulse)
            
        return flat_data

    def _sync_pulse_worker(self):
        """Asynchronously dispatches Pulse Frames via native HTTPS POST to the Cloudflare Worker."""
        # Cache method and references locally to speed up worker loop
        get_pulse = self._pulse_queue.get
        task_done = self._pulse_queue.task_done
        api_key = self.api_key
        endpoint = self.endpoint
        
        while True:
            pulse = get_pulse()
            if pulse is None:
                task_done()
                break
                
            try:
                # Compile payload and configure Request
                data_bytes = json.dumps({
                    "packets": pulse["packets"],
                    "bytes": pulse["bytes"]
                }).encode("utf-8")
                
                req = urllib.request.Request(
                    endpoint,
                    data=data_bytes,
                    headers={
                        "Authorization": f"Bearer {api_key}",
                        "Content-Type": "application/json"
                    },
                    method="POST"
                )
                
                # Execute native HTTPS POST with a 2-second timeout
                with urllib.request.urlopen(req, timeout=2.0) as response:
                    res_body = response.read().decode("utf-8")
                    # Display verified ledger sync receipt
                    print(f"\n[Switchboard Ledger] Synced Pulse Frame: {res_body.strip()}")
            except Exception as net_err:
                # Fail gracefully without blocking or crashing the host application
                print(f"\n[Switchboard Ledger] Network Sync Fallback: {net_err}")
                
            task_done()

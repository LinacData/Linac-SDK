"""
switchboard-ingress Layer 2 Metric & Billing Tracker
Thread-safe, high-frequency, local metric accumulator with near-zero latency overhead.
"""

import threading
import time

class IngressTracker:
    """
    Thread-safe, in-memory metric accumulator.
    Tracks packet counts and processed bytes, batching them into discrete Pulse Frames
    using high-performance conditional triggers to maintain sub-microsecond overhead.
    """
    def __init__(self, pulse_limit=10000, pulse_interval_sec=5.0):
        self._packets = 0
        self._bytes = 0
        self._lock = threading.Lock()
        
        self.pulse_limit = pulse_limit
        self.pulse_interval_sec = pulse_interval_sec
        self._last_pulse_time = time.time()
        
        # History log for outbound ledger synchronization (Layer 3)
        self.pulse_history = []

    def track(self, byte_size):
        """
        Record a processed packet and its approximate byte size.
        Returns a Pulse Frame dictionary if a threshold limit is met, otherwise None.
        """
        with self._lock:
            self._packets += 1
            self._bytes += byte_size
            
            # Fast-path check: Trigger immediately if packet threshold is reached
            if self._packets >= self.pulse_limit:
                return self._flush_pulse()
            
            # Micro-optimization: check elapsed time only every 100 packets
            # to prevent expensive time.time() C-calls on every iteration.
            if self._packets % 100 == 0:
                now = time.time()
                if (now - self._last_pulse_time) >= self.pulse_interval_sec:
                    return self._flush_pulse(now)
                    
        return None

    def _flush_pulse(self, now=None):
        """Flushes active counters into a Pulse Frame package and resets counters."""
        if now is None:
            now = time.time()
        elapsed = now - self._last_pulse_time
        
        pulse_frame = {
            "packets": self._packets,
            "bytes": self._bytes,
            "timestamp": now,
            "elapsed_sec": elapsed
        }
        
        self.pulse_history.append(pulse_frame)
        if len(self.pulse_history) > 10:
            self.pulse_history.pop(0)
        
        # Reset active metrics
        self._packets = 0
        self._bytes = 0
        self._last_pulse_time = now
        
        return pulse_frame

    def get_stats(self):
        """Read-only access to current active in-memory counters."""
        with self._lock:
            return {
                "active_packets": self._packets,
                "active_bytes": self._bytes,
                "last_pulse_time": self._last_pulse_time,
                "pulse_history_count": len(self.pulse_history)
            }

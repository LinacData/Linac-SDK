"""
benchmark_engine.py - High-Density Parsing Engine Microsecond Performance Audit
Executes core matrix flattening diagnostics on heavily chaotic, deeply nested
telemetry packets under standard microsecond latency constraints.
"""

import time
import json
import sys
from engine_core import IngressEngine

def main():
    print("==================================================================")
    print("SWITCHBOARD INGRESS ENGINE PERFORMANCE AUDIT RUNNER (REV 3)")
    print("==================================================================")
    
    # 1. Initialize Ingress engine flattener
    engine = IngressEngine(delimiter=".")
    
    # 2. Compile heavily chaotic, deeply nested multi-type mock JSON data packet
    nested_packet = {
        "timestamp": "2026-05-31T20:11:49Z",
        "active": True,
        "device": {
            "id": "VTX-X99",
            "metadata": {
                "firmware": "V3.2.1",
                "vendor_details": {
                    "manufacturer": "AG Pixel Studio",
                    "serial": "SN-98103-BETA"
                }
            },
            "telemetry_arrays": [
                {"sensor_id": "SENS-1", "value": 12.4, "status": "OK"},
                {"sensor_id": "SENS-2", "value": 94.0, "status": "WARN"},
                {"sensor_id": "SENS-3", "value": -18.5, "status": "CRITICAL"}
            ]
        },
        "coordinates": {
            "gps": {
                "latitude": "43.6532",
                "longitude": "-79.3832"
            },
            "vectors": {
                "sog_knots": 14.5,
                "heading_degrees": 180.5
            }
        },
        "payload_checksum": "0xDEADBEEF"
    }
    
    # Pre-warmup iterations to stabilize CPython interpreter allocations
    print("Executing warmup runs to stabilize JIT/CPython interpreter memory allocations...")
    for _ in range(1000):
        engine.normalize(nested_packet)
        
    # 3. Launch high-density processing loop executing core flattening logic for 10,000 iterations
    iterations = 10000
    print(f"Starting {iterations:,} continuous iterations performance audit...")
    
    start_time = time.perf_counter()
    for _ in range(iterations):
        engine.normalize(nested_packet)
    end_time = time.perf_counter()
    
    # 4. Calculate absolute average processing time per packet in microseconds
    total_duration = end_time - start_time
    avg_micros = (total_duration / iterations) * 1_000_000
    
    print("\n--- Latency Performance Metrics Summary ---")
    print(f"Total Audit Execution Time : {total_duration * 1000:.3f} milliseconds")
    print(f"Average Speed Per Packet   : {avg_micros:.4f} microseconds")
    print(f"Latencies Boundary Limit   : 15.0000 microseconds")
    
    if avg_micros < 15.0:
        print("\nSTATUS: PASSED (Core engine processes successfully within target constraint limits)")
        exit_code = 0
    else:
        print("\nSTATUS: FAILED (Core engine latency exceeds target 15-microsecond threshold)")
        exit_code = 1
        
    print("==================================================================")
    sys.exit(exit_code)

if __name__ == "__main__":
    main()

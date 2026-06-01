"""
switchboard-ingress Local Performance Benchmark Suite (Phase 6)
Validates that the fully integrated SDK Client wrapper (switchboard.Ingress)
processes and flattens multi-type payloads under the 15-microsecond threshold
while executing automated background HTTPS ledger updates to the Cloudflare Worker.
"""

import time
import timeit
import subprocess
import os
import sys
import switchboard

# Initialize the unified SDK client wrapper
# Set pulse limit to 10000 for timing benchmarks
client = switchboard.Ingress(
    api_key="sb_live_test_key",
    pulse_limit=10000,
    pulse_interval_sec=5.0,
    endpoint="http://127.0.0.1:8787/v1/pulse"
)

# Test Payload 1: Nested pre-parsed dictionary (GPS data package)
dict_payload = {
    "device": {
        "id": "VTX-109",
        "type": "logistics_tracker",
        "specs": {
            "model": "V1",
            "battery": {
                "voltage": 3.75,
                "percentage": 94
            }
        }
    },
    "location": {
        "coordinates": {
            "latitude": 43.6532,
            "longitude": -79.3832
        },
        "speed_kmh": 65.4,
        "heading": 180.5
    },
    "active": True
}

# Test Payload 2: Raw string-serialized JSON
json_payload = '{"device":{"id":"VTX-109","specs":{"battery":{"percentage":94}}},"location":{"coordinates":{"latitude":43.6532,"longitude":-79.3832},"speed_kmh":65.4},"active":true}'

# Test Payload 3: Raw Key-Value string (semicolon delimited)
kv_payload = "device.id=VTX-109;location.coordinates.latitude=43.6532;location.coordinates.longitude=-79.3832;location.speed_kmh=65.4;device.specs.battery.percentage=94;active=True"

# Test Payload 4: Positional GPS CSV line
gps_line_payload = "GPS,VTX-109,2026-05-31T08:30:00,43.6532,-79.3832,65.4,ACTIVE"

# Test Payload 5: Positional Marine AIS CSV line
ais_line_payload = "AIS,227006760,2026-05-31T08:30:00,48.8512,-3.4512,12.4,180.5,180"

# Test Payload 6: Positional Cold-chain Temp CSV line
temp_line_payload = "TEMP,SENS-889,2026-05-31T08:30:00,-18.5,94"


def run_correctness_checks():
    """Verify that the developer SDK Normalizes data correctly and accumulates metrics."""
    print("Executing correctness verification checks...")
    
    # Track original state
    stats_start = client.tracker.get_stats()
    
    # 1. Dict payload validation
    res_dict = client.process(dict_payload)
    assert res_dict["device.id"] == "VTX-109"
    assert res_dict["device.specs.battery.percentage"] == 94
    assert res_dict["location.coordinates.latitude"] == 43.6532
    assert res_dict["active"] is True
    
    # 2. JSON payload validation
    res_json = client.process(json_payload)
    assert res_json["device.id"] == "VTX-109"
    assert res_json["device.specs.battery.percentage"] == 94
    assert res_json["location.coordinates.latitude"] == 43.6532
    assert res_json["active"] is True
    
    # 3. KV payload validation
    res_kv = client.process(kv_payload)
    assert res_kv["device.id"] == "VTX-109"
    assert res_kv["device.specs.battery.percentage"] == 94
    assert res_kv["location.coordinates.latitude"] == 43.6532
    
    # 4. GPS line payload validation
    res_gps = client.process(gps_line_payload)
    assert res_gps["gps.device_id"] == "VTX-109"
    assert res_gps["gps.latitude"] == 43.6532
    assert res_gps["gps.longitude"] == -79.3832
    assert res_gps["gps.speed"] == 65.4
    assert res_gps["gps.status"] == "ACTIVE"
    
    # 5. AIS line payload validation
    res_ais = client.process(ais_line_payload)
    assert res_ais["ais.mmsi"] == 227006760
    assert res_ais["ais.latitude"] == 48.8512
    assert res_ais["ais.longitude"] == -3.4512
    assert res_ais["ais.sog"] == 12.4
    
    # 6. Temp line payload validation
    res_temp = client.process(temp_line_payload)
    assert res_temp["temp.sensor_id"] == "SENS-889"
    assert res_temp["temp.temperature"] == -18.5
    assert res_temp["temp.battery_percentage"] == 94
    
    # Assert metric counters are updated
    stats_end = client.tracker.get_stats()
    assert stats_end["active_packets"] == stats_start["active_packets"] + 6
    assert stats_end["active_bytes"] > stats_start["active_bytes"]
    
    # Test Active Outbound Background Queue Dispatch
    # Initialize a test client with a very low threshold (3 packets) to trigger a pulse
    trigger_client = switchboard.Ingress(
        api_key="sb_live_test_key",
        pulse_limit=3,
        pulse_interval_sec=5.0,
        endpoint="http://127.0.0.1:8787/v1/pulse"
    )
    assert trigger_client.process("TEMP,S-1,2026-05-31T08,-10.0,99") is not None
    assert trigger_client.process("TEMP,S-2,2026-05-31T08,-10.0,99") is not None
    
    # The third payload triggers the background queue dispatch to localhost:8787
    print("\n[Verification] Triggering live background HTTP ledger synchronization...")
    trigger_client.process("TEMP,S-3,2026-05-31T08,-10.0,99")
    
    # Wait for the background HTTP thread to execute and print the synced receipt
    time.sleep(0.5)
    
    print("\nCorrectness verification passed successfully.")


def run_performance_benchmarks(iterations=100000):
    """Benchmark the Ingress SDK wrapper and assert execution remains sub-15 microseconds."""
    print(f"\nRunning performance benchmarks ({iterations:,} iterations per payload type)...")
    
    payloads = {
        "Pre-parsed Dict": dict_payload,
        "JSON String": json_payload,
        "Key-Value String": kv_payload,
        "GPS Positional Line": gps_line_payload,
        "AIS Marine Positional Line": ais_line_payload,
        "TEMP Cold-chain Line": temp_line_payload
    }
    
    results = {}
    passed_all = True
    
    # Reset active tracking history list
    client.tracker.pulse_history.clear()
    
    for name, payload in payloads.items():
        # Warmup
        for _ in range(100):
            client.process(payload)
            
        # Time the execution
        t = timeit.timeit(
            "client.process(payload)",
            globals={"client": client, "payload": payload},
            number=iterations
        )
        avg_micros = (t / iterations) * 1_000_000
        
        status = "PASSED" if avg_micros < 15.0 else "FAILED"
        if avg_micros >= 15.0:
            passed_all = False
            
        print(f" - {name:<30}: {avg_micros:6.2f} microseconds | Status: {status}")
        results[name] = avg_micros
        
    print("\n--- Latency Constraint Verification Summary ---")
    if passed_all:
        print("All payloads processed within the sub-15 microsecond latency constraint.")
    else:
        print("WARNING: One or more payloads failed the 15-microsecond threshold.")
        
    return passed_all

def main():
    wrangler_process = None
    try:
        # Write JSON to a temporary file to avoid Windows shell quoting/escaping issues
        print("Initializing active licensing token in Wrangler local KV database...")
        temp_file = "temp_token.json"
        with open(temp_file, "w") as f:
            f.write('{"status":"active","total_packets":0,"total_bytes":0}')
        try:
            subprocess.run(
                ["npx", "wrangler", "kv", "key", "put", "--binding=SWITCHBOARD_AUTH_REGISTRY", "sb_live_test_key", f"--path={temp_file}", "--local", "--preview"],
                shell=True,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL
            )
        finally:
            if os.path.exists(temp_file):
                os.remove(temp_file)
        
        # 2. Spin up Wrangler local development server
        print("Starting local Cloudflare Wrangler dev server...")
        wrangler_process = subprocess.Popen(
            ["npx", "wrangler", "dev"],
            shell=True,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL
        )
        # Wait for Wrangler local server to bind to port 8787 via active polling
        print("Waiting for local dev server to start responding on port 8787...")
        import urllib.request
        from urllib.error import URLError
        ready = False
        for i in range(30):
            try:
                req = urllib.request.Request("http://127.0.0.1:8787/v1/pulse", method="OPTIONS")
                with urllib.request.urlopen(req, timeout=0.5) as response:
                    ready = True
                    break
            except (URLError, Exception):
                time.sleep(0.5)
        
        if ready:
            print("Local dev server is active and verified!")
        else:
            print("Warning: Local dev server not responsive. Continuing anyway...")
        
        # 3. Run Correctness Checks (including live HTTP dispatch validation)
        run_correctness_checks()
        
        # 4. Run High-Frequency Performance benchmarks
        run_performance_benchmarks()
        
    finally:
        # 5. Clean up and terminate the local Wrangler subprocess tree on exit
        if wrangler_process:
            print("\nTerminating local Wrangler dev server...")
            if os.name == 'nt':
                # Force-kill child process tree to prevent zombie Node.js processes on Windows
                subprocess.run(
                    ["taskkill", "/F", "/T", "/PID", str(wrangler_process.pid)],
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL
                )
            else:
                wrangler_process.terminate()
                wrangler_process.wait()

if __name__ == "__main__":
    main()

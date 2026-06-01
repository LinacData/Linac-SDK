"""
linac CLI and Developer SDK Command Stack Implementation
Provides full authentication, streaming telematics, local proxy gateway,
validation diagnostics, and billing management.
"""

import sys
import os
import json
import argparse
import urllib.request
import urllib.parse
import webbrowser
import time
import gzip
import concurrent.futures
import asyncio
import subprocess
from client_sdk import Ingress
from engine_core import IngressEngine

CREDENTIALS_DIR = os.path.expanduser("~/.config/linac")
CREDENTIALS_FILE = os.path.join(CREDENTIALS_DIR, "credentials.json")
LOCAL_CONFIG_FILE = ".linacjson"

def launch_app_window(url):
    """
    Attempts to launch the URL inside a native Chromium app-mode standalone window 
    using standard system browser installations (Google Chrome, Edge, or Brave).
    Falls back gracefully to webbrowser.open_new(url) on failure.
    Returns True if successfully launched, False otherwise.
    """

    
    if os.environ.get("FORCE_HEADLESS") == "true":
        return False
        
    paths = []
    
    if os.name == 'nt':  # Windows
        prog_files = os.environ.get("ProgramFiles", "C:\\Program Files")
        prog_files_x86 = os.environ.get("ProgramFiles(x86)", "C:\\Program Files (x86)")
        local_app_data = os.environ.get("LocalAppData", os.path.expanduser("~\\AppData\\Local"))
        
        paths = [
            # Chrome
            os.path.join(prog_files, "Google\\Chrome\\Application\\chrome.exe"),
            os.path.join(prog_files_x86, "Google\\Chrome\\Application\\chrome.exe"),
            os.path.join(local_app_data, "Google\\Chrome\\Application\\chrome.exe"),
            # Edge
            os.path.join(prog_files_x86, "Microsoft\\Edge\\Application\\msedge.exe"),
            os.path.join(prog_files, "Microsoft\\Edge\\Application\\msedge.exe"),
            # Brave
            os.path.join(prog_files, "BraveSoftware\\Brave-Browser\\Application\\brave.exe"),
            os.path.join(local_app_data, "BraveSoftware\\Brave-Browser\\Application\\brave.exe"),
        ]
    elif sys.platform == 'darwin':  # macOS
        paths = [
            "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome",
            "/Applications/Microsoft Edge.app/Contents/MacOS/Microsoft Edge",
            "/Applications/Brave Browser.app/Contents/MacOS/Brave Browser",
        ]
    else:  # Linux / Unix
        # Check if running in a headless environment (no DISPLAY variable)
        if not os.environ.get("DISPLAY"):
            return False
            
        paths = [
            "/usr/bin/google-chrome",
            "/usr/bin/microsoft-edge",
            "/usr/bin/brave-browser",
            "/usr/bin/chromium-browser",
            "/usr/bin/chromium",
        ]
        
    for path in paths:
        if os.path.exists(path):
            try:
                subprocess.Popen([path, f"--app={url}"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
                return True
            except Exception:
                pass
                
    try:
        webbrowser.open_new(url)
        return True
    except Exception:
        return False


def save_credentials(api_key):
    os.makedirs(CREDENTIALS_DIR, exist_ok=True)
    with open(CREDENTIALS_FILE, "w") as f:
        json.dump({"api_key": api_key}, f, indent=2)

def load_credentials():
    if not os.path.exists(CREDENTIALS_FILE):
        return None
    try:
        with open(CREDENTIALS_FILE, "r") as f:
            data = json.load(f)
            return data.get("api_key")
    except Exception:
        return None

def load_local_config():
    if not os.path.exists(LOCAL_CONFIG_FILE):
        return {}
    try:
        with open(LOCAL_CONFIG_FILE, "r") as f:
            return json.load(f)
    except Exception:
        return {}

def handle_init(args):
    """Initializes local project configuration (.linacjson)."""
    config = {
        "default_endpoint": "http://127.0.0.1:8787/v1/pulse",
        "default_schema": "logistics_tracker",
        "compression_enabled": True,
        "batch_size": 1000,
        "thread_workers": 4,
        "environment": "sandbox_beta"
    }
    with open(LOCAL_CONFIG_FILE, "w") as f:
        json.dump(config, f, indent=2)
    print(f"[Success] Initialized local project folder configuration in {LOCAL_CONFIG_FILE}")

def handle_auth_set_key(args):
    """
    Surgically registers the developer API key token with text safety,
    length, and printable ASCII checks, storing it in credentials.json.
    """
    api_key = args.api_key.strip()
    
    # Safety checks
    if not api_key:
        print("Error: API key cannot be empty.")
        sys.exit(1)
        
    if len(api_key) < 8:
        print("Error: API key is too short. Minimum length is 8 characters.")
        sys.exit(1)
        
    # Printable ASCII checks (safety & shell compatibility)
    if any(ord(c) < 32 or ord(c) > 126 for c in api_key):
        print("Error: API key contains invalid characters. Only standard printable ASCII characters are allowed.")
        sys.exit(1)
        
    # Load existing credentials if present
    try:
        if os.path.exists(CREDENTIALS_FILE):
            with open(CREDENTIALS_FILE, "r") as f:
                cred_data = json.load(f)
        else:
            cred_data = {}
    except Exception:
        cred_data = {}
        
    # Set the key and ensure valid beta unlocked state
    cred_data["api_key"] = api_key
    if "status" not in cred_data:
        cred_data["status"] = "beta_unlocked"
    if "limit" not in cred_data:
        cred_data["limit"] = "50_000_000"
        
    try:
        os.makedirs(CREDENTIALS_DIR, exist_ok=True)
        with open(CREDENTIALS_FILE, "w") as f:
            json.dump(cred_data, f, indent=2)
        print(">>> API key configured successfully.")
        print(f"[Success] Credentials written successfully to {CREDENTIALS_FILE}")
    except Exception as e:
        print(f"Error writing credentials to file: {e}")
        sys.exit(1)

def handle_billing_setup(args):
    local_cfg = load_local_config()
    environment = local_cfg.get("environment", "sandbox_beta")
    
    if environment == "disabled":
        print(">>> Linac Data is currently in Free Beta. No billing registration required!")
        return
        
    if environment == "sandbox_beta":
        print(">>> Connecting to Linac Ingress Network...")
        print(">>> Beta Phase Detected: Bypassing Dodo Payment Wall.")
        print(">>> Validating Free Beta Access...")
        
        token = load_credentials()
        if not token:
            print("Error: No authentication token found. Please run 'linac auth set-key <API_KEY>' first.")
            sys.exit(1)
            
        endpoint = local_cfg.get("default_endpoint") or "http://127.0.0.1:8787/v1/pulse"
        if "/v1/pulse" in endpoint:
            setup_endpoint = endpoint.replace("/v1/pulse", "/v1/billing/setup")
        else:
            setup_endpoint = endpoint.rsplit("/", 1)[0] + "/billing/setup"
            
        req = urllib.request.Request(
            setup_endpoint,
            data=b"",
            headers={
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json"
            },
            method="POST"
        )
        
        try:
            with urllib.request.urlopen(req, timeout=5.0) as res:
                res_body = json.loads(res.read().decode("utf-8"))
                
                try:
                    with open(CREDENTIALS_FILE, "r") as f:
                        cred_data = json.load(f)
                except Exception:
                    cred_data = {}
                    
                cred_data["status"] = "beta_unlocked"
                cred_data["limit"] = "50_000_000"
                cred_data["api_key"] = token
                
                with open(CREDENTIALS_FILE, "w") as f:
                    json.dump(cred_data, f, indent=2)
                    
                print(">>> Success! 50,000,000 Beta Packets credited. No card required.")
                print("[Success] System Unlocked! Your account has been granted 50M free beta units.")
        except Exception as e:
            print(f"\n[Error] Connection failed: {e}")
            
    elif environment == "production_live":
        print(">>> Generating your live production checkout portal...")
        token = load_credentials()
        if not token:
            print("Error: No authentication token found. Please run 'linac auth set-key <API_KEY>' first.")
            sys.exit(1)
            
        endpoint = local_cfg.get("default_endpoint") or "http://127.0.0.1:8787/v1/pulse"
        if "/v1/pulse" in endpoint:
            setup_endpoint = endpoint.replace("/v1/pulse", "/v1/billing/setup")
        else:
            setup_endpoint = endpoint.rsplit("/", 1)[0] + "/billing/setup"
            
        req = urllib.request.Request(
            setup_endpoint,
            data=b"",
            headers={
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json"
            },
            method="POST"
        )
        
        try:
            with urllib.request.urlopen(req, timeout=5.0) as res:
                res_body = json.loads(res.read().decode("utf-8"))
                checkout_url = res_body.get("checkout_url", "https://test.dodopayments.com/checkout/pdt_0Ng12GAt92ZzplghcjveE")
        except Exception as e:
            print(f"\n[Error] Connection failed: {e}")
            sys.exit(1)
            
        print(f"Binding payment instruments to checkout URL: {checkout_url}")
        
        if not launch_app_window(checkout_url):
            # Headless fallback
            print(">>> Linac Ingress Network -- Billing Activation <<<")
            print("\n[ALERT] No local browser engine was detected on this environment.")
            print("Please copy and paste this secure Dodo Payments gateway URL into")
            print("your personal desktop browser to safely attach your corporate card:")
            print(f"\n-> {checkout_url}")
            print("\n>>> Awaiting card linkage verification on the network...")
            
            if "/v1/pulse" in endpoint:
                status_endpoint = endpoint.replace("/v1/pulse", "/v1/auth/status")
            else:
                status_endpoint = endpoint.rsplit("/", 1)[0] + "/auth/status"
                
            unlocked = False
            while not unlocked:
                time.sleep(3.0)
                try:
                    status_req = urllib.request.Request(
                        status_endpoint,
                        headers={"Authorization": f"Bearer {token}"},
                        method="GET"
                    )
                    with urllib.request.urlopen(status_req, timeout=3.0) as status_res:
                        res_data = json.loads(status_res.read().decode("utf-8"))
                        if res_data.get("billing_active"):
                            unlocked = True
                            print("[Success] Payment Detected. Server unlocked.")
                except Exception:
                    pass
        else:
            print("\n[Success] Live production payments checkout portal spawned.")

def handle_status(args):
    token = load_credentials()
    if not token:
        print("Error: No authentication token found. Please run 'linac auth set-key <API_KEY>' first.")
        sys.exit(1)
        
    url = "http://127.0.0.1:8787/v1/pulse"
    payload = {"packets": 0, "bytes": 0}
    data_bytes = json.dumps(payload).encode("utf-8")
    
    req = urllib.request.Request(
        url,
        data=data_bytes,
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json"
        },
        method="POST"
    )
    
    try:
        with open(CREDENTIALS_FILE, "r") as f:
            cred_data = json.load(f)
    except Exception:
        cred_data = {}
        
    is_beta_unlocked = cred_data.get("status") == "beta_unlocked"
    
    def mask_token(t):
        if not t:
            return "None"
        if len(t) <= 8:
            return "***"
        return f"{t[:12]}***{t[-3:]}" if len(t) > 15 else f"{t[:3]}***{t[-3:]}"
        
    masked_key = mask_token(token)
    print(f"API Key: {masked_key}")
    
    try:
        with urllib.request.urlopen(req, timeout=3.0) as res:
            res_body = json.loads(res.read().decode("utf-8"))
            ledger = res_body.get("ledger", {})
            packets = ledger.get("packets_accumulated", 0)
            if is_beta_unlocked:
                print(f"Usage: {packets:,} / 50,000,000 free beta packets consumed this cycle (Beta Unlocked)")
            else:
                print(f"Usage: {packets:,} / 100,000 free packets consumed this cycle")
    except urllib.error.HTTPError as he:
        if he.code == 402:
            try:
                err_data = json.loads(he.read().decode("utf-8"))
                msg = err_data.get("message")
                print(f"\n[Payment Required] {msg}")
            except Exception:
                print("\n[Payment Required] Free baseline of 100,000 packets exceeded. Please run 'linac billing setup' to bind a credit card.")
        else:
            print(f"Error checking status: Local Wrangler dev server responded with HTTP {he.code} - {he.reason}")
    except Exception as e:
        print(f"Error checking status: Local Wrangler dev server is not active on http://127.0.0.1:8787")

def stream_batch_worker(batch, target_endpoint, token, compress):
    """Asynchronous worker to compress and stream batches of raw telemetry data."""
    try:
        payload = {
            "packets": len(batch),
            "bytes": sum(len(line) for line in batch)
        }
        payload_bytes = json.dumps(payload).encode("utf-8")
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json"
        }
        
        if compress:
            payload_bytes = gzip.compress(payload_bytes)
            headers["Content-Encoding"] = "gzip"
            
        req = urllib.request.Request(
            target_endpoint,
            data=payload_bytes,
            headers=headers,
            method="POST"
        )
        with urllib.request.urlopen(req, timeout=5.0) as res:
            res.read()
        return True, None, None
    except urllib.error.HTTPError as he:
        try:
            err_data = json.loads(he.read().decode("utf-8"))
            err_msg = err_data.get("message", he.reason)
        except Exception:
            err_msg = he.reason
        return False, he.code, err_msg
    except Exception as e:
        return False, 500, str(e)

def handle_stream(args):
    token = load_credentials()
    if not token:
        print("Error: No authentication token found. Please run 'linac auth set-key <API_KEY>' first.")
        sys.exit(1)
        
    local_cfg = load_local_config()
    
    file_path = args.file
    target = args.target or local_cfg.get("default_endpoint") or "http://127.0.0.1:8787/v1/pulse"
    batch_size = local_cfg.get("batch_size", 1000)
    workers = local_cfg.get("thread_workers", 4)
    compress = local_cfg.get("compression_enabled", True)
    
    if not os.path.exists(file_path):
        print(f"Error: File not found at path: {file_path}")
        sys.exit(1)
        
    print(f"Streaming dataset '{file_path}' (batch={batch_size}, threads={workers}, gzip={compress}) to '{target}'...")
    
    batches = []
    current_batch = []
    
    with open(file_path, "r", encoding="utf-8") as f:
        for line in f:
            stripped = line.strip()
            if stripped:
                current_batch.append(stripped)
                if len(current_batch) >= batch_size:
                    batches.append(current_batch)
                    current_batch = []
        if current_batch:
            batches.append(current_batch)
            
    total_packets = sum(len(b) for b in batches)
    
    start_time = time.time()
    # Multi-threaded concurrent transmission via standard ThreadPoolExecutor
    failures_402 = []
    other_failures = []
    successful_packets = 0

    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {
            executor.submit(stream_batch_worker, b, target, token, compress): len(b)
            for b in batches
        }
        
        for future in concurrent.futures.as_completed(futures):
            batch_len = futures[future]
            try:
                success, code, msg = future.result()
                if success:
                    successful_packets += batch_len
                else:
                    if code == 402:
                        failures_402.append(msg)
                    else:
                        other_failures.append((code, msg))
            except Exception as exc:
                other_failures.append((500, str(exc)))
        
    duration = time.time() - start_time

    if failures_402:
        print("\n" + "="*80)
        print(" [402 PAYMENT REQUIRED] BLOCK ENFORCED BY SWITCHBOARD INGRESS EDGE")
        print(failures_402[0])
        print("="*80 + "\n")
        sys.exit(402)
    elif other_failures:
        print(f"\n[Error] Telematics stream interrupted by errors. Successfully sent {successful_packets:,} packets.")
        print(f"Sample error: HTTP {other_failures[0][0]} - {other_failures[0][1]}")
        sys.exit(1)
    else:
        print(f"\n[Success] Telematics stream finalized. Streamed a total of {total_packets:,} packets in {duration:.2f} seconds.")

SPOOL_FILE = ".linac_spool.bin"
IS_ONLINE = True

async def test_edge_connection(endpoint, token):
    """Checks if the Cloudflare Edge Worker endpoint is active and reachable using non-blocking TCP sockets."""
    try:
        parsed = urllib.parse.urlparse(endpoint)
        host = parsed.hostname or "127.0.0.1"
        port = parsed.port or (80 if parsed.scheme == "http" else 443)
        
        # Non-blocking TCP connection check with 1.0s timeout
        _, writer = await asyncio.wait_for(
            asyncio.open_connection(host, port),
            timeout=1.0
        )
        writer.close()
        await writer.wait_closed()
        return True
    except Exception:
        return False

async def connection_monitor(endpoint, token):
    """Periodically checks if the edge worker is online and updates global status."""
    global IS_ONLINE
    while True:
        IS_ONLINE = await test_edge_connection(endpoint, token)
        await asyncio.sleep(2.0)

def sync_spool_to_disk(body):
    """Synchronous file append execution to run in worker thread."""
    try:
        encoded = body.encode("utf-8")
        length = len(encoded)
        with open(SPOOL_FILE, "ab") as f:
            f.write(length.to_bytes(4, byteorder="big") + encoded)
    except Exception as e:
        print(f"[Switchboard Spooler] Error: Failed to write to local spool: {e}")

async def spool_to_disk(body):
    """Appends a telemetry payload safely to the binary cache file using length-prefixing in a worker thread."""
    await asyncio.to_thread(sync_spool_to_disk, body)

async def proxy_sync_worker(spool_queue, sdk_client, forward, token):
    """Asynchronously syncs enqueued payloads. Spools to disk if connection is down."""
    global IS_ONLINE
    while True:
        body = await spool_queue.get()
        
        if IS_ONLINE:
            try:
                sdk_client.process(body)
            except Exception:
                await spool_to_disk(body)
        else:
            await spool_to_disk(body)
            
        spool_queue.task_done()

async def proxy_drain_worker(sdk_client, forward, token):
    """Monitors the local spool file and drains cached payloads once connection is re-established."""
    global IS_ONLINE
    while True:
        await asyncio.sleep(3.0)
        
        if not os.path.exists(SPOOL_FILE):
            continue
            
        if not IS_ONLINE:
            continue
            
        print("[Switchboard Spooler] Connection detected. Draining spooled logs...")
        
        def read_spool():
            items = []
            with open(SPOOL_FILE, "rb") as f:
                while True:
                    length_bytes = f.read(4)
                    if not length_bytes:
                        break
                    length = int.from_bytes(length_bytes, byteorder="big")
                    body_bytes = f.read(length)
                    items.append(body_bytes.decode("utf-8"))
            return items
            
        try:
            payloads = await asyncio.to_thread(read_spool)
            
            # Process payloads through standard SDK client
            for body in payloads:
                sdk_client.process(body)
                
            os.remove(SPOOL_FILE)
            print(f"[Switchboard Spooler] Successfully drained {len(payloads)} spooled packets to edge ledger.")
        except Exception as e:
            print(f"[Switchboard Spooler] Error during drain cycle: {e}")

async def handle_proxy_client(reader, writer, spool_queue, sdk_client):
    try:
        content_length = 0
        while True:
            line_bytes = await reader.readline()
            if not line_bytes or line_bytes in (b'\r\n', b'\n'):
                break
            line = line_bytes.decode("utf-8")
            if line.lower().startswith("content-length:"):
                try:
                    content_length = int(line.split(":", 1)[1].strip())
                except Exception:
                    pass
        
        body = ""
        if content_length > 0:
            body_bytes = await reader.readexactly(content_length)
            body = body_bytes.decode("utf-8")
            
        # Fast local flattening to return flat payload in HTTP response
        flat_data = sdk_client.engine.normalize(body)
        
        # Enqueue raw payload for asynchronous network streaming / disk spooling
        await spool_queue.put(body)
        
        response_body = json.dumps({
            "success": True, 
            "message": "Piped telemetry successfully to global edge",
            "flattened": flat_data
        }).encode("utf-8")
        
        headers = (
            f"HTTP/1.1 200 OK\r\n"
            f"Content-Type: application/json\r\n"
            f"Content-Length: {len(response_body)}\r\n"
            f"Connection: close\r\n\r\n"
        ).encode("utf-8")
        response = headers + response_body
        writer.write(response)
        await writer.drain()
    except Exception as e:
        err_body = json.dumps({"success": False, "error": str(e)}).encode("utf-8")
        headers = (
            f"HTTP/1.1 500 Internal Server Error\r\n"
            f"Content-Type: application/json\r\n"
            f"Content-Length: {len(err_body)}\r\n"
            f"Connection: close\r\n\r\n"
        ).encode("utf-8")
        response = headers + err_body
        try:
            writer.write(response)
            await writer.drain()
        except Exception:
            pass
    finally:
        writer.close()
        try:
            await writer.wait_closed()
        except Exception:
            pass

async def listen_main(port, forward, sdk_client, token):
    # Initialize the local in-memory asyncio Queue
    spool_queue = asyncio.Queue()
    
    # Spawn background async task processes for connection monitor, sync and drain pipelines
    asyncio.create_task(connection_monitor(forward, token))
    asyncio.create_task(proxy_sync_worker(spool_queue, sdk_client, forward, token))
    asyncio.create_task(proxy_drain_worker(sdk_client, forward, token))
    
    server = await asyncio.start_server(
        lambda r, w: handle_proxy_client(r, w, spool_queue, sdk_client),
        '127.0.0.1',
        port,
        backlog=2048
    )
    print(f"linac proxy gateway active. Listening on http://127.0.0.1:{port}...")
    print(f"Forwarding events dynamically to global edge: {forward}")
    async with server:
        await server.serve_forever()

def handle_listen(args):
    """Turns the SDK into a local proxy gateway listening on a port."""
    token = load_credentials()
    if not token:
        print("Error: No authentication token found. Please run 'linac auth set-key <API_KEY>' first.")
        sys.exit(1)
        
    port = args.port
    forward = args.forward
    
    sdk_client = Ingress(api_key=token, endpoint=forward)
    
    try:
        asyncio.run(listen_main(port, forward, sdk_client, token))
    except KeyboardInterrupt:
        print("\nlinac proxy gateway stopped.")

def handle_validate(args):
    """Runs a local diagnostic check on a dataset before spending data quotas."""
    file_path = args.file
    if not os.path.exists(file_path):
        print(f"Error: File not found at path: {file_path}")
        sys.exit(1)
        
    print(f"Running local diagnostic check on dataset: {file_path}...")
    engine = IngressEngine()
    
    errors = 0
    lines_checked = 0
    
    with open(file_path, "r", encoding="utf-8") as f:
        for line_num, line in enumerate(f, 1):
            stripped = line.strip()
            if stripped:
                lines_checked += 1
                try:
                    res = engine.normalize(stripped)
                    if "raw_payload" in res:
                        print(f"Warning on Line {line_num}: Unrecognized structure format - '{stripped[:60]}...'")
                        errors += 1
                except Exception as err:
                    print(f"Error on Line {line_num}: {err}")
                    errors += 1
                    
    if errors == 0:
        print(f"\n[Success] Validation complete. Checked {lines_checked:,} lines. Zero errors.")
    else:
        print(f"\n[Warning] Validation complete. Checked {lines_checked:,} lines. Found {errors:,} syntax warnings.")

def main():
    parser = argparse.ArgumentParser(description="linac Command Line Interface (CLI)")
    subparsers = parser.add_subparsers(dest="command", required=True)
    
    # Subcommand: init
    subparsers.add_parser("init", help="Initialize local project folder configuration (.linacjson)")
    
    # Subcommand: auth
    auth_parser = subparsers.add_parser("auth", help="Configure developer authentication credentials")
    auth_subparsers = auth_parser.add_subparsers(dest="subcommand", required=True)
    
    # Subcommand: auth set-key
    set_key_parser = auth_subparsers.add_parser("set-key", help="Surgically set active API key credential")
    set_key_parser.add_argument("api_key", help="Active developer Ingress API key token")
    
    # Subcommand: billing setup
    bill_parser = subparsers.add_parser("billing", help="Configure usage-based billing details")
    bill_subparsers = bill_parser.add_subparsers(dest="subcommand", required=True)
    bill_subparsers.add_parser("setup", help="Bind credit card / payment method in sandbox")
    
    # Subcommand: status
    subparsers.add_parser("status", help="Inspect real-time telemetry usage metrics")
    
    # Subcommand: stream
    stream_parser = subparsers.add_parser("stream", help="Stream telematics datasets continuously")
    stream_parser.add_argument("--file", required=True, help="Local dataset file path")
    stream_parser.add_argument("--target", help="Target Cloudflare Worker endpoint override")
    
    # Subcommand: listen
    listen_parser = subparsers.add_parser("listen", help="Turn SDK into local proxy gateway forwarding to edge")
    listen_parser.add_argument("--port", type=int, default=3000, help="Local port to bind server to")
    listen_parser.add_argument("--forward", default="http://127.0.0.1:8787/v1/pulse", help="Target global ingress engine")
    
    # Subcommand: validate
    val_parser = subparsers.add_parser("validate", help="Execute local parser diagnostics on a file")
    val_parser.add_argument("--file", required=True, help="Local file to analyze")
    
    args = parser.parse_args()
    
    if args.command == "init":
        handle_init(args)
    elif args.command == "auth":
        if args.subcommand == "set-key":
            handle_auth_set_key(args)
    elif args.command == "billing":
        if args.subcommand == "setup":
            handle_billing_setup(args)
    elif args.command == "status":
        handle_status(args)
    elif args.command == "stream":
        handle_stream(args)
    elif args.command == "listen":
        handle_listen(args)
    elif args.command == "validate":
        handle_validate(args)

if __name__ == "__main__":
    main()

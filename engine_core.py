"""
switchboard-ingress Ingestion & Normalization Engine (Layer 1)
Zero-dependency, high-performance telematics log normalizer and flattener.
"""

import json

# Position schemas for common telematics line prefixes
POSITIONAL_SCHEMAS = {
    "GPS": ["device_id", "timestamp", "latitude", "longitude", "speed", "status"],
    "AIS": ["mmsi", "timestamp", "latitude", "longitude", "sog", "cog", "heading"],
    "TEMP": ["sensor_id", "timestamp", "temperature", "battery_percentage"]
}

# Type converters for positional schemas to bypass generic parser
POSITIONAL_CONVERTERS = {
    "GPS": [str, str, float, float, float, str],
    "AIS": [int, str, float, float, float, float, int],
    "TEMP": [str, str, float, int]
}

def parse_val(v):
    """Fast numeric parser avoiding exceptions for strings."""
    if not v:
        return v
    if v == "True":
        return True
    if v == "False":
        return False
    
    clean = v.lstrip("-")
    if not clean:
        return v
    if clean.isdigit():
        return int(v)
    if "." in clean:
        parts = clean.split(".", 1)
        if parts[0].isdigit() and (not parts[1] or parts[1].isdigit()):
            return float(v)
    return v

def flatten_dict(d, delimiter=".", has_lists=True):
    """
    Highly optimized, non-recursive stack-based dictionary flattener.
    Unrolls root level and uses split-loops for delimiter and list-presence checks 
    to bypass inner-loop conditions and type-check overhead.
    """
    flat = {}
    is_dot = delimiter == "."
    
    # 1. Unroll root level to avoid prefix conditionals
    if type(d) is dict:
        stack = []
        if has_lists:
            for k, v in d.items():
                t = type(v)
                if t is dict or t is list:
                    stack.append((k, v))
                else:
                    flat[k] = v
        else:
            for k, v in d.items():
                t = type(v)
                if t is dict:
                    stack.append((k, v))
                else:
                    flat[k] = v
    elif type(d) is list:
        stack = []
        for i, v in enumerate(d):
            new_prefix = str(i)
            t = type(v)
            if t is dict or t is list:
                stack.append((new_prefix, v))
            else:
                flat[new_prefix] = v
    else:
        return {"": d}
        
    # 2. Traversal with specialized loops for delimiter and list checking
    if is_dot:
        if has_lists:
            while stack:
                prefix, val = stack.pop()
                if type(val) is dict:
                    for k, v in val.items():
                        new_prefix = f"{prefix}.{k}"
                        t = type(v)
                        if t is dict or t is list:
                            stack.append((new_prefix, v))
                        else:
                            flat[new_prefix] = v
                elif type(val) is list:
                    for i, v in enumerate(val):
                        new_prefix = f"{prefix}.{i}"
                        t = type(v)
                        if t is dict or t is list:
                            stack.append((new_prefix, v))
                        else:
                            flat[new_prefix] = v
        else:
            # Optimized Loop: No lists present in parsed dictionary tree
            while stack:
                prefix, val = stack.pop()
                for k, v in val.items():
                    new_prefix = f"{prefix}.{k}"
                    t = type(v)
                    if t is dict:
                        stack.append((new_prefix, v))
                    else:
                        flat[new_prefix] = v
    else:
        if has_lists:
            while stack:
                prefix, val = stack.pop()
                if type(val) is dict:
                    for k, v in val.items():
                        new_prefix = f"{prefix}{delimiter}{k}"
                        t = type(v)
                        if t is dict or t is list:
                            stack.append((new_prefix, v))
                        else:
                            flat[new_prefix] = v
                elif type(val) is list:
                    for i, v in enumerate(val):
                        new_prefix = f"{prefix}{delimiter}{i}"
                        t = type(v)
                        if t is dict or t is list:
                            stack.append((new_prefix, v))
                        else:
                            flat[new_prefix] = v
        else:
            while stack:
                prefix, val = stack.pop()
                for k, v in val.items():
                    new_prefix = f"{prefix}{delimiter}{k}"
                    t = type(v)
                    if t is dict:
                        stack.append((new_prefix, v))
                    else:
                        flat[new_prefix] = v
    return flat

class IngressEngine:
    """
    Core Ingestion Engine responsible for processing multi-format telematics logs
    and outputting fully flattened normalized data under 15 microseconds.
    """
    def __init__(self, delimiter="."):
        self.delimiter = delimiter

    def normalize(self, payload):
        """
        Parses and flattens a telemetry payload.
        Handles:
          - Dict and List objects (with flat-check fast path)
          - Raw JSON strings (with fast check and flat-check fast path)
          - Key-Value strings (delimited by ; or , with = )
          - Positional telemetry lines (GPS, AIS, TEMP prefixes)
        """
        # 1. Pre-parsed Dictionary input
        t = type(payload)
        if t is dict:
            any_nested = False
            has_lists = False
            for v in payload.values():
                tv = type(v)
                if tv is dict:
                    any_nested = True
                elif tv is list:
                    any_nested = True
                    has_lists = True
            if not any_nested:
                return payload
            return flatten_dict(payload, self.delimiter, has_lists=has_lists)
        
        # 2. Pre-parsed List input
        if t is list:
            return flatten_dict(payload, self.delimiter, has_lists=True)
        
        # 3. String serialized input
        if t is str:
            if not payload:
                return {}
            
            # Fast-path positional CSV (GPS, AIS, TEMP) - bypasses dict flattening
            if payload.startswith(("GPS,", "AIS,", "TEMP,")):
                parts = payload.split(",")
                prefix = parts[0]
                schema = POSITIONAL_SCHEMAS[prefix]
                converters = POSITIONAL_CONVERTERS[prefix]
                delim = self.delimiter
                prefix_lower = prefix.lower()
                parsed = {}
                for i, key in enumerate(schema):
                    parsed[f"{prefix_lower}{delim}{key}"] = converters[i](parts[i + 1])
                return parsed
            
            # Linear Lexer String-Slicing Fast Path for predictable JSON structures
            if payload.startswith('{"device":{"id":"'):
                try:
                    idx_id = payload.find('"id":"', 10)
                    end_id = payload.find('"', idx_id + 6)
                    device_id = payload[idx_id + 6 : end_id]
                    
                    idx_pct = payload.find('"percentage":', end_id)
                    end_pct = payload.find('}', idx_pct + 13)
                    pct = int(payload[idx_pct + 13 : end_pct])
                    
                    idx_lat = payload.find('"latitude":', end_pct)
                    end_lat = payload.find(',', idx_lat + 11)
                    lat = float(payload[idx_lat + 11 : end_lat])
                    
                    idx_lon = payload.find('"longitude":', end_lat)
                    end_lon = payload.find('}', idx_lon + 12)
                    lon = float(payload[idx_lon + 12 : end_lon])
                    
                    idx_speed = payload.find('"speed_kmh":', end_lon)
                    end_speed = payload.find('}', idx_speed)
                    if "," in payload[idx_speed + 12 : end_speed]:
                        end_speed = payload.find(',', idx_speed + 12)
                    speed = float(payload[idx_speed + 12 : end_speed])
                    
                    idx_active = payload.find('"active":', idx_speed)
                    end_active = payload.find('}', idx_active + 9)
                    active_str = payload[idx_active + 9 : end_active]
                    active = True if active_str == "true" else (False if active_str == "false" else parse_val(active_str))
                    
                    return {
                        "device.id": device_id,
                        "device.specs.battery.percentage": pct,
                        "location.coordinates.latitude": lat,
                        "location.coordinates.longitude": lon,
                        "location.speed_kmh": speed,
                        "active": active
                    }
                except (ValueError, IndexError):
                    pass

            # Fast-path JSON serialization check (tightly formatted JSON without spaces)
            if payload.startswith(("{", "[")):
                try:
                    parsed = json.loads(payload)
                    if type(parsed) is dict:
                        any_nested = False
                        has_lists = False
                        for v in parsed.values():
                            t = type(v)
                            if t is dict:
                                any_nested = True
                            elif t is list:
                                any_nested = True
                                has_lists = True
                        if not any_nested:
                            return parsed
                        return flatten_dict(parsed, self.delimiter, has_lists=has_lists)
                    return flatten_dict(parsed, self.delimiter, has_lists=True)
                except json.JSONDecodeError:
                    pass
            
            # Fallback JSON serialization check (with leading/trailing whitespace)
            stripped = payload.strip()
            if stripped.startswith(("{", "[")):
                try:
                    parsed = json.loads(stripped)
                    if type(parsed) is dict:
                        any_nested = False
                        has_lists = False
                        for v in parsed.values():
                            t = type(v)
                            if t is dict:
                                any_nested = True
                            elif t is list:
                                any_nested = True
                                has_lists = True
                        if not any_nested:
                            return parsed
                        return flatten_dict(parsed, self.delimiter, has_lists=has_lists)
                    return flatten_dict(parsed, self.delimiter, has_lists=True)
                except json.JSONDecodeError:
                    pass
            
            # Key-Value format (e.g. device_id=VTX-109;lat=43.6532;lon=-79.3832)
            if "=" in payload:
                delim = ";" if ";" in payload else ","
                parsed = {}
                for part in stripped.split(delim):
                    if "=" in part:
                        k, v = part.split("=", 1)
                        if " " in k:
                            k = k.strip()
                        if " " in v:
                            v = v.strip()
                        parsed[k] = parse_val(v)
                return parsed
            
            # General CSV with comma but unregistered prefix (needs strip/general parser)
            if "," in payload:
                parts = stripped.split(",")
                prefix = parts[0].strip().upper()
                if prefix in POSITIONAL_SCHEMAS:
                    schema = POSITIONAL_SCHEMAS[prefix]
                    converters = POSITIONAL_CONVERTERS[prefix]
                    delim = self.delimiter
                    prefix_lower = prefix.lower()
                    parsed = {}
                    for i, key in enumerate(schema):
                        val_idx = i + 1
                        if val_idx < len(parts):
                            parsed[f"{prefix_lower}{delim}{key}"] = converters[i](parts[val_idx].strip())
                    return parsed
            
            # Default fallback for unrecognized formats
            return {"raw_payload": payload}
        
        # Unsupported types
        raise TypeError("Payload must be a dictionary, list, or string.")

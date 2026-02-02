import json, os

CONFIG_FILE = "config.json"

def ensure_json_file(path, default_obj):
    if not os.path.exists(path):
        with open(path, "w", encoding="utf-8") as f:
            json.dump(default_obj, f, indent=4)

def load_config():
    default_cfg = {
        "token": "",
        "admin_server_id": 0,
        "verification_channel_id": 0,
    }
    ensure_json_file(CONFIG_FILE, default_cfg)
    with open(CONFIG_FILE, "r", encoding="utf-8") as f:
        cfg = json.load(f) or {}

    changed = False
    for k, v in default_cfg.items():
        if k not in cfg:
            cfg[k] = v
            changed = True
    if changed:
        with open(CONFIG_FILE, "w", encoding="utf-8") as f:
            json.dump(cfg, f, indent=4)

    return cfg


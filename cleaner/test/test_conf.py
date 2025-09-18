from datetime import datetime, timedelta, timezone

NOW = datetime(2025, 1, 1, tzinfo=timezone.utc)

def make_component(name, version, days_old=10, last_download_days=None):
    last_modified = (NOW - timedelta(days=days_old)).isoformat()
    assets = [{"lastModified": last_modified}]
    if last_download_days is not None:
        last_downloaded = (NOW - timedelta(days=last_download_days)).isoformat()
        assets[0]["lastDownloaded"] = last_downloaded

    return {
        "id": f"{name}-{version}",
        "name": name,
        "version": version,
        "assets": assets,
    }

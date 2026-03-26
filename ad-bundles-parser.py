import streamlit as st
import requests
import tarfile
import yaml
import re
import pandas as pd
import json
import os
import io
from concurrent.futures import ThreadPoolExecutor, as_completed
from yaml import CSafeLoader as Loader

CACHE_FILE = "cache.json"

MAX_VERSIONS_PER_PRODUCT = 100
MAX_WORKERS = 4

PRODUCT_URLS = {
    "ET": "https://downloads.adsw.io/adcm_bundles/adcm_cluster_et/release/",
    "Monitoring": "https://downloads.adsw.io/adcm_bundles/adcm_cluster_monitoring/release/",
    "ADB": "https://downloads.adsw.io/adcm_bundles/adcm_cluster_adb/release/",
    "ADQM": "https://downloads.adsw.io/adcm_bundles/adcm_cluster_adqm/release/",
    "ADH": "https://downloads.adsw.io/adcm_bundles/adcm_cluster_hadoop/release/",
    "ADS": "https://downloads.adsw.io/adcm_bundles/adcm_cluster_ads/release/",
    "ADPG": "https://downloads.adsw.io/adcm_bundles/adcm_cluster_adpg/release/",
}

ENTERPRISE_PRODUCTS = ["ADB", "ADQM", "ADH", "ADS", "ADPG"]

http_session = requests.Session()

VERSION_REGEX = re.compile(r"\d+(?:\.\d+)+")

# ---------------- CACHE ----------------

def load_cache():
    if os.path.exists(CACHE_FILE):
        with open(CACHE_FILE) as f:
            return json.load(f)
    return {product: {} for product in PRODUCT_URLS}


def save_cache(cache_data):
    with open(CACHE_FILE, "w") as f:
        json.dump(cache_data, f, indent=4)

# ---------------- TAR ----------------

def open_tar_from_url(url):
    response = http_session.get(url, timeout=60)
    response.raise_for_status()
    return tarfile.open(fileobj=io.BytesIO(response.content), mode="r:gz")

# ---------------- VERSION ----------------

def clean_version(version_value):
    if not version_value:
        return None
    match = VERSION_REGEX.search(str(version_value))
    return match.group(0) if match else None


def extract_version(product_name, bundle_name):

    if product_name == "ADB":
        try:
            base_version = bundle_name.split("_v")[1].split("_")[0]

            match = re.search(r"_arenadata(\d+)", bundle_name)
            if match:
                return f"{base_version}.{match.group(1)}"

            return base_version
        except Exception:
            return None

    version_patterns = {
        "ET": r"_v(\d{10})",
        "Monitoring": r"_v(\d+\.\d+\.\d+)",
        "ADQM": r"_v(\d+\.\d+\.\d+\.\d+)_arenadata(\d+)",
        "ADPG": r"_v(\d+\.\d+)_arenadata(\d+)",
        "ADS": r"_v(\d+\.\d+\.\d+)_arenadata(\d+)",
    }

    pattern = version_patterns.get(product_name, r"_v(\d+\.\d+(?:\.\d+)+)")
    match = re.search(pattern, bundle_name)

    if not match:
        return None

    return ".".join(match.groups())

# ---------------- BUNDLES ----------------

def get_bundle_urls(base_url, product_name):
    response = http_session.get(base_url, timeout=30)

    links = re.findall(r'href="([^"]+\.tgz[^"]*)"', response.text)

    bundle_urls = [
        (base_url + href.replace("./", "")) if not href.startswith("http") else href
        for href in links
    ]

    if product_name in ENTERPRISE_PRODUCTS:
        bundle_urls = [b for b in bundle_urls if "enterprise" in b]

    return bundle_urls


def build_version_map(product_name, bundle_urls):
    version_to_bundle = {}

    for bundle_url in bundle_urls:
        bundle_name = bundle_url.split("/")[-1]
        version = extract_version(product_name, bundle_name)

        if not version:
            continue

        if version not in version_to_bundle or "enterprise" in bundle_url:
            version_to_bundle[version] = bundle_url

    return dict(
        sorted(
            version_to_bundle.items(),
            key=lambda x: [int(i) for i in x[0].split(".")],
            reverse=True,
        )[:MAX_VERSIONS_PER_PRODUCT]
    )

# ---------------- PARSERS ----------------

def parse_et_monitoring(bundle_url):
    result = {"grafana": None, "graphite": None}

    with open_tar_from_url(bundle_url) as tar:
        for member in tar:
            if member.issym() or member.islnk():
                continue

            if "packs" not in member.name or not member.name.endswith(".yaml"):
                continue

            file_obj = tar.extractfile(member)
            if not file_obj:
                continue

            data = yaml.load(file_obj.read(), Loader=Loader)

            for image in data.get("images", []):
                if "grafana" in image:
                    result["grafana"] = image.split(":")[1].split("-")[0]
                if "graphite" in image:
                    result["graphite"] = image.split(":")[1].split("_")[0]

            break

    return result


def parse_prometheus_bundle(bundle_url):
    result = {
        "prometheus": None,
        "pushgateway": None,
        "grafana": None,
        "node_exporter": None,
    }

    prototype_text = None
    wanted_packages = None

    with open_tar_from_url(bundle_url) as tar:
        for member in tar:

            if member.issym() or member.islnk():
                continue

            if member.name.endswith((".yaml", ".yml")):
                file_obj = tar.extractfile(member)
                if not file_obj:
                    continue

                try:
                    data = yaml.load(file_obj.read(), Loader=Loader)
                except Exception:
                    continue

                if isinstance(data, dict):

                    if "admprom_prometheus_version" in data:
                        result["prometheus"] = clean_version(
                            data.get("admprom_prometheus_version")
                        )

                    if "admprom_pushgateway_version" in data:
                        result["pushgateway"] = clean_version(
                            data.get("admprom_pushgateway_version")
                        )

                    if "admprom_grafana_version" in data:
                        result["grafana"] = clean_version(
                            data.get("admprom_grafana_version")
                        )

                    if "admprom_node_exporter_version" in data:
                        result["node_exporter"] = clean_version(
                            data.get("admprom_node_exporter_version")
                        )

            if "prototype.yaml.j2" in member.name:
                file_obj = tar.extractfile(member)
                if file_obj:
                    prototype_text = file_obj.read().decode()

            if "wanted_packages.yaml" in member.name:
                file_obj = tar.extractfile(member)
                if file_obj:
                    try:
                        wanted_packages = yaml.load(file_obj.read(), Loader=Loader)
                    except Exception:
                        pass

    if prototype_text:
        patterns = {
            "prometheus": r"prometheus:.*?default: '([^']+)'",
            "grafana": r"grafana:.*?default: '([^']+)'",
            "pushgateway": r"pushgateway:.*?default: '([^']+)'",
            "node_exporter": r"node_exporter:.*?default: '([^']+)'",
        }

        for key, pattern in patterns.items():
            match = re.search(pattern, prototype_text, re.S)
            if match and not result[key]:
                result[key] = match.group(1)

    def walk_yaml(node):
        if isinstance(node, list):
            for pkg in node:
                if not isinstance(pkg, dict):
                    continue

                name = (pkg.get("name") or "").lower()
                version = pkg.get("version")

                if "prometheus" in name and not result["prometheus"]:
                    result["prometheus"] = version
                elif "grafana" in name and not result["grafana"]:
                    result["grafana"] = version
                elif "node" in name and "exporter" in name:
                    result["node_exporter"] = version
                elif "pushgateway" in name and not result["pushgateway"]:
                    result["pushgateway"] = version

        elif isinstance(node, dict):
            for value in node.values():
                walk_yaml(value)

    if wanted_packages:
        walk_yaml(wanted_packages)

    return result

# ---------------- ADCM ----------------

def parse_adb_components(bundle_url):
    result = {
        "adcc": None,
        "adbm": None,
        "gpbackup": None,
        "pxf": None,
        "tkh_connector": None,
        "kafka_connector": None,
    }

    COMPONENT_MAP = {
        "ADCC_AGENT": "adcc",
        "ADBM_AGENT": "adbm",
        "GPBACKUP": "gpbackup",
        "PXF": "pxf",
        "TKH_CONNECTOR": "tkh_connector",
        "KAFKA_CONNECTOR": "kafka_connector",
    }

    try:
        with open_tar_from_url(bundle_url) as tar:
            for member in tar:

                if member.issym() or member.islnk():
                    continue

                path = member.name.lower()

                if not re.search(r"adb/group_vars/cluster/[^/]+\.ya?ml$", path):
                    continue

                if not path.endswith((".yaml", ".yml")):
                    continue

                file_obj = tar.extractfile(member)
                if not file_obj:
                    continue

                try:
                    data = yaml.load(file_obj.read(), Loader=Loader)
                except Exception:
                    continue

                if not isinstance(data, dict):
                    continue

                for component_key, result_key in COMPONENT_MAP.items():
                    if component_key in data:
                        block = data[component_key]

                        if not isinstance(block, dict):
                            continue

                        version = None

                        priority_keys = [
                            "REDHAT_X86_64_VERSION",
                            "RED_X86_64_VERSION",
                            "DEBIAN_X86_64_VERSION",
                            "ASTRALINUX_X86_64_VERSION",
                            "ALTLINUX_X86_64_VERSION",
                        ]

                        for pk in priority_keys:
                            if pk in block and isinstance(block[pk], str):
                                version = block[pk]
                                break

                        if not version:
                            for key, value in block.items():
                                if "VERSION" in key and isinstance(value, str):
                                    version = value
                                    break

                        if not result[result_key] and version:
                            version_str = str(version)

                            base_version = clean_version(version_str)

                            match = re.search(r"arenadata(\d+)", version_str)
                            if match:
                                result[result_key] = f"{base_version}_arenadata{match.group(1)}"
                            else:
                                result[result_key] = base_version

        return result

    except Exception as e:
        print("ADB parse error:", bundle_url, e)
        return result


def parse_adcm_min_version(bundle_url):
    try:
        with open_tar_from_url(bundle_url) as tar:
            for member in tar:

                if member.issym() or member.islnk():
                    continue

                name = member.name.lower()

                if not (name.endswith("config.yaml") or name.endswith("config.yml")):
                    continue

                file_obj = tar.extractfile(member)
                if not file_obj:
                    continue

                raw = file_obj.read()

                try:
                    data = yaml.load(raw, Loader=Loader)
                    if isinstance(data, dict):
                        version = data.get("adcm_min_version")
                        if version:
                            return clean_version(version)
                except Exception:
                    pass

                text = raw.decode(errors="ignore")
                match = re.search(r"adcm_min_version:\s*([0-9\.]+)", text)
                if match:
                    return clean_version(match.group(1))

    except Exception as e:
        print("ADCM error:", bundle_url, e)

    return None

# ---------------- PROCESS ----------------

def process_bundle(product_name, version, bundle_url):
    try:
        if product_name in ["ET", "Monitoring"]:
            data = parse_et_monitoring(bundle_url) or {}

        elif product_name == "ADB":
            data = {}
            data.update(parse_prometheus_bundle(bundle_url) or {})
            data.update(parse_adb_components(bundle_url) or {})

        else:
            data = parse_prometheus_bundle(bundle_url) or {}

        data["adcm_min_version"] = parse_adcm_min_version(bundle_url)

        return product_name, version, data

    except Exception as e:
        print("parse error", bundle_url, e)
        return product_name, version, None

# ---------------- CACHE UPDATE ----------------

def update_cache():
    cache_data = load_cache()
    progress_bar = st.progress(0)

    tasks = [
        (product, version, bundle_url)
        for product, base_url in PRODUCT_URLS.items()
        for version, bundle_url in build_version_map(
            product, get_bundle_urls(base_url, product)
        ).items()
        if version not in cache_data[product]
        or not all(cache_data[product][version].values())
    ]

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [executor.submit(process_bundle, *task) for task in tasks]

        for i, future in enumerate(as_completed(futures)):
            product, version, data = future.result()

            if data and any(data.values()):
                cache_data[product][version] = data

            if tasks:
                progress_bar.progress((i + 1) / len(tasks))

    save_cache(cache_data)
    return cache_data

# ---------------- STREAMLIT ----------------

st.title("Versions of Arenadata bundles components")

cache_data = load_cache()

if not os.path.exists(CACHE_FILE) or not any(cache_data.values()):
    st.info("Cache not found. Building cache...")
    cache_data = update_cache()
    st.success("Cache built")

if st.button("Refresh cache"):
    cache_data = update_cache()
    st.success("Cache updated")

tab1, tab2, tab3 = st.tabs(
    ["ADCM Minimal Version", "Monitoring Versions", "ADB Components"]
)

with tab2:
    st.header("Monitoring Versions")

    ADB_COMPONENTS = [
        "adcc",
        "adbm",
        "gpbackup",
        "pxf",
        "tkh_connector",
        "kafka_connector",
    ]

    for product in PRODUCT_URLS:
        st.subheader(product)

        product_data = cache_data.get(product, {})

        if not product_data:
            st.write("No data")
            continue

        df = pd.DataFrame.from_dict(product_data, orient="index")
        df.index.name = "Version"

        df = df[
            [c for c in df.columns if c not in ["adcm_min_version"] + ADB_COMPONENTS]
        ]

        df = df.dropna(how="all")
        df = df.sort_index(ascending=False)

        st.dataframe(df, width="stretch")

# --- ADCM TAB ---
with tab1:
    st.header("ADCM Minimal Version")

    for product in PRODUCT_URLS:
        st.subheader(product)

        product_data = cache_data.get(product, {})

        rows = [
            {"Version": version, "adcm_min_version": data.get("adcm_min_version")}
            for version, data in product_data.items()
        ]

        df = pd.DataFrame(rows)

        if not df.empty:
            df = df.sort_values(
                by="Version",
                key=lambda col: col.map(
                    lambda v: [int(x) for x in str(v).split(".")]
                ),
                ascending=False,
            ).reset_index(drop=True)

        st.dataframe(df, width="stretch", hide_index=True)

with tab3:
    st.header("ADB Components Versions")

    product = "ADB"
    product_data = cache_data.get(product, {})

    if not product_data:
        st.write("No data")
    else:
        df = pd.DataFrame.from_dict(product_data, orient="index")
        df.index.name = "Version"

        ADB_COMPONENTS = [
            "adcc",
            "adbm",
            "gpbackup",
            "pxf",
            "tkh_connector",
            "kafka_connector",
        ]

        df = df[[c for c in ADB_COMPONENTS if c in df.columns]]

        df = df.dropna(how="all")

        df = df.sort_index(
            key=lambda col: col.map(
                lambda v: [int(x) for x in str(v).split(".")]
            ),
            ascending=False,
        )

        st.dataframe(df, width="stretch")
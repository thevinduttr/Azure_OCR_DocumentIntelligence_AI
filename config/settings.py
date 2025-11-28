# config/settings.py

from pathlib import Path
import yaml

PROJECT_ROOT = Path(__file__).resolve().parent.parent

PATHS_CONFIG_FILE = PROJECT_ROOT / "config" / "paths.yml"
AZURE_CONFIG_FILE = PROJECT_ROOT / "config" / "azure.yml"
AZURE_OPENAI_CONFIG_FILE = PROJECT_ROOT / "config" / "azure_openai.yml"
DOC_TYPES_CONFIG_FILE = PROJECT_ROOT / "config" / "doc_types.yml"
STORAGE_CONFIG_FILE = PROJECT_ROOT / "config" / "storage.yml"
DATABASE_CONFIG_FILE = PROJECT_ROOT / "config" / "database.yml"
CUSTOMER_MAPPING_CONFIG_FILE = PROJECT_ROOT / "config" / "customer_mapping.yml"


def _load_yaml(path: Path) -> dict:
    if not path.exists():
        return {}
    with path.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


# ---- Paths config ----
_paths_cfg = _load_yaml(PATHS_CONFIG_FILE).get("paths", {})

RAW_DOCUMENTS_DIR = PROJECT_ROOT / _paths_cfg.get("raw_documents_dir", "data/raw_documents")
PROCESSED_DOCUMENTS_DIR = PROJECT_ROOT / _paths_cfg.get("processed_documents_dir", "data/processed_documents")
PROCESSED_FILENAME = _paths_cfg.get("processed_filename", "processed_document.pdf")
PROCESSED_PDF_PATH = PROCESSED_DOCUMENTS_DIR / PROCESSED_FILENAME

OCR_OUTPUT_DIR = PROJECT_ROOT / _paths_cfg.get("ocr_output_dir", "data/ocr_output")
AI_OUTPUT_DIR = PROJECT_ROOT / _paths_cfg.get("ai_output_dir", "data/ai_output") 
FINAL_DOCUMENTS_DIR = PROJECT_ROOT / _paths_cfg.get("final_documents_dir", "data/final_documents")


# ---- Azure Document Intelligence config ----
_azure_di_cfg = _load_yaml(AZURE_CONFIG_FILE).get("azure", {}).get("document_intelligence", {})

AZURE_DI_ENDPOINT = _azure_di_cfg.get("endpoint")
AZURE_DI_KEY = _azure_di_cfg.get("key")
AZURE_DI_LAYOUT_MODEL_ID = _azure_di_cfg.get("layout_model_id", "prebuilt-layout")


# ---- Azure OpenAI config ----
_azure_oai_cfg = _load_yaml(AZURE_OPENAI_CONFIG_FILE).get("azure_openai", {})

AZURE_OAI_ENDPOINT = _azure_oai_cfg.get("endpoint")
AZURE_OAI_DEPLOYMENT_NAME = _azure_oai_cfg.get("deployment_name")
AZURE_OAI_KEY = _azure_oai_cfg.get("api_key")
AZURE_OAI_API_VERSION = _azure_oai_cfg.get("api_version", "2025-01-01-preview")

# ---- Doc types config ----
_doc_types_cfg = _load_yaml(DOC_TYPES_CONFIG_FILE).get("doc_types", {})

DOC_TYPE_CONFIG = {
    key: {
        "name": val.get("name"),
        "output_filename": val.get("output_filename"),
        "db_code": val.get("db_code"),
    }
    for key, val in _doc_types_cfg.items()
    if val.get("name") and val.get("output_filename")
}

DOC_TYPE_NAME_TO_FILENAME = {
    cfg["name"]: cfg["output_filename"]
    for cfg in DOC_TYPE_CONFIG.values()
}

DOC_TYPE_NAME_TO_DB_CODE = {
    cfg["name"]: cfg.get("db_code")
    for cfg in DOC_TYPE_CONFIG.values()
    if cfg.get("db_code")
}

# ---- Storage config ----
_storage_cfg = _load_yaml(STORAGE_CONFIG_FILE).get("storage", {})

BLOB_CONNECTION_STRING = _storage_cfg.get("connection_string")
BLOB_ACCOUNT_URL = _storage_cfg.get("account_url")
BLOB_CONTAINER_NAME = _storage_cfg.get("container", "file-container")
BLOB_PARENT_PREFIX = _storage_cfg.get("parent_prefix", "")          # e.g. 2025/11/12/613690000
BLOB_PROCESSED_SUBFOLDER = _storage_cfg.get("processed_subfolder", "processed_document")

# ---- Database config ----
_db_cfg = _load_yaml(DATABASE_CONFIG_FILE).get("database", {})

DB_CONNECTION_STRING = _db_cfg.get("connection_string")
DB_DEFAULT_SUBMISSION_ID = _db_cfg.get("default_submission_id", 70)
DB_DEFAULT_REQUEST_ID = _db_cfg.get("default_request_id", 65)
DB_DEFAULT_OCR_STATUS = _db_cfg.get("default_ocr_status", "SUCCESS")
DB_STORAGE_RETENTION_DAYS = _db_cfg.get("storage_retention_days")

# --- Customer mapping config ----
_customer_mapping_cfg = _load_yaml(CUSTOMER_MAPPING_CONFIG_FILE).get("customer", {})

CUSTOMER_DOC_TYPES = _customer_mapping_cfg.get("doc_types", {})
CUSTOMER_VALIDATION = _customer_mapping_cfg.get("validation", {})
CUSTOMER_FIELD_MAPPING = _customer_mapping_cfg.get("field_mapping", {})
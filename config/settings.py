# config/settings.py

from pathlib import Path
import yaml

PROJECT_ROOT = Path(__file__).resolve().parent.parent

PATHS_CONFIG_FILE = PROJECT_ROOT / "config" / "paths.yml"
AZURE_CONFIG_FILE = PROJECT_ROOT / "config" / "azure.yml"
AZURE_OPENAI_CONFIG_FILE = PROJECT_ROOT / "config" / "azure_openai.yml"
DOC_TYPES_CONFIG_FILE = PROJECT_ROOT / "config" / "doc_types.yml"



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
    }
    for key, val in _doc_types_cfg.items()
    if val.get("name") and val.get("output_filename")
}

# Convenience: map from Doc Type name -> output filename
DOC_TYPE_NAME_TO_FILENAME = {
    cfg["name"]: cfg["output_filename"]
    for cfg in DOC_TYPE_CONFIG.values()
}
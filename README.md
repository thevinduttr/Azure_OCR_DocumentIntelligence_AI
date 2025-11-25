# 📄 Document Processing & AI-Based Field Extraction Pipeline

## 🚀 Overview

This project automates the processing of customer documents using Azure AI services. It merges multiple PDFs/images into a single PDF, extracts text using Azure Document Intelligence (Layout Model), and classifies each page using Azure OpenAI GPT‑4.1. The system identifies Emirates ID, Driving License, Vehicle Registration (Mulkiya), or Other Document types and extracts structured fields for validation or database use. All paths, keys, and endpoints are fully configurable via YAML files, ensuring the system requires no code modification after deployment.

## 📦 Project Structure

```
project-root/
│
├── main.py
├── config/
│   ├── paths.yml
│   ├── azure.yml
│   ├── azure_openai.yml
│   └── settings.py
├── services/
│   ├── document_merger.py
│   ├── azure_ocr_client.py
│   └── document_classifier.py
├── data/
│   ├── raw_documents/
│   ├── processed_documents/
│   ├── ocr_output/
│   └── ai_output/
├── tests/
│   ├── test_document_merger.py
│   └── test_azure_ocr_client.py
├── requirements.txt
└── README.md
```

## 🛠️ Installation

```bash
python -m venv venv
venv\Scripts\activate   # Windows
source venv/bin/activate # Linux/Mac
pip install -r requirements.txt
```

## ⚙️ Configuration

All configs are stored in `config/`.

### paths.yml
```yaml
paths:
  raw_documents_dir: "data/raw_documents"
  processed_documents_dir: "data/processed_documents"
  processed_filename: "processed_document.pdf"
  ocr_output_dir: "data/ocr_output"
  ai_output_dir: "data/ai_output"
```

### azure.yml
```yaml
azure:
  document_intelligence:
    endpoint: "https://<resource>.cognitiveservices.azure.com/"
    key: "<key>"
    layout_model_id: "prebuilt-layout"
```

### azure_openai.yml
```yaml
azure_openai:
  endpoint: "https://<resource>.openai.azure.com/"
  deployment_name: "gpt-4.1"
  api_key: "<key>"
  api_version: "2025-01-01-preview"
```

## ▶️ Running the Pipeline

1. Place documents into:
```
data/raw_documents/
```

2. Run:
```bash
python main.py
```

3. Outputs:
- `data/processed_documents/processed_document.pdf`
- `data/ocr_output/..._layout.json`
- `data/ai_output/..._classified.json`

## 🧪 Testing

```bash
pytest -q
```

## 👤 Author

Thevindu Rathnaweera

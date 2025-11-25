# services/document_classifier.py

import json
import os
from pathlib import Path

import certifi
from openai import AzureOpenAI

from config.settings import (
    AZURE_OAI_ENDPOINT,
    AZURE_OAI_DEPLOYMENT_NAME,
    AZURE_OAI_KEY,
    AZURE_OAI_API_VERSION,
    AI_OUTPUT_DIR,
)

# Fix SSL certificate verification issue (if needed in your env)
os.environ["SSL_CERT_FILE"] = certifi.where()


def classify_document_from_ocr_json(ocr_json_path: Path) -> Path:
    """
    Read OCR JSON (output from Azure Document Intelligence),
    send it to Azure OpenAI for document type + field extraction,
    and save the structured result to AI_OUTPUT_DIR.

    Returns: Path to the saved classification JSON file.
    """
    if not ocr_json_path.exists():
        raise FileNotFoundError(f"OCR JSON file not found: {ocr_json_path}")

    if not (AZURE_OAI_ENDPOINT and AZURE_OAI_DEPLOYMENT_NAME and AZURE_OAI_KEY):
        raise ValueError("Azure OpenAI config (endpoint/deployment/key) is not properly set.")

    # Ensure output directory exists
    AI_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    # Read OCR JSON
    try:
        with ocr_json_path.open("r", encoding="utf-8") as f:
            user_prompt = json.load(f)
    except Exception as e:
        raise RuntimeError(f"Error reading OCR JSON file: {e}") from e

    # Initialize Azure OpenAI client
    client = AzureOpenAI(
        azure_endpoint=AZURE_OAI_ENDPOINT,  # base URL only
        api_key=AZURE_OAI_KEY,
        api_version=AZURE_OAI_API_VERSION,
    )

    # Build messages (system + user) – using your original prompt
    chat_prompt = [
        {
            "role": "system",
            "content": (
                "You will receive a JSON input that contains OCR-extracted text for each page of a document. "
                "Your task is to identify the document type on each page and extract relevant fields with varify and into "
                "a structured JSON format.\n\n"

                "There are four possible document types:\n"
                "1. **Emirates ID**\n"
                "2. **Driving License**\n"
                "3. **Mulkiya / Vehicle Registration Card**\n"
                "4. **Other Document** (any page that does not match above types)\n\n"

                "# 🚀 Document Identification Rules\n"
                "- If the page contains terms like: 'Emirates ID', 'Emirates Identity Authority', 'EmiratesID', 'EID', then Doc Type = 'Emirates ID'.\n"
                "- If the page contains: 'Driving License', 'Driver License', 'DL No', then Doc Type = 'Driving License'.\n"
                "- If the page contains: 'Vehicle Registration', 'Mulkiya', 'Chassis No', 'Engine No', 'Vehicle Make', 'Model', then Doc Type = 'Vehicle Registration'.\n"
                "- Otherwise, Doc Type = 'Other Document'.\n\n"

                "# 📄 Output Requirements\n"
                "Return an object with key `Pages`, containing a list of extracted results.\n"
                "Each page output must follow this structure:\n\n"

                "### **Emirates ID Fields**\n"
                "- Emirates ID\n"
                "- Emirates First Name: Always first name in emirate ID\n"
                "- Emirates Last Name: Except first name all the last name\n"
                "- Nationality\n"
                "- Gender\n"
                "- Emirates ID Expiry Date\n"
                "- Emirates ID Issue Date\n\n"

                "### **Driving License Fields**\n"
                "- License Number\n"
                "- Expiry Date\n"
                "- Issue Date (if available)\n"
                "- Traffic Code\n"
                "- Place Of Issue\n"
                "- Category (if available)\n\n"

                "### **Vehicle Registration (Mulkiya) Fields**\n"
                "- Vehicle Year\n"
                "- Vehicle Make\n"
                "- Vehicle Model\n"
                "- Vehicle Color\n"
                "- Chassis Number\n"
                "- Engine Number: alphanumeric only, 8–17 characters. First, try to locate the Engine Number field (رقم المحرك). "
                " If not found or invalid, find the next alphanumeric value that appears after the Chassis Number in the text. "
                " If still not found or invalid, return empty string."
                "- Plate Number\n"
                "- No of Seat\n"
                "- No of Cylinder\n"
                "### **Other Document**\n"
                "- Only return: \"Doc Type\": \"Other Document\"\n\n"

                "# ℹ️ Missing Fields\n"
                "If a field is missing, return empty string.\n\n"

                "# 📢 Final Output Format Example\n"
                "Return JSON strictly in this format:\n\n"
                "{\n"
                "  \"Pages\": [\n"
                "     {\"page\": 1, \"Doc Type\": \"Emirates ID\", \"Emirates ID\": \"\", \"Nationality\": \"\", \"Gender\": \"\", \"Emirates ID Expiry Date\": \"\", \"Emirates ID Issue Date\": \"\"},\n"
                "     {\"page\": 2, \"Doc Type\": \"Driving License\", \"License Number\": \"\", \"Expiry Date\": \"\", \"Traffic Code\": \"\", \"Place Of Issue\": \"\"},\n"
                "     {\"page\": 3, \"Doc Type\": \"Other Document\"},\n"
                "     {\"page\": 4, \"Doc Type\": \"Vehicle Registration\", \"Vehicle Year\": \"\", \"Vehicle Make\": \"\", \"Vehicle Model\": \"\", \"Vehicle Color\": \"\", \"Chassis Number\": \"\", \"Engine Number\": \"\", \"Plate Number\": \"\", \"No of Seat\": \"\", \"No of Cylinder\": \"\"}\n"
                "  ]\n"
                "}\n\n"

                "Always return only the JSON object and no additional text."
            ),
        },
        {
            "role": "user",
            "content": json.dumps(user_prompt, ensure_ascii=False),
        },
    ]

    try:
        completion = client.chat.completions.create(
            model=AZURE_OAI_DEPLOYMENT_NAME,
            messages=chat_prompt,
            max_tokens=800,
            temperature=0.8,
            top_p=1,
            frequency_penalty=0,
            presence_penalty=0,
        )
    except Exception as e:
        raise RuntimeError(f"Azure OpenAI API call failed: {e}") from e

    response_content = completion.choices[0].message.content

    try:
        classification_data = json.loads(response_content)
    except json.JSONDecodeError as e:
        raise RuntimeError(f"Model response is not valid JSON:\n{response_content}") from e

    # Attach token usage if available
    if hasattr(completion, "usage") and completion.usage is not None:
        classification_data["prompt_tokens"] = completion.usage.prompt_tokens
        classification_data["completion_tokens"] = completion.usage.completion_tokens

    # Save output JSON
    output_path = AI_OUTPUT_DIR / f"{ocr_json_path.stem}_classified.json"
    with output_path.open("w", encoding="utf-8") as f_out:
        json.dump(classification_data, f_out, indent=2, ensure_ascii=False)

    return output_path

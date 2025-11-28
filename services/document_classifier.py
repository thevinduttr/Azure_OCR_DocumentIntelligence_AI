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

    # 🔒 Locked-style prompt, extended with extra document types
    chat_prompt = [
        {
            "role": "system",
            "content": (
                "You will receive a JSON input that contains OCR-extracted text for each page of a document. "
                "Your task is to identify the document type on each page and extract relevant fields with varify and into "
                "a structured JSON format.\n\n"

                "There are nine possible document types:\n"
                "1. **Emirates ID**\n"
                "2. **Driving License**\n"
                "3. **Mulkiya Front Document**\n"
                "4. **Mukiya Back Document**\n"
                "5. **VCC**\n"
                "6. **PCD/Hyasa**\n"
                "7. **E Mulkiya**\n"
                "8. **Other Document** (any page that does not match above types)\n\n"

                "# 🚀 Document Identification Rules\n"
                "- If the page contains terms like: 'Emirates ID', 'Emirates Identity Authority', 'EmiratesID', 'EID', then Doc Type = 'Emirates ID'.\n"
                "- If the page contains: 'Driving License', 'Driver License', 'DL No', then Doc Type = 'Driving License'.\n"
                "- If the page is clearly the front side of a Mulkiya with plate, TC number, registration/expiry dates → Doc Type = 'Mulkiya Front Document'.\n"
                "- If the page is clearly the back side of a Mulkiya with technical data (weights, seats, engine/chassis) → Doc Type = 'Mukiya Back Document'.\n"
                "- If the page is a Vehicle Conformity Certificate → Doc Type = 'VCC'.\n"
                "- If the page is a PCD / Hyasa document (port/customs related, showing vehicle import details) → Doc Type = 'PCD/Hyasa'.\n"
                "- If the page is an electronic Mulkiya (digital/e-card style) → Doc Type = 'E Mulkiya'.\n"
                "- Otherwise, Doc Type = 'Other Document'.\n\n"

                "# 📄 Output Requirements\n"
                "Return an object with key `Pages`, containing a list of extracted results.\n"
                "Each page output must follow this structure:\n\n"

                "### **Common Fields for All Pages**\n"
                "- page (1-based page number)\n"
                "- Doc Type (one of the 9 types listed above)\n\n"

                "### **Emirates ID Fields**\n"
                "- Emirates ID\n"
                "- Emirates First Name: Always first name in emirate ID\n"
                "- Emirates Last Name: Except first name all the last name\n"
                "- Date of Birth\n"
                "- Nationality\n"
                "- Gender\n"
                "- Emirates ID Expiry Date\n"
                "- Emirates ID Issue Date\n\n"

                "### **Driving License Fields**\n"
                "- License Number\n"
                "- License First Name : Always first name in driving license\n"
                "- License Last Name : Except first name all the last name\n"
                "- Nationality\n"
                "- Date of Birth\n"
                "- License Expiry Date\n"
                "- License Issue Date\n"
                "- Traffic Code\n"
                "- Place Of Issue\n\n"

                "### **Mulkiya Front Document Fields**\n"
                "- Traffic Plate Number\n"
                "- Traffic code\n"
                "- TC Number\n"
                "- Mulkiya Expiry Date\n"
                "- Insurance Expiry Date\n"
                "- Reg.Date\n"
                "- Place of issue\n"
                "- Vehicle Category\n"
                "- Policy Type\n\n"

                "### **Mukiya Back Document Fields**\n"
                "- Model\n"
                "- No of Pass.\n"
                "- Origin\n"
                "- Vehicle Type\n"
                "- G.V.W\n"
                "- Empty Weight\n"
                "- Eng. No\n"
                "- Chasis No\n"
                "- Vehicle Type (if repeated)\n"
                "- Color\n\n"

                "### **VCC Fields**\n"
                "- Vehicle Type\n"
                "- Model Year\n"
                "- Origin\n"
                "- Chasis No\n"
                "- Color\n"
                "- Engine No\n"
                "- GCC Standard\n"

                "### **PCD/Hyasa Fields**\n"
                "- TC NO\n"
                "- Plate Number\n"
                "- Plate Color\n"
                "- Vehicle Make\n"
                "- Vehicle Color\n"
                "- Country of Origin\n"
                "- Number of Cylinders\n"
                "- Model\n"
                "- Chasisi Number\n"
                "- Engine Number\n"
                "- Weight Loaded\n"
                "- Net Weight\n"
                "- No of Seats\n"
                "- Registration date\n\n"

                "### **E Mulkiya Fields**\n"
                "- Traffic Plate No\n"
                "- Place of Issue\n"
                "- Plate Class\n"
                "- Plate Number\n"
                "- Traffic Code\n"
                "- Owner Name\n"
                "- Insurance Expiry Date\n"
                "- Insurance Type\n"
                "- Nationality\n"
                "- Registration Date\n"
                "- Mulkiya Expiry Date\n"
                "- Vehicle Year\n"
                "- Vehicle Make\n"
                "- Vehicle Model\n"
                "- Vehicle Color\n"
                "- Vehicle Class\n"
                "- Vehicle Type\n"
                "- Chassis Number\n"
                "- Engine Number\n"
                "- No of Seat\n"
                "- Chasis Number\n"
                "- Empty Weight"
                "- Origin\n\n"
                
                "### **Other Document**\n"
                "- Only return: \"page\" and \"Doc Type\": \"Other Document\". Do not include extra fields.\n\n"

                "# ℹ️ Missing Fields\n"
                "If a field is missing, return empty string.\n\n"

                "# 📢 Final Output Format Example\n"
                "Return JSON strictly in this format:\n\n"
                "{\n"
                "  \"Pages\": [\n"
                "     {\"page\": 1, \"Doc Type\": \"Emirates ID\", \"Emirates ID\": \"\", \"Emirates First Name\": \"\", \"Emirates Last Name\": \"\", \"Nationality\": \"\", \"Gender\": \"\", \"Emirates ID Expiry Date\": \"\", \"Emirates ID Issue Date\": \"\"},\n"
                "     {\"page\": 2, \"Doc Type\": \"Driving License\", \"License Number\": \"\", \"Expiry Date\": \"\", \"Issue Date\": \"\", \"Traffic Code\": \"\", \"Place Of Issue\": \"\", \"Category\": \"\"},\n"
                "     {\"page\": 3, \"Doc Type\": \"Other Document\"},\n"
                "  ]\n"
                "}\n\n"

                "Always return only the JSON object and no additional text."
                "- Engine Number: alphanumeric only, 8–17 characters. First, try to locate the Engine Number field (رقم المحرك). "
                " If not found or invalid, find the next alphanumeric value that appears after the Chassis Number in the text. "
                " If still not found or invalid, return empty string.\n"
                
                "Identify Make and Model separately.\n"
                    "Rules:\n"
                    "- Make must be a real, known car brand.\n"
                    "- If text contains 'Brand/Model', split on / - . or space.\n"
                    "- First valid car brand = Make.\n"
                    "- Everything after that = Model (remove year and special characters).\n"

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
            max_tokens=1200,
            temperature=0.0,
            top_p=1,
            frequency_penalty=0,
            presence_penalty=0,
        )
    except Exception as e:
        raise RuntimeError(f"Azure OpenAI API call failed: {e}") from e

    response_content = completion.choices[0].message.content

    try:
        classification_data = json.loads(response_content)
    except json.JSONDecodeError:
        raise RuntimeError(f"Model response is not valid JSON:\n{response_content}")

    # Attach token usage if available
    if hasattr(completion, "usage") and completion.usage is not None:
        classification_data["prompt_tokens"] = completion.usage.prompt_tokens
        classification_data["completion_tokens"] = completion.usage.completion_tokens

    # Save output JSON
    output_path = AI_OUTPUT_DIR / f"{ocr_json_path.stem}_classified.json"
    with output_path.open("w", encoding="utf-8") as f_out:
        json.dump(classification_data, f_out, indent=2, ensure_ascii=False)

    return output_path

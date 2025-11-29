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
                "Your task is to identify the document type on each page and extract relevant fields with verification "
                "into a structured JSON format.\n\n"

                "There are eight possible document types:\n"
                "1. **Emirates ID**\n"
                "2. **Driving License**\n"
                "3. **Mulkiya Front Document**\n"
                "4. **Mukiya Back Document**\n"
                "5. **VCC**\n"
                "6. **PCD/Hyasa**\n"
                "7. **E Mulkiya**\n"
                "8. **Other Document** (any page that does not match above types)\n\n"

                "# 🚀 Document Identification Rules\n"
                "- If the page contains terms like: 'Resident Identity Card' 'Emirates ID', 'Emirates Identity Authority', 'EmiratesID', 'EID' or If the page contains a machine-readable zone (MRZ) with many '<' characters OR a long numeric card number together with a smart-card chip layout, then Doc Type = 'Emirates ID'.\n"
                "- If the page contains: 'Driving License', 'Driver License', 'DL No' or If the page contains colorful rectangular permit boxes or a Traffic Code Number, and a barcode at the bottom, then Doc Type = 'Driving License'.\n"
                "- If the page is contains: 'Vehicle License', 'Vehicle Information' ,'Mulkiya with plate', 'TC number', registration/expiry dates → Doc Type = 'Mulkiya'.\n"
                "- If the page is a Vehicle Clearence Certificate and it should contain : 'Vehicle Clearence Certificate' → Doc Type = 'VCC'.\n"
                "- If the page is a PCD / Hyasa document (port/customs related, showing vehicle import details) and contains: 'Vehicle Possession Certificate' , 'Local Transfer cetificate' → Doc Type = 'PCD/Hyasa'.\n"
                "- If the page is an electronic Mulkiya (digital/e-card style) and and contains: 'Vehicle License' → Doc Type = 'E Mulkiya'.\n"
                "- Otherwise, Doc Type = 'Other Document'.\n\n"

                "# 📄 Output Requirements\n"
                "Return an object with key `Pages`, containing a list of extracted results.\n"
                "Each page output must follow this structure:\n\n"

                "### **Common Fields for All Pages**\n"
                "- page (1-based page number)\n"
                "- Doc Type (one of the 8 types listed above)\n\n"

                "### **Emirates ID Fields**\n"
                "- Emirates ID (also look for: ID Number, IDN, Identity Number)\n"
                "- Emirates First Name: Always first name in emirate ID (also: Name, First Name)\n"
                "- Emirates Last Name: Except first name all the last name (also: Surname, Name, Last Name)\n"
                "- Date of Birth (also: DOB, Birth Date, Date Of Birth)\n"
                "- Nationality (also: Nationality)\n"
                "- Gender (also: Sex)\n"
                "- Emirates ID Expiry Date (also: Expiry Date, Expiry, Exp. Date, Valid Until)\n"
                "- Emirates ID Issue Date (also: Issue Date, Issueing Date, Date of Issue)\n\n"

                "### **Driving License Fields**\n"
                "- License Number (also: DL No, Licence No, License ID)\n"
                "- License First Name: Always first name in driving license (also: Name, First Name)\n"
                "- License Last Name: Except first name all the last name (also: Surname, Name, Last Name)\n"
                "- Nationality (also: Nationality)\n"
                "- Date of Birth (also: DOB, Birth Date , Date of Birth)\n"
                "- License Expiry Date (also: Expiry Date, Exp. Date, Valid Until)\n"
                "- License Issue Date (also: Issue Date, Issueing Date, Date of Issue)\n"
                "- Traffic Code (also: TC, Traffic Code No, TC No)\n"
                "- License Place Of Issue (also: Issued At, Place, Issuing Authority)\n\n"

                "### **Mulkiya Fields**\n"
                "- Traffic Plate Number: Extract ONLY the numeric part (also: Traffic Plate No, Plate No, Plate Number)\n"
                "- Traffic code: Extract ONLY the letter/code part (also: Traffic Plate No, Traffic Code, TC No)\n"
                "- TC Number (also: Traffic Code Number, TC No , T.C. No.)\n"
                "- Mulkiya Expiry Date (also: Exp. Date, Expiry Date, Valid Until)\n"
                "- Mulkiya Insurance Expiry Date (also: Insurance Exp, Ins. Exp Date, Ins. Exp.)\n"
                "- Mulkiya Registration Date (also: Registration Date, Reg Date, Date of Registration , Reg. Date)\n"
                
                "- Model Year (also: Model, Vehicle Model, Model Year)\n"
                "- No of Pass. (also: Num of Pass, Seats, No. of Passengers)\n"
                "- Origin (also: Origin, Country of Origin, Made In, Manufacturing Country)\n"
                "- Vehicle Make Type (also: Veh. Type, Veh Type, Category, Class)\n"
                "- Vehicle Model Type (also: Veh Type, Vehicle Model)\n"
                "- Gross Vehicle Weight (also: G.V.W, GVW, Gross Vehicle Weight, Total Weight)\n"
                "- Empty Weight (also: Empty Weight, Weight Empty)\n"
                "- Engine Number (also: Eng. No, Eng No, Engine Number, Engine No.)\n"
                "- Chassis Number (also: Chassis Number, VIN, Chasis No, Chassis No.)\n\n"
                
                "### **VCC Fields**\n"
                "- Vehicle Make Type (also: Vehicle Type)\n"
                "- Vehicle Model Type (also: Vehicle Type)\n"
                "- Model Year (also: Model Year, Manufacturing Year, Year of Manufacture) — MUST return only the 4-digit year (see Special Extraction Rules).\n"
                "- Origin (also: Origin, Country of Origin, Made In, Manufacturing Country)\n"
                "- Chassis Number (also: Chassis Number, VIN, Chasis No, Frame Number)\n"
                "- Color (also: Color, Vehicle Color)\n"
                "- Engine No (also: Engine No, Engine Number)\n"
                "- GCC Standard (also: GCC Standard, GCC, Standard, Compliance) — If GCC or GCC Standard is present on the page, set this field to exactly \"Yes\"; if not present, return empty string.\n\n"

                "### **PCD/Hyasa Fields**\n"
                "- TC NO (also: Traffic Code, TC, T.C. No.)\n"
                "- Plate Number (also: Plate Number, Vehicle Plate, Plate No)\n"
                "- Plate Color (also: Plate Color, Color, Plate Colour)\n"
                "- Vehicle Make (also: Vehicle Make, Make, Brand, Manufacturer)\n"
                "- Vehicle Color (also: Vehicle Color, Color, Colour)\n"
                "- Country of Origin (also: Country of Origin, Origin, Made In, Manufacturing Country)\n"
                "- Number of Cylinders (also: Number of Cylinders, Cylinders, No of Cylinders, Cyl)\n"
                "- Model (also: Model, Vehicle Model, Model Name)\n"
                "- Chassis Number (also: Chasisi Number, Chassis Number, VIN, Chasis No, Frame Number)\n"
                "- Engine Number (also: Engine Number, Eng. No, Motor Number, Engine No.)\n"
                "- Weight Loaded (also: Weight Loaded, Gross Weight, Total Weight, GVW)\n"
                "- No of Seats (also: No of Seats, Seats, Seating Capacity, Passengers)\n"
                "- Registration date (also: Registration date, Reg. Date, Registration Date, First Registration)\n\n"

                "### **E Mulkiya Fields**\n"
                "- Traffic Plate No (also: Traffic Plate No, Plate Number, Registration Plate, Vehicle Plate)\n"
                "- Place of Issue (also: Place of Issue, Place)\n"
                "- Plate Class (also: Plate Class, Class, Category, Type)\n"
                "- Traffic Code (also: Traffic Code No, TC, TC No, Traffic Code Number)\n"
                "- Insurance Expiry Date (also: Insurance Expiry, Insurance Exp, Ins. Exp Date, Insurance Valid Until)\n"
                "- Registration Date (also: Registration Date,Reg. Date, First Registration, Date of Registration)\n"
                "- E Mulkiya Insurance Type (also: Insurance Type, Policy Type)\n"
                "- Mulkiya Expiry Date (also: Registration Expiry, Expiry Date, Valid Until)\n"
                "- No of Passengers (also: No of Passengers, Seats, Seating Capacity, Passengers, No of Pass.)\n"
                "- Vehicle Year (also: Year Model, Manufacturing Year, Model Year)\n"
                "- Vehicle Make : only Need to be manufacture of Vehicle (also: Vehicle Class)\n"
                "- Vehicle Model : only neewd to be Vehicel model without manufacturer (also: Vehicle Class)\n"
                "- Origin (also: Country of Origin, Origin, Manufacturing Country)\n"
                "- Vehicle Color (also: Vehicle Color, Color, Colour)\n"
                "- Vehicle Type (DO NOT return this field in final output; see special rule below)\n"
                "- Empty Weight (also: Empty Weight)\n"
                "- Engine Number (also: Engine No, Eng. No, Motor Number, Engine No.)\n"
                "- Chassis Number (also: Chasis No, VIN)\n\n"
                
                "### **Other Document**\n"
                "- Only return: \"page\" and \"Doc Type\": \"Other Document\". Do not include extra fields.\n\n"

                "# 🔍 Field Name Variation Rules - IMPORTANT\n"
                "**The exact field names specified above may NOT appear in the document. You MUST:**\n"
                "1. Look for similar/alternative field names (shown in parentheses after each field)\n"
                "2. Check for field labels in BOTH English and Arabic\n"
                "3. Match field values based on context and position, not just exact label matches\n"
                "4. Common variations to always check:\n"
                "   - Gender = Sex, M/F, Male/Female\n"
                "   - Nationality = Nat., Country, Citizenship\n"
                "   - Date of Birth = DOB, Birth Date, Born\n"
                "   - Expiry Date = Exp. Date, Valid Until, Validity\n"
                "   - Issue Date = Issued On, Date of Issue, Issued Date\n"
                "   - Number = No, No., #\n"
                "   - Chassis = Chasis, VIN, Frame\n"
                "   - Engine = Motor, Eng.\n"
                "   - Plate = Registration Plate, Vehicle Plate\n"
                "   - Make = Brand, Manufacturer\n"
                "   - Model = Model Name, Vehicle Model\n"
                "   - Seats = Seating, Passengers, Capacity\n"
                "   - Weight = Wt., Mass\n"
                "   - Origin = Country, Made In\n"
                "   - If exact field name not found, extract value from most similar field\n"
                "5. If exact field name not found, extract value from most similar field\n"
                "6. Use field position and document layout to identify fields\n\n"

                "# ℹ️ Missing Fields\n"
                "If a field is missing or cannot be extracted (even after checking variations), return empty string (\"\").\n\n"

                "# 🔧 Special Extraction Rules\n"
                "- **Traffic Plate Number & Traffic Code Parsing**: \n"
                "  * If you find a combined format like 'A/12345', 'A-12345', 'A 12345', or 'A12345':\n"
                "    - Extract the LETTER(S) before the separator/number as 'Traffic code' (e.g., 'A')\n"
                "    - Extract the NUMERIC part as 'Traffic Plate Number' (e.g., '12345')\n"
                "  * Common formats:\n"
                "    - 'A/12345' → Traffic code = 'A', Traffic Plate Number = '12345'\n"
                "    - 'AB-98765' → Traffic code = 'AB', Traffic Plate Number = '98765'\n"
                "    - 'D 54321' → Traffic code = 'D', Traffic Plate Number = '54321'\n"
                "    - 'M12345' → Traffic code = 'M', Traffic Plate Number = '12345'\n"
                "  * Separators to check: '/', '-', ' ' (space), or no separator\n"
                "  * If field label is 'Traffic Plate No' or 'Plate Number' and contains both letters and numbers, ALWAYS split them\n"
                "  * If already separate fields exist, use them directly\n\n"
                "- **Engine Number**: alphanumeric only, 8–17 characters. First, try to locate the Engine Number field (رقم المحرك) or alternatives (Eng. No, Motor Number). "
                "If not found or invalid, find the next alphanumeric value that appears after the Chassis Number in the text. "
                "If still not found or invalid, return empty string.\n"
                "- **Chassis Number**: alphanumeric, typically 17 characters (VIN format). Look for 'Chassis', 'Chasis', 'VIN', or 'Frame Number' field.\n"
                "- **Make and Model**: Identify separately. Make must be a real, known car brand. If the source contains a combined value (commonly found under labels like 'Veh. Type', 'Vehicle Type', 'Vehicle Make/Model', or 'Veh Type') such as 'honda civic', 'TOYOTA COROLLA 2015', or 'BMW-320i', you MUST split into Make and Model as follows:\n"
                "  * Prefer recognizing a known brand name (case-insensitive). If recognized, set Make to the brand capitalized (e.g., 'Honda', 'Toyota', 'BMW') and Model to the remaining text trimmed and title-cased (remove standalone year tokens like 2015 and remove engine size tokens like '2.0L').\n"
                "  * If brand is not in known list, fallback: take first word as Make and the rest as Model (apply capitalization: Make = Title case first token, Model = Title case rest).\n"
                "  * Remove common separators such as '/', '-', or multiple spaces. Examples:\n"
                "    - 'honda civic' -> Make='Honda', Model='Civic'\n"
                "    - 'TOYOTA COROLLA 2015' -> Make='Toyota', Model='Corolla'\n"
                "    - 'BMW-320i' -> Make='BMW', Model='320i'\n"
                "  * If you cannot confidently split, set Make to first token and Model to remaining tokens; never merge both into a single field only.\n"
                "- **Dates**: Extract in format shown in document (DD/MM/YYYY or similar). If date Like this '09 October 2025' it need to convert to DD/MM/YYYY format. date need to have numbers. not text \n"
                "- **Names**: For First Name and Last Name, split properly. First word = First Name, remaining = Last Name.\n"
                "- **Gender/Sex**: Accept M, F, Male, Female, or Arabic equivalents. Convert to single letter if found.\n\n"

                "# 📢 Final Output Format - STRICT JSON STRUCTURE\n"
                "Return JSON strictly in this format. Each document type MUST include ALL its fields (use empty string if not found):\n\n"
                "{\n"
                "  \"Pages\": [\n"
                "    {\n"
                "      \"page\": 1,\n"
                "      \"Doc Type\": \"Emirates ID\",\n"
                "      \"Emirates ID\": \"\",\n"
                "      \"Emirates First Name\": \"\",\n"
                "      \"Emirates Last Name\": \"\",\n"
                "      \"Date of Birth\": \"\",\n"
                "      \"Nationality\": \"\",\n"
                "      \"Gender\": \"\",\n"
                "      \"Emirates ID Expiry Date\": \"\",\n"
                "      \"Emirates ID Issue Date\": \"\"\n"
                "    },\n"
                "    {\n"
                "      \"page\": 2,\n"
                "      \"Doc Type\": \"Driving License\",\n"
                "      \"License Number\": \"\",\n"
                "      \"License First Name\": \"\",\n"
                "      \"License Last Name\": \"\",\n"
                "      \"Nationality\": \"\",\n"
                "      \"Date of Birth\": \"\",\n"
                "      \"License Expiry Date\": \"\",\n"
                "      \"License Issue Date\": \"\",\n"
                "      \"Traffic Code\": \"\",\n"
                "      \"License Place Of Issue\": \"\"\n"
                "    },\n"
                "    {\n"
                "      \"page\": 3,\n"
                "      \"Doc Type\": \"Mulkiya\",\n"
                "      \"Traffic Plate Number\": \"\",\n"
                "      \"Traffic code\": \"\",\n"
                "      \"TC Number\": \"\",\n"
                "      \"Mulkiya Expiry Date\": \"\",\n"
                "      \"Mulkiya Insurance Expiry Date\": \"\",\n"
                "      \"Mulkiya Registration Date\": \"\"\n"
                "    },\n"
                "    {\n"
                "      \"page\": 4,\n"
                "      \"Doc Type\": \"Mulkiya\",\n"
                "      \"Model Year\": \"\",\n"
                "      \"No of Pass.\": \"\",\n"
                "      \"Origin\": \"\",\n"
                "      \"Vehicle Make Type\": \"\",\n"
                "      \"Vehicle Model Type\": \"\",\n"
                "      \"Gross Vehicle Weight\": \"\",\n"
                "      \"Empty Weight\": \"\",\n"
                "      \"Engine Number\": \"\",\n"
                "      \"Chassis Number\": \"\"\n"
                "    },\n"
                "    {\n"
                "      \"page\": 5,\n"
                "      \"Doc Type\": \"VCC\",\n"
                "      \"Vehicle Make Type\": \"\",\n"
                "      \"Vehicle Model Type\": \"\",\n"
                "      \"Model Year\": \"\",\n"
                "      \"Origin\": \"\",\n"
                "      \"Chassis Number\": \"\",\n"
                "      \"Color\": \"\",\n"
                "      \"Engine No\": \"\",\n"
                "      \"GCC Standard\": \"\"\n"
                "    },\n"
                "    {\n"
                "      \"page\": 6,\n"
                "      \"Doc Type\": \"PCD/Hyasa\",\n"
                "      \"TC NO\": \"\",\n"
                "      \"Plate Number\": \"\",\n"
                "      \"Plate Color\": \"\",\n"
                "      \"Vehicle Make\": \"\",\n"
                "      \"Vehicle Color\": \"\",\n"
                "      \"Country of Origin\": \"\",\n"
                "      \"Number of Cylinders\": \"\",\n"
                "      \"Model\": \"\",\n"
                "      \"Chassis Number\": \"\",\n"
                "      \"Engine Number\": \"\",\n"
                "      \"Weight Loaded\": \"\",\n"
                "      \"No of Seats\": \"\",\n"
                "      \"Registration date\": \"\"\n"
                "    },\n"
                "    {\n"
                "      \"page\": 7,\n"
                "      \"Doc Type\": \"E Mulkiya\",\n"
                "      \"Traffic Plate No\": \"\",\n"
                "      \"Place of Issue\": \"\",\n"
                "      \"Plate Class\": \"\",\n"
                "      \"Traffic Code\": \"\",\n"
                "      \"Insurance Expiry Date\": \"\",\n"
                "      \"Registration Date\": \"\",\n"
                "      \"E Mulkiya Insurance Type\": \"\",\n"
                "      \"Mulkiya Expiry Date\": \"\",\n"
                "      \"No of Passengers\": \"\",\n"
                "      \"Vehicle Year\": \"\",\n"
                "      \"Vehicle Make\": \"\",\n"
                "      \"Vehicle Model\": \"\",\n"
                "      \"Origin\": \"\",\n"
                "      \"Vehicle Color\": \"\",\n"
                "      \"Vehicle Class\": \"\",\n"
                "      \"Empty Weight\": \"\",\n"
                "      \"Engine Number\": \"\",\n"
                "      \"Chassis Number\": \"\"\n"
                "    },\n"
                "    {\n"
                "      \"page\": 8,\n"
                "      \"Doc Type\": \"Other Document\"\n"
                "    }\n"
                "  ]\n"
                "}\n\n"

                "# ⚠️ CRITICAL RULES\n"
                "1. ALWAYS return ALL fields for each document type, even if empty.\n"
                "2. Use empty string \"\" for missing values, never null or undefined.\n"
                "3. Field names in OUTPUT must match EXACTLY as shown above (case-sensitive, including spaces and dots).\n"
                "4. When SEARCHING for fields in INPUT, check ALL variations and alternatives listed.\n"
                "5. Return ONLY the JSON object. No additional text, explanations, or markdown.\n"
                "6. For 'Other Document', only include 'page' and 'Doc Type' fields.\n"
                "7. Check both English AND Arabic field labels in the document.\n"
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

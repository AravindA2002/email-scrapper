import os, io, json, time, base64, logging
from datetime import datetime, timezone
from typing import List, Dict, Tuple

import requests
import pdfplumber
import pandas as pd
from docx import Document
from dotenv import load_dotenv

from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build

# -------------------- setup --------------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")
logger = logging.getLogger("email-poller")

load_dotenv()
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "").strip()
OPENAI_MODEL   = os.getenv("OPENAI_MODEL", "gpt-4.1-mini")

if not OPENAI_API_KEY or not OPENAI_API_KEY.startswith(("sk-", "sk-")):
    raise SystemExit("OPENAI_API_KEY missing or invalid. Put a server key in .env")

# choose one place in your file
MARK_AS_READ = True   # or False to disable

SCOPES = ["https://www.googleapis.com/auth/gmail.modify"] if MARK_AS_READ \
         else ["https://www.googleapis.com/auth/gmail.readonly"]

STATE_FILE = "state.json"   # stores last historyId or message ids processed

# -------------------- Google Auth --------------------
def get_gmail_service():
    creds = None
    if os.path.exists("token.json"):
        creds = Credentials.from_authorized_user_file("token.json", SCOPES)
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            from google.auth.transport.requests import Request
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file("credentials.json", SCOPES)
            creds = flow.run_local_server(port=0)
        with open("token.json", "w") as f:
            f.write(creds.to_json())
    return build("gmail", "v1", credentials=creds, cache_discovery=False)

# -------------------- Attachment extraction --------------------
def _b64(data: str) -> bytes:
    return base64.urlsafe_b64decode(data.encode("utf-8"))

def extract_text_from_pdf(b: bytes) -> str:
    try:
        with pdfplumber.open(io.BytesIO(b)) as pdf:
            return "\n".join(page.extract_text() or "" for page in pdf.pages)
    except Exception as e:
        logger.warning("PDF parse failed: %s", e)
        return ""

def extract_text_from_docx(b: bytes) -> str:
    try:
        doc = Document(io.BytesIO(b))
        return "\n".join(p.text for p in doc.paragraphs)
    except Exception as e:
        logger.warning("DOCX parse failed: %s", e)
        return ""

def extract_text_from_csv_xlsx(b: bytes, ext: str) -> str:
    try:
        if ext == ".csv":
            df = pd.read_csv(io.BytesIO(b))
        else:
            df = pd.read_excel(io.BytesIO(b))
        return df.to_csv(index=False)
    except Exception as e:
        logger.warning("%s parse failed: %s", ext.upper(), e)
        return ""

def extract_attachment_text(filename: str, data: bytes) -> str:
    name = (filename or "").lower()
    if name.endswith(".pdf"):
        return extract_text_from_pdf(data)
    if name.endswith(".docx"):
        return extract_text_from_docx(data)
    if name.endswith(".csv"):
        return extract_text_from_csv_xlsx(data, ".csv")
    if name.endswith(".xlsx"):
        return extract_text_from_csv_xlsx(data, ".xlsx")
    if name.endswith((".txt", ".md", ".html")):
        try: return data.decode("utf-8", errors="ignore")
        except: return ""
    # images/others: skip text extraction but note filename
    return ""

# -------------------- OpenAI (Responses API) --------------------
def openai_map_reduce(plain_text: str, attachments: List[Tuple[str,str]]) -> Dict:
    headers = {
        "Authorization": f"Bearer {OPENAI_API_KEY}",
        "Content-Type": "application/json",
    }
    # Map: split to chunks to keep prompts small
    chunks, size, step = [], len(plain_text), 4000
    for i in range(0, size, step):
        chunks.append(plain_text[i:i+step])

    mapped = []
    map_prompt = (
        "You are extracting clean text from an email chunk. "
        "Return only the readable text (no boilerplate), keep lists as lines.\n\n{chunk}"
    )
    for c in chunks or [""]:
        payload = {"model": OPENAI_MODEL, "input": map_prompt.format(chunk=c)}
        r = requests.post("https://api.openai.com/v1/responses", headers=headers, json=payload, timeout=60)
        if r.status_code >= 400:
            raise RuntimeError(f"OpenAI map error {r.status_code}: {r.text}")
        mapped.append(r.json()["output"][0]["content"][0]["text"])

    att_text = "\n\n".join(f"\n### Attachment: {fn}\n{txt}" for fn, txt in attachments if txt.strip())
    reduce_input = "\n\n".join(mapped) + ("\n\n" + att_text if att_text else "")
    reduce_prompt = (
        "You will produce a JSON with fields: subject, key_points (list), plain_text, tl_dr.\n"
        "Summarize, normalize spacing, and keep URLs or order numbers if present.\n\n{body}"
    )
    payload = {"model": OPENAI_MODEL, "input": reduce_prompt.format(body=reduce_input)}
    r = requests.post("https://api.openai.com/v1/responses", headers=headers, json=payload, timeout=90)
    if r.status_code >= 400:
        raise RuntimeError(f"OpenAI reduce error {r.status_code}: {r.text}")
    result_text = r.json()["output"][0]["content"][0]["text"]
    try:
        return json.loads(result_text)
    except Exception:
        # Fallback: wrap as JSON
        return {"subject": None, "key_points": [], "plain_text": reduce_input[:2000], "tl_dr": result_text[:500]}

# -------------------- Gmail helpers --------------------
def get_message_full(gmail, msg_id: str) -> Dict:
    return gmail.users().messages().get(userId="me", id=msg_id, format="full").execute()

def list_new_message_ids(gmail, since_ts: int) -> List[str]:
    """
    Returns unread messages from Primary Inbox only.
    Filters to 'newer_than' to keep the list small on first run.
    """
    # You can tighten/loosen this window; 14d is a safe default
    q = "in:inbox category:primary label:unread newer_than:14d"

    # If you keep your own timestamp, you can add it too (optional)
    # if since_ts:
    #     # Gmail doesn't support absolute epoch in queries; we rely on newer_than
    #     pass

    ids = []
    page_token = None
    while True:
        res = gmail.users().messages().list(
            userId="me",
            q=q,
            maxResults=50,
            pageToken=page_token
        ).execute()
        ids.extend([m["id"] for m in res.get("messages", [])])
        page_token = res.get("nextPageToken")
        if not page_token:
            break
    return ids

def mark_as_read(gmail, msg_id: str):
    gmail.users().messages().modify(
        userId="me",
        id=msg_id,
        body={"removeLabelIds": ["UNREAD"]}
    ).execute()



def decode_payload(parts_or_payload: Dict) -> str:
    data = parts_or_payload.get("body", {}).get("data")
    if data: return _b64(data).decode("utf-8", errors="ignore")
    return ""

def get_attachments(gmail, msg: Dict) -> List[Tuple[str,str]]:
    out = []
    def walk(part):
        mime = part.get("mimeType","")
        body = part.get("body", {})
        filename = part.get("filename") or ""
        if filename and body.get("attachmentId"):
            att = gmail.users().messages().attachments().get(
                userId="me", messageId=msg["id"], id=body["attachmentId"]).execute()
            data = _b64(att["data"])
            text = extract_attachment_text(filename, data)
            out.append((filename, text))
        for p in part.get("parts", []) or []:
            walk(p)
    payload = msg.get("payload", {})
    walk(payload)
    return out

def plain_text_from_msg(msg: Dict) -> str:
    payload = msg.get("payload", {})
    stack = [payload]
    texts = []
    while stack:
        p = stack.pop()
        mime = p.get("mimeType","")
        if mime in ("text/plain", "text/html"):
            texts.append(decode_payload(p))
        for c in p.get("parts", []) or []:
            stack.append(c)
    return "\n\n".join(texts)

# -------------------- Poll loop --------------------
def load_state():
    if os.path.exists(STATE_FILE):
        return json.load(open(STATE_FILE))
    return {"last_run": 0, "seen": []}

def save_state(s): json.dump(s, open(STATE_FILE,"w"))

def process_once():
    gmail = get_gmail_service()
    state = load_state()
    ids = list_new_message_ids(gmail, state.get("last_run", 0))
    new_ids = [i for i in ids if i not in set(state.get("seen", []))]
    if not new_ids:
        logger.info("No new messages.")
        return

    for mid in new_ids:
        msg = get_message_full(gmail, mid)
        headers = msg.get("payload", {}).get("headers", [])
        subject = next((h["value"] for h in headers if h["name"].lower()=="subject"), "(no subject)")
        body_text = plain_text_from_msg(msg)
        atts = get_attachments(gmail, msg)

        logger.info("Parsing message %s: %s (attachments: %d)", mid, subject, len(atts))
        try:
            parsed = openai_map_reduce(body_text, atts)
        except Exception as e:
            logger.error("OpenAI parse failed: %s", e)
            continue

        # Output: write a JSON per email
        ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        out = {"id": mid, "subject": subject, "parsed": parsed}
        os.makedirs("out", exist_ok=True)
        with open(f"out/{ts}_{mid}.json","w",encoding="utf-8") as f:
            json.dump(out, f, ensure_ascii=False, indent=2)
        logger.info("Saved out/%s_%s.json", ts, mid)

                # after saving output without errors
        try:
            mark_as_read(gmail, mid)   # comment this out if you prefer not to auto-mark read
        except Exception as e:
            logger.warning("Could not mark %s as read: %s", mid, e)


        state.setdefault("seen", []).append(mid)
        state["last_run"] = int(time.time())
        save_state(state)

if __name__ == "__main__":
    # run once; to daemonize, wrap in while True + sleep
    process_once()

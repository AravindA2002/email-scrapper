import os, io, json, time, base64, logging
from datetime import datetime, timezone
from typing import List, Dict, Tuple

import requests
import pdfplumber
import pandas as pd
import time
import base64
import re
from bs4 import BeautifulSoup
from email.message import EmailMessage
from email.utils import parseaddr
from typing import Set

POLL_SECONDS = 5
START_TS_MS = int(time.time() * 1000)   
processed_ids: Set[str] = set()

from docx import Document
from dotenv import load_dotenv

from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build


logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")
logger = logging.getLogger("email-poller")

load_dotenv()
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "").strip()
OPENAI_MODEL   = os.getenv("OPENAI_MODEL", "gpt-4.1-mini")
USE_OPENAI_CLEAN = os.environ.get("USE_OPENAI_CLEAN", "false").lower() in ("1","true","yes")


if not OPENAI_API_KEY or not OPENAI_API_KEY.startswith(("sk-", "sk-")):
    raise SystemExit("OPENAI_API_KEY missing or invalid. Put a server key in .env")


MARK_AS_READ = True   

SCOPES = [
    "https://www.googleapis.com/auth/gmail.modify",  
    "https://www.googleapis.com/auth/gmail.send",    
]
STATE_FILE = "state.json" 



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

def _openai(payload: dict):
    
    headers = {
        "Authorization": f"Bearer {OPENAI_API_KEY}",
        "Content-Type": "application/json",
    }
    r = requests.post("https://api.openai.com/v1/responses", headers=headers, json=payload, timeout=60)
    r.raise_for_status()
    return r.json()

def clean_text_with_openai(text: str) -> str:
    if not text or not USE_OPENAI_CLEAN:
        return text or ""
    try:
        payload = {
            "model": os.getenv("OPENAI_MODEL", "gpt-4.1-mini"),
            "input": f"Clean and normalize this text for downstream parsing. Output plain text only:\n\n{text[:120000]}",
        }
        data = _openai(payload)  
       
        return data.get("output_text", text)  
    except Exception as e:
        logger.warning("OpenAI clean failed, returning raw text: %s", e)
        return text or ""

def _b64(data: str) -> bytes:
    return base64.urlsafe_b64decode(data.encode("utf-8"))

ACK_SUBJECT_PREFIX = "Re: "
ACK_BODY = (
    "Hi,\n\n"
    "Your invoice email has been received. Please allow us some time to process the invoice.\n\n"
    "Thanks"
)

def get_header(msg, name, default=""):
    headers = msg.get("payload", {}).get("headers", [])
    return next((h["value"] for h in headers if h["name"].lower() == name.lower()), default)

def send_acknowledgement(gmail, *, original_msg, to_addr: str) -> None:
   
    subject = get_header(original_msg, "Subject", "(no subject)")
    thread_id = original_msg.get("threadId")

    em = EmailMessage()
    em["To"] = to_addr
    em["Subject"] = f"{ACK_SUBJECT_PREFIX}{subject}"
    em["In-Reply-To"] = get_header(original_msg, "Message-ID", "")
    refs = " ".join(filter(None, [
        get_header(original_msg, "References", ""),
        get_header(original_msg, "Message-ID", "")
    ])).strip()
    if refs:
        em["References"] = refs

    em.set_content(ACK_BODY, subtype="plain", cte="7bit")


    
    raw = base64.urlsafe_b64encode(em.as_bytes()).decode("utf-8")

    body = {"raw": raw}
    if thread_id:
        body["threadId"] = thread_id  

    gmail.users().messages().send(userId="me", body=body).execute()

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
   
    return ""


'''
def openai_map_reduce(plain_text: str, attachments: List[Tuple[str,str]]) -> Dict:
    headers = {
        "Authorization": f"Bearer {OPENAI_API_KEY}",
        "Content-Type": "application/json",
    }
   
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
       
        return {"subject": None, "key_points": [], "plain_text": reduce_input[:2000], "tl_dr": result_text[:500]}
    
'''


def get_message_full(gmail, msg_id: str) -> Dict:
    return gmail.users().messages().get(userId="me", id=msg_id, format="full").execute()

def list_new_message_ids(gmail, since_ts: int) -> List[str]:
    
   
    q = "in:inbox category:primary label:unread newer_than:14d"

   

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

  

def _normalize_text(s: str) -> str:
    if not s:
        return ""
  
    s = s.replace("\r\n", "\n").replace("\r", "\n")
    s = re.sub(r"\n{3,}", "\n\n", s)
    return s.strip()

def _html_to_text(html: str) -> str:
    if not html:
        return ""
    soup = BeautifulSoup(html, "html.parser")
   
    text = soup.get_text(separator="\n")
    return _normalize_text(text)

def plain_text_from_msg(msg: Dict) -> str:
    
    payload = msg.get("payload", {})
    stack = [payload]
    plains = []
    htmls = []

    while stack:
        p = stack.pop()
        mime = p.get("mimeType", "") or ""
        if mime == "text/plain":
            plains.append(decode_payload(p))
        elif mime == "text/html":
            htmls.append(decode_payload(p))
        for c in p.get("parts", []) or []:
            stack.append(c)

    if plains:
      
        text = "\n\n".join(t for t in ( _normalize_text(x) for x in plains ) if t)
        return text

 
    if htmls:
        text = "\n\n".join(t for t in (_html_to_text(h) for h in htmls) if t)
        return text

    return ""



def load_state():
    if os.path.exists(STATE_FILE):
        return json.load(open(STATE_FILE))
    return {"last_run": 0, "seen": []}

def save_state(s): json.dump(s, open(STATE_FILE,"w"))

def poll_forever():
    gmail = get_gmail_service()
    logger.info("Polling every %s seconds. Only emails AFTER %s will be processed.",
                POLL_SECONDS, START_TS_MS)

    while True:
        try:
        
            q = "in:inbox category:primary newer_than:14d"
            res = gmail.users().messages().list(userId="me", q=q, maxResults=50).execute()
            ids = [m["id"] for m in res.get("messages", [])]

            for mid in ids:
                if mid in processed_ids:
                    continue

            
                meta = gmail.users().messages().get(
                    userId="me", id=mid, format="metadata"
                ).execute()

                internal_ms = int(meta.get("internalDate", "0"))
                if internal_ms <= START_TS_MS:
                   
                    continue

              
                msg = get_message_full(gmail, mid)
                
                body_text = plain_text_from_msg(msg)
                atts = get_attachments(gmail, msg)

             
                email_text = clean_text_with_openai(body_text)
                attachments_json = [
                    {"filename": fn, "text": clean_text_with_openai(txt)}
                    for (fn, txt) in atts
                    ]
                
                out_doc = {"email_text": email_text, "attachments": attachments_json}

                os.makedirs("out", exist_ok=True)
                ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
                out_path = f"out/{ts}_{mid}.json"
                with open(out_path, "w", encoding="utf-8") as f:
                    json.dump(out_doc, f, ensure_ascii=False, indent=2)

                size = os.path.getsize(out_path)
                logger.info(f"Saved {out_path} ({size} bytes); ready for downstream invoice parsing.")

           
                from_header = get_header(msg, "From", "")
                _, sender_email = parseaddr(from_header)

                try:
                    if sender_email:
                        send_acknowledgement(gmail, original_msg=msg, to_addr=sender_email)
                        logger.info("Sent acknowledgement to %s", sender_email)

                    else:
                        logger.warning("Could not parse sender address from From: %r", from_header)

                except Exception as e:
                    logger.error("Failed to send acknowledgement: %s", e)


                processed_ids.add(mid)


               
                mark_as_read(gmail, mid)


               
           
                logger.info("Saved %s (%d bytes) and finished message %s",out_path, size, mid)
                logger.info("Polling for next email...")


                

        except Exception as e:
            logger.warning("Poll iteration error: %s", e)

        time.sleep(POLL_SECONDS)

if __name__ == "__main__":
    poll_forever()


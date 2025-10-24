## Setup Instructions

### Step 1: Clone the repository

git clone https://github.com/AravindA2002/email-scrapper.git

cd email-scrapper


---

### Step 2: Add Google credentials
Download your Google OAuth `credentials.json` file from Google Cloud Console and place it in the root folder of the project (same location as `main.py`).

---

### Step 3: Create and activate virtual environment

python -m venv .venv


---

### Step 4: Install dependencies

pip install -r requirements.txt


---

### Step 5: Configure environment variables
Rename `.env.sample` to `.env`


Open `.env` and add your OpenAI API key and model. Example:

OPENAI_API_KEY=yourapikeyhere
OPENAI_MODEL=gpt-4.1-mini
USE_OPENAI_CLEAN=true


---

### Step 6: Run the script


On the first run, a browser window will open asking you to sign in to your Google account and authorize the app.  
After authorization, the script will start polling every few seconds for new unread emails.

---

## Output
Processed emails are saved inside the `out` folder in JSON format.



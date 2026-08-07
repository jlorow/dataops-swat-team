# DataOps SWAT Team
AI-powered incident response for data pipelines, built on DataHub.

## Stack
- DataHub (local Docker)
- Python 3.11+
- Streamlit
- Ollama (local LLM)

## Quick Start (GitHub Codespaces)

The easiest way to run this project is in GitHub Codespaces:

1. Click the green **<> Code** button on this repo → **Codespaces** tab → **Create codespace on main**
2. In the terminal: `docker compose up -d`
3. Wait 90 seconds for DataHub to initialize
4. Open the **PORTS** tab and click the links for:
   - Port `9002` → DataHub UI
   - Port `8501` → Streamlit Dashboard (once running)

## Local Setup (Alternative)

Requires Docker Desktop and Python 3.11+.

```bash
docker compose up -d
pip install -r requirements.txt
streamlit run ui/app.py
```

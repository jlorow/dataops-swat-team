.PHONY: setup run seed test

setup:
	@echo "Run: docker compose up -d"

run:
	@echo "Run: streamlit run ui/app.py"

seed:
	@echo "Run: python scripts/seed_datahub.py"

test:
	@echo "Run: pytest tests/"

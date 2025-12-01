# ReStructured Version

20251126
Harry Kember

This is the exact same as the previous V5 (sourceNEW), but we have broken down app.py into multiple modules for better control and understanding. 

## Structure and workflow 
Keeping same workflow for ingest_dir.py and the Docs Retail and Charts data. Main Changes from V5 are the layout of the backend workflow. 

### Backend
- db_insert_company_counts.py AND db_view_compnany_counts.py
This is the preliminary V5 work that we did for the compnaies spoken about in the doucments! 
- pdfint.db 
DB file 


- database_manager.py 
Handles DB connections, schema checks, and file/path helpers.

- query_manager.py
This is the “brain”: it parses queries, builds filters, calls DB + OpenAI, ranks chunks, and formats results for the API.

- app.py

- config.py
all config 

- openai_manager.py





## Updates

### 20251126: Creation! 


### 2025112 




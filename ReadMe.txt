agentic_data_ops/
│
├── docker-compose.yml          # Main container orchestrator
│
├── orchestrator/               # CONTAINER 1: Brain of the system (FastAPI + LangGraph)
│   ├── Dockerfile
│   ├── requirements.txt
│   └── src/
│       ├── app.py              # Web server and endpoints for HITL
│       ├── graph.py            # Graph logic and routing
│       └── database.py         # Connection to PostgresSaver
│
├── mcp_worker/                 # CONTAINER 2: Computational arms (MCP + Sandbox)
│   ├── Dockerfile
│   ├── requirements.txt
│   ├── server.py               # MCP server listening for commands over HTTP/JSON-RPC
│   ├── core/                   # Black boxes of business logic
│   │   ├── validate.py
│   │   └── transform.py
│   └── data/                   # Isolated data sandbox
│       └── stage_buffer.csv
│
└── shared_store/               # Shared volume on disk (for RAG and Checkpoints)
    ├── business_rules.json
    ├── architecture_notes.txt
    └── checkpoints.db          # SQLite DB where checkpoints were historically written (if SQLite is used)


-----------------------------------------------------------------------------

# Stop and completely clean old isolated networks
docker-compose down

# Stop and completely clean old volume
docker-compose down -v

docker-compose up --build

-----------------------------------------------------------------------------

docker build -t dataops_init_indexer ./orchestrator/init
docker run --rm --network agentic_data_ops_dataops_network --env-file .env -v $(pwd)/shared_store:/app/store dataops_init_indexer

----------------------------------------------------------------------------- 

curl -X POST "http://localhost:8000/api/v1/pipeline/start" -H "Content-Type: application/json" -d '{"max_retries": 3}'
curl -X POST "http://localhost:8000/api/v1/pipeline/approve" -H "Content-Type: application/json" -d '{"thread_id": "thread_f5d89908", "approved": true, "comment": "Department patch approved"}'

-----------------------------------------------------------------------------




-----------------------------------------------------------------------------
QA with Tree State
1. Run new pipeline to commit the root of the tree:
curl -X POST "http://localhost:8000/api/v1/pipeline/start" -H "Content-Type: application/json" -d '{"max_retries": 3}'

2. Request the state tree (DAG History) using the received thread_id:
curl -X GET "http://localhost:8000/api/v1/pipeline/history/<THREAD_ID>"
(The response will contain an array of nodes with their node_id, parent_id and branch names).

3. Check branching (Time-Travel / Fork), taking the node_id from the previous response:
curl -X POST "http://localhost:8000/api/v1/pipeline/fork" -H "Content-Type: application/json" -d '{ "thread_id": "<THREAD_ID>", "from_node_id": "<NODE_ID>", "new_branch_name": "hypothesis/experimental_fix" }'
Also take a look at Adminer on http://localhost:8000 (or http://localhost:8080) in the state_nodes table — every node's parent_id will be visible there!

-----------------------------------------------------------------------------
If you need to restart something without rollbacks:
docker-compose restart orchestrator

-----------------------------------------------------------------------------



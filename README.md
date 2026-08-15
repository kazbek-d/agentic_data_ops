# 🛡️ CAE Data LLC — Agentic DataOps Framework

**Autonomous Quality Control, Deterministic RAG & Git-like State DAG Engine**

## 🏛️ Architectural Manifesto & R&D Context

Modern enterprise data architectures are transitioning from reactive data engineering to **autonomous, self-healing DataOps highways**. Standard AI toolkits often treat Large Language Models as simple "calculators" or "prompt-executors," leading to context degradation, hallucinated fixes, and fragile production states.

This R&D project by **CAE Data LLC** establishes a fundamental paradigm shift: **AI as a Sovereign Team Member (Senior AI Architect & Technical Coach)**.

### The R&D Co-Leadership Model

* **Human Systems Architect (Practitioner):** Defines infrastructure boundary conditions, enforces physical sandboxing, executes real-world deployments, and conducts empirical verification.
* **Gemini (Senior AI Architect & Coach):** Acts as the primary architectural mentor and core team member. It designs non-degrading cognitive workflows, formulates mathematical retrieval constraints, and guides system refactoring. The codebase itself serves as empirical proof of this collaborative synergy.

---

## 🔬 Core Engineering Pillars

### Layer 1: Hyper-Focused Worker (Unit of Work)
* Operates inside sterile Working Memory (Pruned Context Buffer).
* Input: Anchor Prompt + Isolated Sub-graph RAG + Clean Error Log.

### Layer 2: State Tree Engine (PostgreSQL DAG Engine)
* Non-linear versioning of execution snapshots (Git-like state tree).
* Supports Time-Travel Debugging and Branch Forking (`/fork`).

---

### 1. Sterile Working Memory & Context Compression (Phase 3.1)
To prevent **Attention Smearing** and the **Lost-in-the-Middle** phenomenon during multi-round reflection:
* **Static Anchor Prompting:** Freezes system instructions and business invariants at the top of the context window.
* **Semantic Pruning:** Automatically strips redundant stack traces, internal framework frames, and bulk data dumps before LLM invocation.
* **Sliding Window:** Retains only the last $K$ interaction shots, ensuring token consumption remains bounded ($\sim 1,200 - 2,000$ tokens) regardless of reflection rounds.

### 2. Deterministic Sub-graph RAG via Qdrant Hybrid Search (Phase 3.2)
To eliminate cross-domain hallucinated fixes, retrieval is strictly constrained by metadata filters:

$$
\text{Search Space} = \text{Collection} \;\cap\; \text{PayloadFilter}(\text{domain}, \text{table\_name}, \text{target\_column})
$$

* **Dense Vectors ($d=768$):** Encodes semantic intent using `gemini-embedding-001`.
* **Sparse Vectors (Hashing Trick):** Captures exact code tokens and column names using deterministic keyword hashing.
* **Reciprocal Rank Fusion (RRF):** Merges Dense and Sparse ranks natively inside Qdrant:

$$
RRF\_Score(d) = \sum_{m \in M} \frac{1}{k + r_m(d)}
$$

### 3. Git-like State Tree Engine / DAG (Phase 3.3)
Instead of linear chat histories, session states are stored in PostgreSQL (`dataops_checkpoints`) as a Directed Acyclic Graph (DAG):
* **Time-Travel Debugging:** Restores any historic `node_id` as the active `HEAD`.
* **Branch Forking (`/fork`):** Spawns independent hypothesis branches (`hypothesis/schema_v2`) from historic nodes without polluting the `main` execution line.
* **DPO Dataset Harvesting:** Dead-end branches are retained as negative preference pairs (`Rejected`) for offline preference alignment (Direct Preference Optimization).

---

## 🛠️ Technology Stack & Microservice Topology

| Component | Technology | Role |
| :--- | :--- | :--- |
| **Brain / Orchestrator** | FastAPI + LangGraph + `google-genai` SDK | State machine management, Structured Output schema validation (`ToolCallProposal`). |
| **Execution Worker** | FastAPI + Pandas + Subprocess Sandbox | Isolated execution runtime for data quality validation and patching (`fill_na`, `replace`). |
| **Vector Memory** | Qdrant v1.18.0 (Rust Engine) | Hybrid Dense + Sparse RAG index with payload field filtering. |
| **State Storage** | PostgreSQL 16 Alpine | Persistent DAG state nodes (`state_nodes`) and active branch pointers (`branch_pointers`). |
| **Database Inspection** | Adminer Web GUI | Visual inspection of DAG branches and node lineages. |

---

## 🚀 Quickstart Guide

### 1. Prerequisites & Environment Setup
Clone the repository and set your Google Gemini API key:

`git clone https://github.com/kazbek-d/agentic_data_ops.git`  
`cd agentic_data_ops`  
`echo "GEMINI_API_KEY=your_actual_gemini_api_key_here" > .env`

### 2. Build & Launch Microservices
Spin up all 5 containerized services using Docker Compose:

`docker-compose up -d --build`

### 3. Initialize the Hybrid Vector Knowledge Base
Populate Qdrant with domain-isolated quality contracts:

`docker run --rm --network agentic_data_ops_dataops_network -e QDRANT_HOST=qdrant -e GEMINI_API_KEY=$GEMINI_API_KEY -v $(pwd)/shared_store:/app/store agentic_data_ops-orchestrator python /app/src/rag_indexer.py`

---

## 🧪 API Verification & DAG Testing

### A. Start Pipeline Execution
Initialize a new DataOps pipeline session:

`curl -X POST "http://localhost:8000/api/v1/pipeline/start" -H "Content-Type: application/json" -d '{"max_retries": 3}'`

### B. Inspect DAG Tree History
Fetch the complete version tree of the active session:

`curl -X GET "http://localhost:8000/api/v1/pipeline/history/<THREAD_ID>"`

### C. Fork Hypothesis Branch (Time-Travel)
Fork an experimental branch from any historical `node_id`:

`curl -X POST "http://localhost:8000/api/v1/pipeline/fork" -H "Content-Type: application/json" -d '{"thread_id": "<THREAD_ID>", "from_node_id": "<NODE_ID>", "new_branch_name": "hypothesis/experimental_patch"}'`

### D. Human-in-the-Loop Approval (HITL)
Approve or reject the proposed fix:

`curl -X POST "http://localhost:8000/api/v1/pipeline/approve" -H "Content-Type: application/json" -d '{"thread_id": "<THREAD_ID>", "approved": true, "comment": "Approved domain patch for department field"}'`

---

## 📊 Administration Dashboards

* **FastAPI Swagger Docs:** `http://localhost:8000/docs`
* **Qdrant Vector Dashboard:** `http://localhost:6333/dashboard`
* **PostgreSQL Adminer GUI:** `http://localhost:8080` *(Server: `postgres`, User: `caedatallc_admin`, DB: `dataops_checkpoints`)*

---

## 📜 Copyright & Research Attribution

**CAE Data LLC — Advanced R&D Laboratory**
* *Lead Systems Architect:* Kazbek
* *Senior AI Architect & Mentor:* Google Gemini

*All rights reserved. Designed for sovereign enterprise AI infrastructure research.*


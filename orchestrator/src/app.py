import os
import uuid
from typing import Optional, Dict, Any
from fastapi import FastAPI, HTTPException, status
from pydantic import BaseModel, Field
from fastapi.middleware.cors import CORSMiddleware

# Import the graph singleton and session configuration
from graph import orchestrator_graph, AgentState
# Import the branching state tree manager (DAG Engine)
from dag_state_manager import dag_manager

app = FastAPI(
    title="CAE Data LLC Agentic DataOps Orchestrator API",
    description="Synchronous orchestration API server with branching state tree support (DAG Engine) and Time-Travel Debugging",
    version="3.3.0"
)

# =============================================================================
# CORS CONFIGURATION
# =============================================================================
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# =============================================================================
# PYDANTIC CONTRACTS FOR INCOMING API REQUESTS
# =============================================================================

class PipelineInitRequest(BaseModel):
    max_retries: Optional[int] = Field(default=4, description="AI reflection retry limit")

class HumanDecisionRequest(BaseModel):
    thread_id: str = Field(..., description="Unique execution session ID (Thread ID)")
    approved: bool = Field(..., description="Operator decision: True (approve fix) / False (reject)")
    comment: Optional[str] = Field(default=None, description="Engineer comment on the decision")

class ForkBranchRequest(BaseModel):
    thread_id: str = Field(..., description="Unique session ID (Thread ID)")
    from_node_id: str = Field(..., description="UUID of the historical node from which we branch")
    new_branch_name: str = Field(..., description="Name of the new branch (e.g., hypothesis/schema_v2)")

class PipelineStatusResponse(BaseModel):
    thread_id: str
    pipeline_id: str
    status: str
    retry_count: int
    proposed_fix: Optional[Dict[str, Any]] = None
    execution_logs: list[str]

# =============================================================================
# HELPER FUNCTIONS
# =============================================================================

def get_thread_config(thread_id: str) -> Dict[str, Any]:
    return {"configurable": {"thread_id": thread_id}}

# =============================================================================
# API ENDPOINTS
# =============================================================================

@app.get("/health", status_code=status.HTTP_200_OK)
def health_check():
    return {"status": "healthy", "engine": "FastAPI + LangGraph + DAG State Engine (PostgreSQL)"}


@app.post("/api/v1/pipeline/start", response_model=PipelineStatusResponse, status_code=status.HTTP_201_CREATED)
def start_pipeline(req: PipelineInitRequest):
    """
    Initializes a new pipeline execution flow and commits the DAG tree root
    """
    thread_id = f"thread_{uuid.uuid4().hex[:8]}"
    pipeline_id = f"pl-ops-{uuid.uuid4().hex[:6]}"
    config = get_thread_config(thread_id)
    
    initial_state: AgentState = {
        "pipeline_id": pipeline_id,
        "status": "PENDING",
        "retry_count": 0,
        "max_retries": req.max_retries,
        "current_errors": [],
        "proposed_fix": {},
        "execution_logs": [f"Session initialization {thread_id} for pipeline {pipeline_id}"],
        "raw_llm_response": ""
    }
    
    print(f"🚀 [API] Launching new process with DAG node commit. Thread ID: {thread_id}", flush=True)
    
    orchestrator_graph.graph.invoke(initial_state, config=config)
    
    state_snapshot = orchestrator_graph.graph.get_state(config)
    if not state_snapshot or not state_snapshot.values:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Critical error: graph failed to initialize session in the database."
        )
        
    final_state: AgentState = state_snapshot.values
    
    # 🌳 Commit node to DAG state tree
    dag_manager.save_checkpoint(
        thread_id=thread_id,
        state_data=final_state,
        node_type="CHECKPOINT_PROPOSED",
        branch_name="main"
    )
    
    return PipelineStatusResponse(
        thread_id=thread_id,
        pipeline_id=final_state["pipeline_id"],
        status=final_state["status"],
        retry_count=final_state["retry_count"],
        proposed_fix=final_state.get("proposed_fix"),
        execution_logs=final_state["execution_logs"]
    )


@app.get("/api/v1/pipeline/status/{thread_id}", response_model=PipelineStatusResponse)
def get_pipeline_status(thread_id: str):
    config = get_thread_config(thread_id)
    state_snapshot = orchestrator_graph.graph.get_state(config)
    
    if not state_snapshot or not state_snapshot.values:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Session with Thread ID '{thread_id}' not found"
        )
        
    state: AgentState = state_snapshot.values
    return PipelineStatusResponse(
        thread_id=thread_id,
        pipeline_id=state["pipeline_id"],
        status=state["status"],
        retry_count=state["retry_count"],
        proposed_fix=state.get("proposed_fix"),
        execution_logs=state.get("execution_logs", [])
    )


@app.post("/api/v1/pipeline/approve", response_model=PipelineStatusResponse)
def approve_pipeline_fix(req: HumanDecisionRequest):
    """
    HITL approval committing decisions and transitions to DAG tree
    """
    config = get_thread_config(req.thread_id)
    state_snapshot = orchestrator_graph.graph.get_state(config)
    
    if not state_snapshot or not state_snapshot.values:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Session '{req.thread_id}' not found."
        )
        
    state: AgentState = state_snapshot.values
    
    if state["status"] not in ["VALIDATION_FAILED", "TRANSFORM_FAILED", "FORMAT_ERROR", "PENDING"]:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Session is in final status '{state['status']}'."
        )

    print(f"📥 [API HITL] Operator decision received for {req.thread_id}. Approved: {req.approved}", flush=True)

    if req.approved:
        state["execution_logs"].append(f"[HITL APPROVED] Operator decision: APPROVED. {f'Comment: {req.comment}' if req.comment else ''}")
        orchestrator_graph.graph.update_state(config, {"execution_logs": state["execution_logs"]}, as_node="ai_analyst")
        orchestrator_graph.graph.invoke(None, config=config)
        node_type = "HUMAN_APPROVED"
    else:
        state["execution_logs"].append(f"[HITL REJECTED] Operator decision: REJECTED. {f'Comment: {req.comment}' if req.comment else ''}")
        orchestrator_graph.graph.update_state(
            config, 
            {"status": "REJECTED", "execution_logs": state["execution_logs"]}, 
            as_node="ai_analyst"
        )
        orchestrator_graph.graph.invoke(None, config=config)
        node_type = "HUMAN_REJECTED"

    updated_snapshot = orchestrator_graph.graph.get_state(config)
    final_state: AgentState = updated_snapshot.values

    # 🌳 Commit human decision outcome to DAG tree
    dag_manager.save_checkpoint(
        thread_id=req.thread_id,
        state_data=final_state,
        node_type=node_type
    )

    return PipelineStatusResponse(
        thread_id=req.thread_id,
        pipeline_id=final_state["pipeline_id"],
        status=final_state["status"],
        retry_count=final_state["retry_count"],
        proposed_fix=final_state.get("proposed_fix"),
        execution_logs=final_state["execution_logs"]
    )


# =============================================================================
# 🌳 DAG ENGINE ENDPOINTS: INSPECTION AND BRANCHING (FORK)
# =============================================================================

@app.get("/api/v1/pipeline/history/{thread_id}")
def get_pipeline_dag_history(thread_id: str):
    """
    Returns the entire session graph tree (DAG) from the state_nodes table
    """
    history = dag_manager.get_branch_history(thread_id)
    if not history:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"DAG history for session '{thread_id}' is missing."
        )
    return {"thread_id": thread_id, "nodes_count": len(history), "dag_tree": history}


@app.post("/api/v1/pipeline/fork")
def fork_pipeline_branch(req: ForkBranchRequest):
    """
    Time-Travel / Fork: Creates a new branch from historical node from_node_id
    """
    try:
        branch = dag_manager.fork_branch(
            thread_id=req.thread_id,
            from_node_id=req.from_node_id,
            new_branch_name=req.new_branch_name
        )
        return {
            "success": True,
            "message": f"Successfully created a new branch [{branch}] from node {req.from_node_id[:8]}",
            "thread_id": req.thread_id,
            "active_branch": branch
        }
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Error creating branch: {e}"
        )


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("app:app", host="0.0.0.0", port=8000, reload=True)
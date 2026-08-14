import os
import json
import requests
import hashlib
import re
from collections import Counter
from typing import TypedDict, List, Dict, Any, Literal, Optional
from pydantic import BaseModel, Field, ValidationError

# Official Google SDK
from google import genai
from google.genai import types

# Qdrant client and filters for deterministic RAG
from qdrant_client import QdrantClient, models

# LangGraph components and our database memory manager
from langgraph.graph import StateGraph, END
from database import db_manager

# Sterile Working Memory manager
from context_manager import prune_and_compress_context, format_working_memory_for_llm


# =============================================================================
# 1. STRICT SEMANTIC CONTRACT FOR LLM RESPONSE VALIDATION
# =============================================================================

class ToolCallProposal(BaseModel):
    analysis: str = Field(description="Analysis of the error log and context.")
    explanation: str = Field(description="Engineering rationale for the proposed fix.")
    criticality_assessment: Literal["CRITICAL", "NON_CRITICAL"] = Field(
        description="Assessment of field criticality based on business rules."
    )
    action_type: Literal["fill_na", "replace"] = Field(description="Type of tool to apply.")
    target_column: str = Field(description="Name of the column to repair.")
    patch_value: str = Field(description="Value to substitute (string).")


# =============================================================================
# 2. ENTERPRISE STATE STRUCTURE
# =============================================================================

class AgentState(TypedDict):
    pipeline_id: str
    status: Literal["PENDING", "VALIDATION_FAILED", "TRANSFORM_FAILED", "FORMAT_ERROR", "INFRA_ERROR", "SUCCESS", "REJECTED", "CRITICAL_HALT"]
    retry_count: int
    max_retries: int
    current_errors: List[Dict[str, Any]]
    proposed_fix: Dict[str, Any]
    execution_logs: List[str]
    raw_llm_response: str
    target_table: Optional[str]
    domain: Optional[str]


# =============================================================================
# 3. ORCHESTRATOR CLASS (DETERMINISTIC SUB-GRAPH RAG + WORKING MEMORY)
# =============================================================================

class AgenticDataOpsGraph:
    def __init__(self):
        self.mcp_url = os.environ.get("MCP_WORKER_URL", "http://mcp_worker:5011")
        self.model_id = "gemini-2.5-flash"
        self.embedding_model = "gemini-embedding-001"
        self.collection_name = "cae_business_rules"
        
        qdrant_host = os.environ.get("QDRANT_HOST", "qdrant")
        self.qdrant_client = QdrantClient(
            host=qdrant_host, 
            port=6333,
            check_compatibility=False
        )
        
        self.gemini_client = genai.Client()
        self.checkpointer = db_manager.get_checkpointer()
        self.graph = self._build_workflow()

    def _log(self, state: AgentState, message: str) -> None:
        state["execution_logs"].append(message)
        print(f"👉 [ORCHESTRATOR GRAPH] {message}", flush=True)

    def _generate_sparse_vector(self, text: str) -> models.SparseVector:
        words = re.findall(r"\b[а-яА-ЯёЁa-zA-Z0-9_-]{2,}\b", text.lower())
        word_counts = Counter(words)
        
        indices = []
        values = []
        
        for word, count in word_counts.items():
            h = int(hashlib.md5(word.encode('utf-8')).hexdigest(), 16)
            index = (h % 999_999) + 1
            indices.append(index)
            values.append(float(count))
            
        sorted_pairs = sorted(zip(indices, values))
        sorted_indices = [p[0] for p in sorted_pairs]
        sorted_values = [p[1] for p in sorted_pairs]
        
        return models.SparseVector(indices=sorted_indices, values=sorted_values)


    # --- NODES (SYNCHRONOUS GRAPH NODES) ---

    def node_check_and_transform(self, state: AgentState) -> AgentState:
        if state["status"] == "REJECTED":
            return state

        self._log(state, "Running node: Check and Transform data via HTTP MCP")
        
        # Pin table domain contexts
        state["target_table"] = "salaries_stage"
        state["domain"] = "payroll_ops"

        try:
            res = requests.post(f"{self.mcp_url}/api/v1/validate", timeout=15).json()
        except Exception as e:
            state["status"] = "INFRA_ERROR"
            state["current_errors"] = [{"code": "mcp_unreachable", "message": f"Worker unreachable: {e}"}]
            return state

        if not res["success"]:
            if res.get("stderr"): 
                self._log(state, f"🚨 WORKER INFRASTRUCTURE FAILURE: {res['message']}")
                state["status"] = "INFRA_ERROR"
                state["current_errors"] = [{"code": "worker_infra_error", "stderr": res["stderr"]}]
                return state
            
            self._log(state, "Worker validator detected data structure defects.")
            state["status"] = "VALIDATION_FAILED"
            state["current_errors"] = res["data"]
            return state

        try:
            res = requests.post(f"{self.mcp_url}/api/v1/transform", timeout=15).json()
        except Exception as e:
            state["status"] = "INFRA_ERROR"
            state["current_errors"] = [{"code": "mcp_unreachable", "message": f"Worker unreachable: {e}"}]
            return state

        if not res["success"]:
            self._log(state, "Transformer failed with a runtime error.")
            state["status"] = "TRANSFORM_FAILED"
            state["current_errors"] = [{"code": "runtime_error", "stderr": res["stderr"]}]
            return state

        self._log(state, "[ SUCCESS ] Pipeline successfully executed by worker!")
        state["status"] = "SUCCESS"
        state["current_errors"] = []
        return state

    def node_ai_analyst(self, state: AgentState) -> AgentState:
        self._log(state, f"Running node: AI Analyst (Reflection iteration: {state['retry_count'] + 1})")
        state["retry_count"] += 1
        
        # =============================================================================
        # 1. DETERMINISTIC RAG SEARCH (QDRANT PAYLOAD FILTERING)
        # =============================================================================
        error_context_text = f"Data pipeline failure. Defects and traceback: {json.dumps(state['current_errors'])}"
        rule_text = ""
        
        try:
            # 1.1 Dense vector
            emb_res = self.gemini_client.models.embed_content(
                model=self.embedding_model,
                contents=error_context_text,
                config={"output_dimensionality": 768}
            )
            query_dense_vector = emb_res.embeddings[0].values
            
            # 1.2 Sparse vector
            query_sparse_vector = self._generate_sparse_vector(error_context_text)
            
            # 1.3 Building rigid payload domain filter (Payload Filter)
            target_table = state.get("target_table", "salaries_stage")
            domain = state.get("domain", "payroll_ops")
            
            # If there is a specific column in the error log - narrow the filter down to the column!
            target_col = None
            if state['current_errors'] and isinstance(state['current_errors'], list):
                first_err = state['current_errors'][0]
                target_col = first_err.get("column")

            must_conditions = [
                models.FieldCondition(key="domain", match=models.MatchValue(value=domain)),
                models.FieldCondition(key="table_name", match=models.MatchValue(value=target_table))
            ]
            
            if target_col:
                must_conditions.append(
                    models.FieldCondition(key="target_column", match=models.MatchValue(value=target_col))
                )

            query_filter = models.Filter(must=must_conditions)

            self._log(
                state, 
                f"🛡️ Executing deterministic RAG query in subgraph [{domain} -> {target_table}" + 
                (f" -> {target_col}]" if target_col else "]")
            )
            
            # 1.4 Native hybrid query with deterministic subgraph isolation
            search_results = self.qdrant_client.query_points(
                collection_name=self.collection_name,
                prefetch=[
                    models.Prefetch(query=query_dense_vector, using="dense-meaning", limit=5),
                    models.Prefetch(query=query_sparse_vector, using="sparse-keywords", limit=5)
                ],
                query=models.FusionQuery(fusion=models.Fusion.RRF),
                query_filter=query_filter, # Subgraph isolation at engine level
                limit=1
            ).points
            
            if search_results:
                best_match = search_results[0]
                confidence = round(best_match.score * 100, 2)
                rule_text = best_match.payload.get("rule_text", "")
                self._log(state, f"🎯 Retrieved isolated domain rule with RRF confidence {confidence}%!")
            else:
                self._log(state, "⚠️ Rule not found in this isolated subgraph. Cross-domain rules blocked!")
                rule_text = "Specific domain quality rule is absent in the isolated context."
                
        except Exception as e:
            self._log(state, f"💥 Deterministic RAG failure in Qdrant: {e}")
            state["status"] = "INFRA_ERROR"
            state["current_errors"] = [{"code": "qdrant_deterministic_rag_error", "message": str(e)}]
            return state

        # 2. Getting source code
        code = ""
        try:
            script_type = "transform" if state["status"] in ["TRANSFORM_FAILED", "FORMAT_ERROR"] else "validate"
            code = requests.get(f"{self.mcp_url}/api/v1/code?script_type={script_type}", timeout=5).json().get("data", "")
        except Exception as e:
            self._log(state, f"Failed to get source code from worker: {e}")
            state["status"] = "INFRA_ERROR"
            state["current_errors"] = [{"code": "worker_context_unreachable", "message": str(e)}]
            return state

        # =============================================================================
        # 3. STERILIZATION IN WORKING MEMORY
        # =============================================================================
        anchor_prompt = (
            "You are a Senior Data Support AI Engineer at CAE Data LLC. "
            "You analyze pipeline failures and generate strictly valid data patches."
        )

        rag_rules_list = [rule_text] if rule_text else []

        working_memory_state = prune_and_compress_context(
            raw_error=json.dumps(state['current_errors'], ensure_ascii=False, indent=2),
            history=[],
            rag_rules=rag_rules_list,
            anchor_prompt=anchor_prompt,
            max_window=2
        )

        user_content = format_working_memory_for_llm(working_memory_state)
        
        if code:
            user_content += f"\n\n=== SOURCE CODE ===\n{code}"

        if state["status"] == "FORMAT_ERROR":
            user_content = (
                "⚠️ CRITICAL FORMAT ERROR: In the previous step, your response failed Pydantic validation!\n"
                f"Previous response: {state.get('raw_llm_response', '')}\n\n" + user_content
            )

        # =============================================================================
        # 4. GEMINI SDK CALL WITH STRUCTURED OUTPUTS
        # =============================================================================
        try:
            response = self.gemini_client.models.generate_content(
                model=self.model_id,
                contents=user_content,
                config=types.GenerateContentConfig(
                    system_instruction=anchor_prompt,
                    temperature=0.1,
                    response_mime_type="application/json",
                    response_schema=ToolCallProposal,
                )
            )
            llm_text = response.text
        except Exception as e:
            self._log(state, f"Gemini API direct call failed: {e}")
            state["status"] = "INFRA_ERROR"
            state["current_errors"] = [{"code": "gemini_api_error", "message": str(e)}]
            state["proposed_fix"] = {}
            return state

        state["raw_llm_response"] = llm_text

        try:
            proposal = ToolCallProposal.model_validate_json(llm_text)
            state["proposed_fix"] = proposal.model_dump()
            state["status"] = "PENDING"
            self._log(state, f"Successful format validation for field '{proposal.target_column}'")
        except ValidationError as e:
            self._log(state, "💥 Model response format error!")
            state["status"] = "FORMAT_ERROR"
            state["current_errors"] = e.errors(include_url=False)
            state["proposed_fix"] = {}
            
        return state

    def node_apply_fix(self, state: AgentState) -> AgentState:
        if state["status"] == "REJECTED":
            self._log(state, "Patch rejected by operator.")
            return state

        self._log(state, "apply_fix node: Sending patch to worker...")
        proposal = state["proposed_fix"]
        
        if not proposal or "target_column" not in proposal:
            self._log(state, "🚨 Attempted to apply an empty patch!")
            state["status"] = "INFRA_ERROR"
            return state

        try:
            res = requests.post(
                f"{self.mcp_url}/api/v1/patch",
                json={
                    "column": proposal["target_column"],
                    "action": proposal["action_type"],
                    "value": proposal["patch_value"]
                },
                timeout=10
            ).json()
            self._log(state, res["message"])
        except Exception as e:
            self._log(state, f"Failed to send patch to worker: {e}")
            state["status"] = "INFRA_ERROR"
            return state

        state["status"] = "PENDING"
        return state


    # --- EDGES & ROUTING ---

    def _router(self, state: AgentState) -> str:
        if state["status"] == "SUCCESS":
            return END
        
        if state["status"] == "INFRA_ERROR":
            print("🚨 [GUARDRAIL] System failure. Graph halted.", flush=True)
            state["status"] = "CRITICAL_HALT"
            return END
            
        if state["status"] in ["VALIDATION_FAILED", "TRANSFORM_FAILED", "FORMAT_ERROR"]:
            if state["retry_count"] < state["max_retries"]:
                return "ai_analyst"
            else:
                print("🚨 [CRITICAL] Retry limit exhausted!", flush=True)
                state["status"] = "CRITICAL_HALT"
                return END
                
        if state["status"] == "PENDING":
            return "apply_fix"
            
        if state["status"] == "REJECTED":
            return END
            
        return END

    def _build_workflow(self):
        workflow = StateGraph(AgentState)
        workflow.add_node("check_and_transform", self.node_check_and_transform)
        workflow.add_node("ai_analyst", self.node_ai_analyst)
        workflow.add_node("apply_fix", self.node_apply_fix)

        workflow.set_entry_point("check_and_transform")
        
        workflow.add_conditional_edges("check_and_transform", self._router, {"ai_analyst": "ai_analyst", END: END})
        workflow.add_conditional_edges("ai_analyst", self._router, {"ai_analyst": "ai_analyst", "apply_fix": "apply_fix", END: END})
        workflow.add_edge("apply_fix", "check_and_transform")

        return workflow.compile(checkpointer=self.checkpointer, interrupt_before=["apply_fix"])

orchestrator_graph = AgenticDataOpsGraph()
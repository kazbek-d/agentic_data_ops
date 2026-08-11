import os
import uuid
from typing import Optional, Dict, Any
from fastapi import FastAPI, HTTPException, status
from pydantic import BaseModel, Field
from fastapi.middleware.cors import CORSMiddleware

# Импортируем синглтон графа и конфигурацию сессий
from graph import orchestrator_graph, AgentState
# Импортируем менеджер ветвящегося дерева состояний (DAG Engine)
from dag_state_manager import dag_manager

app = FastAPI(
    title="CAE Data LLC Agentic DataOps Orchestrator API",
    description="Синхронный API-сервер оркестрации с поддержкой ветвящегося дерева состояний (DAG Engine) и Time-Travel Debugging",
    version="3.3.0"
)

# =============================================================================
# НАСТРОЙКА CORS
# =============================================================================
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# =============================================================================
# PYDANTIC КОНТРАКТЫ ДЛЯ ВХОДЯЩИХ ЗАПРОСОВ API
# =============================================================================

class PipelineInitRequest(BaseModel):
    max_retries: Optional[int] = Field(default=4, description="Лимит попыток рефлексии ИИ")

class HumanDecisionRequest(BaseModel):
    thread_id: str = Field(..., description="Уникальный ID сессии выполнения (Thread ID)")
    approved: bool = Field(..., description="Решение оператора: True (одобрить фикс) / False (отклонить)")
    comment: Optional[str] = Field(default=None, description="Комментарий инженера к решению")

class ForkBranchRequest(BaseModel):
    thread_id: str = Field(..., description="Уникальный ID сессии (Thread ID)")
    from_node_id: str = Field(..., description="UUID исторического узла, от которого ответвляемся")
    new_branch_name: str = Field(..., description="Имя новой ветки (например, hypothesis/schema_v2)")

class PipelineStatusResponse(BaseModel):
    thread_id: str
    pipeline_id: str
    status: str
    retry_count: int
    proposed_fix: Optional[Dict[str, Any]] = None
    execution_logs: list[str]

# =============================================================================
# ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ
# =============================================================================

def get_thread_config(thread_id: str) -> Dict[str, Any]:
    return {"configurable": {"thread_id": thread_id}}

# =============================================================================
# ЭНДПОИНТЫ API
# =============================================================================

@app.get("/health", status_code=status.HTTP_200_OK)
def health_check():
    return {"status": "healthy", "engine": "FastAPI + LangGraph + DAG State Engine (PostgreSQL)"}


@app.post("/api/v1/pipeline/start", response_model=PipelineStatusResponse, status_code=status.HTTP_201_CREATED)
def start_pipeline(req: PipelineInitRequest):
    """
    Инициализирует новый поток выполнения пайплайна и фиксирует корень дерева DAG
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
        "execution_logs": [f"Инициализация сессии {thread_id} для пайплайна {pipeline_id}"],
        "raw_llm_response": ""
    }
    
    print(f"🚀 [API] Запуск нового процесса с фиксированием DAG узла. Thread ID: {thread_id}", flush=True)
    
    orchestrator_graph.graph.invoke(initial_state, config=config)
    
    state_snapshot = orchestrator_graph.graph.get_state(config)
    if not state_snapshot or not state_snapshot.values:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Критическая ошибка: граф не смог инициализировать сессию в базе данных."
        )
        
    final_state: AgentState = state_snapshot.values
    
    # 🌳 Записываем узел в дерево состояний DAG
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
            detail=f"Сессия с Thread ID '{thread_id}' не найдена"
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
    HITL согласование с записью решений и переходов в дерево DAG
    """
    config = get_thread_config(req.thread_id)
    state_snapshot = orchestrator_graph.graph.get_state(config)
    
    if not state_snapshot or not state_snapshot.values:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Сессия '{req.thread_id}' не найдена."
        )
        
    state: AgentState = state_snapshot.values
    
    if state["status"] not in ["VALIDATION_FAILED", "TRANSFORM_FAILED", "FORMAT_ERROR", "PENDING"]:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Сессия находится в финальном статусе '{state['status']}'."
        )

    print(f"📥 [API HITL] Получено решение оператора для {req.thread_id}. Решение: {req.approved}", flush=True)

    if req.approved:
        state["execution_logs"].append(f"[HITL APPROVED] Решение оператора: ОДОБРЕНО. {f'Комментарий: {req.comment}' if req.comment else ''}")
        orchestrator_graph.graph.update_state(config, {"execution_logs": state["execution_logs"]}, as_node="ai_analyst")
        orchestrator_graph.graph.invoke(None, config=config)
        node_type = "HUMAN_APPROVED"
    else:
        state["execution_logs"].append(f"[HITL REJECTED] Решение оператора: ОТКЛОНЕНО. {f'Комментарий: {req.comment}' if req.comment else ''}")
        orchestrator_graph.graph.update_state(
            config, 
            {"status": "REJECTED", "execution_logs": state["execution_logs"]}, 
            as_node="ai_analyst"
        )
        orchestrator_graph.graph.invoke(None, config=config)
        node_type = "HUMAN_REJECTED"

    updated_snapshot = orchestrator_graph.graph.get_state(config)
    final_state: AgentState = updated_snapshot.values

    # 🌳 Фиксируем результат решения человека в дереве DAG
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
# 🌳 ЭНДПОИНТЫ DAG ENGINE: ИНСПЕКЦИЯ И ВЕТВЛЕНИЕ (FORK)
# =============================================================================

@app.get("/api/v1/pipeline/history/{thread_id}")
def get_pipeline_dag_history(thread_id: str):
    """
    Возвращает всё дерево-граф сессии (DAG) из таблицы state_nodes
    """
    history = dag_manager.get_branch_history(thread_id)
    if not history:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"История DAG для сессии '{thread_id}' отсутствует."
        )
    return {"thread_id": thread_id, "nodes_count": len(history), "dag_tree": history}


@app.post("/api/v1/pipeline/fork")
def fork_pipeline_branch(req: ForkBranchRequest):
    """
    Time-Travel / Fork: Создает новую ветку от исторического узла from_node_id
    """
    try:
        branch = dag_manager.fork_branch(
            thread_id=req.thread_id,
            from_node_id=req.from_node_id,
            new_branch_name=req.new_branch_name
        )
        return {
            "success": True,
            "message": f"Успешно создана новая ветка [{branch}] от узла {req.from_node_id[:8]}",
            "thread_id": req.thread_id,
            "active_branch": branch
        }
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Ошибка создания ветки: {e}"
        )


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("app:app", host="0.0.0.0", port=8000, reload=True)
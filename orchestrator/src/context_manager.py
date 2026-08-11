import re
import json
from typing import List, Dict, Any, Optional
from pydantic import BaseModel, Field

# =============================================================================
# 1. PYDANTIC МОДЕЛЬ СТЕРИЛЬНОЙ WORKING MEMORY
# =============================================================================

class WorkingMemoryState(BaseModel):
    anchor_prompt: str = Field(description="Замороженные системные правила и бизнес-аксиомы")
    isolated_rag_rules: List[str] = Field(default_factory=list, description="Отфильтрованные правила Qdrant")
    pruned_error_log: str = Field(description="Очищенный от системного шума лог ошибки")
    sliding_history: List[Dict[str, str]] = Field(
        default_factory=list, 
        description="Плавающее окно последних K сообщений (max 2-3)"
    )
    distilled_summary: Optional[str] = Field(default=None, description="Резюме предыдущих нереализованных гипотез")


# =============================================================================
# 2. ФУНКЦИИ СЕМАНТИЧЕСКОЙ ЗАЧИСТКИ (SEMANTIC PRUNING)
# =============================================================================

def remove_system_frames_from_traceback(raw_log: str) -> str:
    """
    Вырезает из Python Traceback стандартные фреймы библиотек (/usr/local/lib/python..., site-packages),
    оставляя только релевантный код пользователя и суть ошибки.
    """
    lines = raw_log.split("\n")
    cleaned_lines = []
    
    skip_next = False
    for line in lines:
        if "site-packages" in line or "/usr/local/lib/python" in line or "/lib/python" in line:
            skip_next = True
            continue
            
        if skip_next and line.startswith("    "):
            continue
            
        skip_next = False
        cleaned_lines.append(line)
        
    return "\n".join(cleaned_lines).strip()


def truncate_data_dumps(raw_log: str, max_lines: int = 12) -> str:
    """
    Если в логе прилетел огромный дамп JSON или таблицы, обрезает его до N строк.
    """
    lines = raw_log.split("\n")
    if len(lines) > max_lines:
        truncated = lines[:max_lines]
        truncated.append(f"\n... [Сжато: вырезано {len(lines) - max_lines} строк системного дампа] ...")
        return "\n".join(truncated)
    return raw_log


# =============================================================================
# 3. ГЛАВНЫЙ АЛГОРИТМ СБОРКИ И СЖАТИЯ КОНТЕКСТА
# =============================================================================

def prune_and_compress_context(
    raw_error: str,
    history: List[Dict[str, str]],
    rag_rules: List[str],
    anchor_prompt: str,
    max_window: int = 2
) -> WorkingMemoryState:
    """
    Принимает сырой контекст сессии и возвращает стерильную Working Memory.
    """
    cleaned_error = remove_system_frames_from_traceback(raw_error)
    cleaned_error = truncate_data_dumps(cleaned_error)
    
    recent_history = history[-max_window:] if len(history) > max_window else history
    
    distilled_summary = None
    if len(history) > max_window:
        prior_attempts = history[:-max_window]
        summary_items = []
        for idx, item in enumerate(prior_attempts, 1):
            if "proposed_fix" in item:
                fix = item.get("proposed_fix", {})
                col = fix.get("target_column", "unknown")
                act = fix.get("action_type", "unknown")
                summary_items.append(f"Попытка {idx}: Пробовали {act} для {col} (не решило проблему).")
        
        if summary_items:
            distilled_summary = "Ранее не сработавшие гипотезы:\n" + "\n".join(summary_items)

    return WorkingMemoryState(
        anchor_prompt=anchor_prompt,
        isolated_rag_rules=rag_rules,
        pruned_error_log=cleaned_error,
        sliding_history=recent_history,
        distilled_summary=distilled_summary
    )


def format_working_memory_for_llm(memory: WorkingMemoryState) -> str:
    """
    Преобразует WorkingMemoryState в финальный стерильный промпт для LLM.
    """
    prompt_parts = [
        f"=== ANCHOR INSTRUCTIONS (FROZEN) ===\n{memory.anchor_prompt}\n",
    ]
    
    if memory.isolated_rag_rules:
        rules_str = "\n".join([f"- {r}" for r in memory.isolated_rag_rules])
        prompt_parts.append(f"=== BUSINESS RULES (FILTERED RAG) ===\n{rules_str}\n")
        
    if memory.distilled_summary:
        prompt_parts.append(f"=== PRIOR ATTEMPTS SUMMARY ===\n{memory.distilled_summary}\n")
        
    if memory.sliding_history:
        history_str = json.dumps(memory.sliding_history, ensure_ascii=False, indent=2)
        prompt_parts.append(f"=== RECENT INTERACTION WINDOW (LAST SHOTS) ===\n{history_str}\n")
        
    prompt_parts.append(f"=== PRUNED ERROR PAYLOAD ===\n{memory.pruned_error_log}\n")
    prompt_parts.append("Проанализируй проблему и предложи строго валидный патч в JSON формате.")
    
    return "\n".join(prompt_parts)
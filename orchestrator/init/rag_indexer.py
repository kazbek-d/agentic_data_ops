import os
import json
import hashlib
import re
from collections import Counter
from google import genai
from qdrant_client import QdrantClient, models
from qdrant_client.models import Distance, PointStruct

# Настройки подключения к Qdrant во внутренней сети Docker
QDRANT_HOST = os.environ.get("QDRANT_HOST", "qdrant")
QDRANT_PORT = int(os.environ.get("QDRANT_PORT", 6333))

qdrant_client = QdrantClient(host=QDRANT_HOST, port=QDRANT_PORT)
gemini_client = genai.Client()

COLLECTION_NAME = "cae_business_rules"
EMBEDDING_MODEL = "gemini-embedding-001"

def init_vector_db():
    """
    Инициализирует гибридную коллекцию в Qdrant с индексацией полей метаданных
    для супербыстрой детерминированной фильтрации подграфов.
    """
    print(f"📡 Подключение к Qdrant на {QDRANT_HOST}:{QDRANT_PORT}...")
    
    try:
        collections = qdrant_client.get_collections().collections
        exists = any(c.name == COLLECTION_NAME for c in collections)
    except Exception:
        exists = False
    
    if exists:
        print(f"🧹 Пересоздание коллекции '{COLLECTION_NAME}'...")
        qdrant_client.delete_collection(COLLECTION_NAME)
        
    qdrant_client.create_collection(
        collection_name=COLLECTION_NAME,
        vectors_config={
            "dense-meaning": models.VectorParams(size=768, distance=Distance.COSINE)
        },
        sparse_vectors_config={
            "sparse-keywords": models.SparseVectorParams()
        }
    )
    
    # Создаем детерминированные индексы полезной нагрузки для изоляции доменов
    qdrant_client.create_payload_index(
        collection_name=COLLECTION_NAME,
        field_name="target_column",
        field_schema=models.PayloadSchemaType.KEYWORD,
    )
    qdrant_client.create_payload_index(
        collection_name=COLLECTION_NAME,
        field_name="domain",
        field_schema=models.PayloadSchemaType.KEYWORD,
    )
    qdrant_client.create_payload_index(
        collection_name=COLLECTION_NAME,
        field_name="table_name",
        field_schema=models.PayloadSchemaType.KEYWORD,
    )
    print(f"✨ Коллекция '{COLLECTION_NAME}' с индексами метаданных создана!")

def generate_dense_embedding(text: str) -> list[float]:
    response = gemini_client.models.embed_content(
        model=EMBEDDING_MODEL,
        contents=text,
        config={"output_dimensionality": 768}
    )
    return response.embeddings[0].values

def generate_sparse_vector(text: str) -> models.SparseVector:
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

def index_business_rules():
    """Считывает контракты, обогащает доменными метаданными и индексирует в Qdrant"""
    rules_path = "/app/store/business_rules.json"
    if not os.path.exists(rules_path):
        # Резервный локальный путь для тестов вне докера
        rules_path = "shared_store/business_rules.json"

    if not os.path.exists(rules_path):
        print(f"🚨 Ошибка: файл бизнес-правил не найден: {rules_path}")
        return

    with open(rules_path, "r", encoding="utf-8") as f:
        rules = json.load(f)

    points = []
    for idx, (column, rule_details) in enumerate(rules.items()):
        domain = rule_details.get("domain", "payroll_ops")
        table_name = rule_details.get("table_name", "salaries_stage")
        schema_version = rule_details.get("schema_version", "v1.0")

        rule_text = (
            f"Правило качества данных для колонки '{column}' в таблице '{table_name}' (Домен: {domain}, Версия: {schema_version}). "
            f"Тип данных: {rule_details.get('type')}. "
            f"Уровень критичности: {rule_details.get('criticality')}. "
            f"Разрешены ли пустые значения: {rule_details.get('allow_null')}. "
            f"Стратегия восстановления: {rule_details.get('fallback_action')}. "
            f"Значение по умолчанию: {rule_details.get('default_value')}."
        )
        
        print(f"🧠 [Dense Embed] Кодирование правила для {table_name}.{column}...")
        dense_vector = generate_dense_embedding(rule_text)
        
        print(f"🔍 [Sparse Hash] Генерация разреженного вектора для {table_name}.{column}...")
        sparse_vector = generate_sparse_vector(rule_text)
        
        point = PointStruct(
            id=idx,
            vector={
                "dense-meaning": dense_vector,
                "sparse-keywords": sparse_vector
            },
            payload={
                "tenant_id": "cae_data_llc",
                "domain": domain,
                "table_name": table_name,
                "schema_version": schema_version,
                "target_column": column,
                "criticality": rule_details.get("criticality"),
                "rule_text": rule_text,
                "rule_details": rule_details
            }
        )
        points.append(point)

    qdrant_client.upsert(
        collection_name=COLLECTION_NAME,
        points=points
    )
    print(f"🚀 Успешно загружено {len(points)} векторов с изолированными метаданными доменов!")

if __name__ == "__main__":
    init_vector_db()
    index_business_rules()
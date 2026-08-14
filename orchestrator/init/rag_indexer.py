import os
import json
import hashlib
import re
from collections import Counter
from google import genai
from qdrant_client import QdrantClient, models
from qdrant_client.models import Distance, PointStruct

# Qdrant connection settings inside the internal Docker network
QDRANT_HOST = os.environ.get("QDRANT_HOST", "qdrant")
QDRANT_PORT = int(os.environ.get("QDRANT_PORT", 6333))

qdrant_client = QdrantClient(host=QDRANT_HOST, port=QDRANT_PORT)
gemini_client = genai.Client()

COLLECTION_NAME = "cae_business_rules"
EMBEDDING_MODEL = "gemini-embedding-001"

def init_vector_db():
    """
    Initializes a hybrid collection in Qdrant with metadata fields indexing
    for super-fast deterministic subgraph filtering.
    """
    print(f"📡 Connecting to Qdrant at {QDRANT_HOST}:{QDRANT_PORT}...")
    
    try:
        collections = qdrant_client.get_collections().collections
        exists = any(c.name == COLLECTION_NAME for c in collections)
    except Exception:
        exists = False
    
    if exists:
        print(f"🧹 Recreating collection '{COLLECTION_NAME}'...")
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
    
    # Create deterministic payload indexes for domain isolation
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
    print(f"✨ Collection '{COLLECTION_NAME}' with metadata indexes created!")

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
    """Reads contracts, enriches with domain metadata, and indexes in Qdrant"""
    rules_path = "/app/store/business_rules.json"
    if not os.path.exists(rules_path):
        # Fallback local path for testing outside Docker
        rules_path = "shared_store/business_rules.json"

    if not os.path.exists(rules_path):
        print(f"🚨 Error: business rules file not found: {rules_path}")
        return

    with open(rules_path, "r", encoding="utf-8") as f:
        rules = json.load(f)

    points = []
    for idx, (column, rule_details) in enumerate(rules.items()):
        domain = rule_details.get("domain", "payroll_ops")
        table_name = rule_details.get("table_name", "salaries_stage")
        schema_version = rule_details.get("schema_version", "v1.0")

        rule_text = (
            f"Data quality rule for column '{column}' in table '{table_name}' (Domain: {domain}, Version: {schema_version}). "
            f"Data type: {rule_details.get('type')}. "
            f"Criticality level: {rule_details.get('criticality')}. "
            f"Are null values allowed: {rule_details.get('allow_null')}. "
            f"Fallback action strategy: {rule_details.get('fallback_action')}. "
            f"Default value: {rule_details.get('default_value')}."
        )
        
        print(f"🧠 [Dense Embed] Encoding rule for {table_name}.{column}...")
        dense_vector = generate_dense_embedding(rule_text)
        
        print(f"🔍 [Sparse Hash] Generating sparse vector for {table_name}.{column}...")
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
    print(f"🚀 Successfully uploaded {len(points)} vectors with isolated domain metadata!")

if __name__ == "__main__":
    init_vector_db()
    index_business_rules()

agentic_data_ops/
│
├── docker-compose.yml          # Главный оркестратор контейнеров
│
├── orchestrator/               # КОНТЕЙНЕР 1: Мозг системы (FastAPI + LangGraph)
│   ├── Dockerfile
│   ├── requirements.txt
│   └── src/
│       ├── app.py              # Веб-сервер и эндпоинты для HITL
│       ├── graph.py            # Логика графа и роутинг
│       └── database.py         # Подключение к SQLiteSaver
│
├── mcp_worker/                 # КОНТЕЙНЕР 2: Вычислительные руки (MCP + Песочница)
│   ├── Dockerfile
│   ├── requirements.txt
│   ├── server.py               # MCP-сервер, слушающий команды по HTTP/JSON-RPC
│   ├── core/                   # Черные ящики бизнес-логики
│   │   ├── validate.py
│   │   └── transform.py
│   └── data/                   # Изолированная песочница данных
│       └── stage_buffer.csv
│
└── shared_store/               # Общий Volume на диске (для RAG и Чекпоинтов)
    ├── business_rules.json
    ├── architecture_notes.txt
    └── checkpoints.db          # Сюда SQLite будет намертво бессмертно писать стейт


-----------------------------------------------------------------------------

# Останавливаем и полностью вычищаем старые изолированные сети
docker-compose down

# Останавливаем и полностью вычищаем старый вольюм
docker-compose down -v

docker-compose up --build

-----------------------------------------------------------------------------

docker build -t dataops_init_indexer ./orchestrator/init
docker run --rm --network agentic_data_ops_dataops_network --env-file .env -v $(pwd)/shared_store:/app/store dataops_init_indexer

----------------------------------------------------------------------------- 

curl -X POST "http://localhost:8000/api/v1/pipeline/start" -H "Content-Type: application/json" -d '{"max_retries": 3}'
curl -X POST "http://localhost:8000/api/v1/pipeline/approve" -H "Content-Type: application/json" -d '{"thread_id": "thread_f5d89908", "approved": true, "comment": "Патч для департамента одобрен"}'

-----------------------------------------------------------------------------




-----------------------------------------------------------------------------
QA with Tree State
1. Запусти новый пайплайн, чтобы зафиксировать корень дерева:
curl -X POST "http://localhost:8000/api/v1/pipeline/start" -H "Content-Type: application/json" -d '{"max_retries": 3}'

2. Запроси дерево состояний (DAG History) по полученному thread_id:
curl -X GET "http://localhost:8000/api/v1/pipeline/history/<THREAD_ID>"
(В ответе прилетит массив узлов с их node_id, parent_id и именами веток).

3. Проверь ответвление (Time-Travel / Fork), взяв node_id из предыдущего ответа:
curl -X POST "http://localhost:8000/api/v1/pipeline/fork" -H "Content-Type: application/json" -d '{ "thread_id": "<THREAD_ID>", "from_node_id": "<NODE_ID>", "new_branch_name": "hypothesis/experimental_fix" }'
А также загляни в Adminer на http://localhost:8000 (или http://localhost:8080) в таблицу state_nodes — там у каждого узла будет виден его родитель parent_id!

-----------------------------------------------------------------------------
Если нужно что-то перезапустить без отката:
docker-compose restart orchestrator

-----------------------------------------------------------------------------



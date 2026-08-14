import os
import uuid
import json
from typing import Dict, Any, Optional, List
from psycopg_pool import ConnectionPool

DB_USER = os.environ.get("POSTGRES_USER", "caedatallc_admin")
DB_PASSWORD = os.environ.get("POSTGRES_PASSWORD", "SecretPassword2026")
DB_HOST = os.environ.get("POSTGRES_HOST", "postgres")
DB_PORT = os.environ.get("POSTGRES_PORT", "5432")
DB_NAME = os.environ.get("POSTGRES_DB", "dataops_checkpoints")

DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

class DAGStateManager:
    def __init__(self):
        print(f"🌳 [DAG ENGINE] Connecting to PostgreSQL state tree...", flush=True)
        self.pool = ConnectionPool(
            conninfo=DATABASE_URL,
            max_size=10,
            kwargs={"autocommit": True}
        )
        self._init_tables()

    def _init_tables(self):
        """Creates tables for branching state tree (DAG) if they don't exist"""
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS state_nodes (
                        node_id UUID PRIMARY KEY,
                        thread_id TEXT NOT NULL,
                        parent_id UUID REFERENCES state_nodes(node_id) ON DELETE SET NULL,
                        branch_name TEXT NOT NULL,
                        node_type TEXT NOT NULL,
                        state_data JSONB NOT NULL,
                        created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
                    );
                    
                    CREATE TABLE IF NOT EXISTS branch_pointers (
                        thread_id TEXT PRIMARY KEY,
                        active_branch TEXT NOT NULL,
                        head_node_id UUID REFERENCES state_nodes(node_id)
                    );
                    
                    CREATE INDEX IF NOT EXISTS idx_state_nodes_thread ON state_nodes(thread_id);
                    CREATE INDEX IF NOT EXISTS idx_state_nodes_parent ON state_nodes(parent_id);
                """)
        print("✨ [DAG ENGINE] State tree and branch indices are ready!", flush=True)

    def save_checkpoint(
        self, 
        thread_id: str, 
        state_data: Dict[str, Any], 
        node_type: str = "CHECKPOINT",
        branch_name: Optional[str] = None
    ) -> str:
        """
        Saves a new state node as a child of the current HEAD.
        """
        node_id = str(uuid.uuid4())
        
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                # 1. Get the current HEAD pointer
                cur.execute("SELECT active_branch, head_node_id FROM branch_pointers WHERE thread_id = %s", (thread_id,))
                row = cur.fetchone()
                
                if row:
                    current_branch, current_head_id = row[0], str(row[1]) if row[1] else None
                else:
                    current_branch, current_head_id = "main", None

                target_branch = branch_name or current_branch

                # 2. Insert new node
                cur.execute("""
                    INSERT INTO state_nodes (node_id, thread_id, parent_id, branch_name, node_type, state_data)
                    VALUES (%s, %s, %s, %s, %s, %s)
                """, (node_id, thread_id, current_head_id, target_branch, node_type, json.dumps(state_data)))

                # 3. Update the HEAD pointer
                cur.execute("""
                    INSERT INTO branch_pointers (thread_id, active_branch, head_node_id)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (thread_id) 
                    DO UPDATE SET active_branch = EXCLUDED.active_branch, head_node_id = EXCLUDED.head_node_id
                """, (thread_id, target_branch, node_id))

        print(f"🌳 [DAG CHECKPOINT] Node {node_id[:8]} written to branch [{target_branch}] (Parent: {str(current_head_id)[:8] if current_head_id else 'ROOT'})", flush=True)
        return node_id

    def fork_branch(self, thread_id: str, from_node_id: str, new_branch_name: str) -> str:
        """
        Forks a new branch from a historical node from_node_id.
        """
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT state_data FROM state_nodes WHERE node_id = %s", (from_node_id,))
                row = cur.fetchone()
                if not row:
                    raise ValueError(f"Node {from_node_id} not found in state tree!")
                
                # Move active_branch and head_node_id to the source node for branching
                cur.execute("""
                    UPDATE branch_pointers 
                    SET active_branch = %s, head_node_id = %s 
                    WHERE thread_id = %s
                """, (new_branch_name, from_node_id, thread_id))

        print(f"🔀 [DAG FORK] Created a new branch [{new_branch_name}] from node {str(from_node_id)[:8]}", flush=True)
        return new_branch_name

    def get_branch_history(self, thread_id: str) -> List[Dict[str, Any]]:
        """
        Returns the entire session tree graph for GUI visualization or DPO distillation.
        """
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT node_id, parent_id, branch_name, node_type, created_at
                    FROM state_nodes 
                    WHERE thread_id = %s 
                    ORDER BY created_at ASC
                """, (thread_id,))
                rows = cur.fetchall()
                
                return [
                    {
                        "node_id": str(r[0]),
                        "parent_id": str(r[1]) if r[1] else None,
                        "branch_name": r[2],
                        "node_type": r[3],
                        "created_at": r[4].isoformat()
                    }
                    for r in rows
                ]

# DAG manager singleton
dag_manager = DAGStateManager()
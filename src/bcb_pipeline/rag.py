"""Última camada: documentos do dbt viram arquivos Markdown e são indexados no pgvector.

A indexação usa o próprio assistente (repositório personal-ai-assistant-rag-mcp), chamado como
um programa à parte: os dois projetos só combinam o **formato** (pasta de Markdown + tabelas
rag_* no Postgres), sem um importar o código do outro.

As duas etapas são incrementais:
- exportação: só reescreve o arquivo cujo conteúdo mudou (hash);
- indexação: o assistente compara o hash de cada documento e só gera embeddings do que mudou.
  Embedding custa chamada de API; reindexar tudo a cada dia seria desperdício.
"""

from __future__ import annotations

import hashlib
import os
import re
import subprocess
from dataclasses import dataclass
from pathlib import Path

import psycopg

ROOT = Path(__file__).resolve().parents[2]
DEFAULT_EXPORT_DIR = ROOT / "exports" / "rag"
DEFAULT_ASSISTANT_DIR = ROOT.parent / "personal-ai-assistant-rag-mcp"


@dataclass
class ExportStats:
    total: int = 0
    written: int = 0
    unchanged: int = 0
    removed: int = 0


def _md5(text: str) -> str:
    return hashlib.md5(text.encode("utf-8")).hexdigest()


def export_documents(conn: psycopg.Connection, out_dir: Path = DEFAULT_EXPORT_DIR) -> ExportStats:
    """Grava `marts.rag_documentos` como `<doc_id>.md`. Arquivo igual não é reescrito; arquivo
    que não existe mais na tabela é apagado da pasta."""
    stats = ExportStats()
    rows = conn.execute(
        "SELECT doc_id, conteudo, content_hash FROM marts.rag_documentos ORDER BY doc_id"
    ).fetchall()
    wanted: set[Path] = set()
    for doc_id, content, content_hash in rows:
        path = out_dir / f"{doc_id}.md"
        wanted.add(path)
        stats.total += 1
        if path.exists() and _md5(path.read_text(encoding="utf-8")) == content_hash:
            stats.unchanged += 1
            continue
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content, encoding="utf-8")
        stats.written += 1
    for old in out_dir.rglob("*.md") if out_dir.exists() else []:
        if old not in wanted:
            old.unlink()
            stats.removed += 1
    return stats


INGEST_LINE = re.compile(
    r"Arquivos: (?P<files>\d+) \| indexados: (?P<indexed>\d+) \| sem mudança: (?P<unchanged>\d+)"
    r" \| chunks novos: (?P<chunks>\d+) \| total de chunks no banco: (?P<total_chunks>\d+)"
)


def index_documents(
    export_dir: Path = DEFAULT_EXPORT_DIR,
    assistant_dir: Path | None = None,
    database_url: str | None = None,
    embedding_provider: str | None = None,
) -> dict[str, int]:
    """Roda `python -m assistant ingest <pasta>` do assistente, gravando no Postgres do pipeline.

    Embedder: `RAG_EMBEDDING_PROVIDER` (padrão `hash`, local, sem chave e sem custo; `gemini` usa
    `GEMINI_API_KEY`). O assistente recusa misturar embedders no mesmo banco.
    """
    assistant_dir = Path(assistant_dir or os.getenv("ASSISTANT_DIR", DEFAULT_ASSISTANT_DIR))
    python = assistant_dir / ".venv" / "bin" / "python"
    if not python.exists():
        raise FileNotFoundError(f"ambiente do assistente não encontrado: {python}")
    env = {
        **os.environ,
        "VECTOR_STORE": "pgvector",
        "DATABASE_URL": database_url or os.environ["DATABASE_URL"],
        "EMBEDDING_PROVIDER": embedding_provider or os.getenv("RAG_EMBEDDING_PROVIDER", "hash"),
        "PYTHONPATH": str(assistant_dir / "src"),
        # Título do documento (com a data por extenso) no início de cada chunk. Sem isso, os
        # 237 comunicados, quase iguais no vocabulário, ficavam indistinguíveis na busca.
        "CHUNK_TITLE_PREFIX": os.getenv("CHUNK_TITLE_PREFIX", "true"),
    }
    proc = subprocess.run(
        [str(python), "-m", "assistant", "ingest", str(export_dir)],
        cwd=assistant_dir,
        env=env,
        capture_output=True,
        text=True,
        timeout=3600,
    )
    if proc.returncode != 0:
        raise RuntimeError(f"ingestão do assistente falhou:\n{proc.stderr[-2000:]}")
    match = INGEST_LINE.search(proc.stdout)
    if not match:
        raise RuntimeError(f"saída inesperada do assistente: {proc.stdout[-500:]}")
    return {k: int(v) for k, v in match.groupdict().items()}

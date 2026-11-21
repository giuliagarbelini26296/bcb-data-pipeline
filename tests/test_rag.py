"""Exportação incremental dos documentos e leitura da saída do assistente."""

import hashlib
from pathlib import Path

import pytest

from bcb_pipeline.rag import INGEST_LINE, export_documents, index_documents


@pytest.fixture
def docs(conn):
    conn.execute("CREATE SCHEMA IF NOT EXISTS marts")
    conn.execute("DROP TABLE IF EXISTS marts.rag_documentos")
    conn.execute(
        "CREATE TABLE marts.rag_documentos (doc_id text, conteudo text, content_hash text)"
    )
    conn.commit()

    def put(rows: dict[str, str]):
        conn.execute("DELETE FROM marts.rag_documentos")
        for doc_id, text in rows.items():
            conn.execute(
                "INSERT INTO marts.rag_documentos VALUES (%s, %s, %s)",
                (doc_id, text, hashlib.md5(text.encode()).hexdigest()),
            )
        conn.commit()

    yield put
    conn.execute("DROP SCHEMA marts CASCADE")
    conn.commit()


def test_export_writes_only_changed_and_removes_stale(conn, docs, tmp_path: Path):
    docs({"copom/a": "um", "copom/b": "dois"})
    first = export_documents(conn, tmp_path)
    assert (first.written, first.unchanged) == (2, 0)
    assert (tmp_path / "copom" / "a.md").read_text() == "um"

    second = export_documents(conn, tmp_path)
    assert (second.written, second.unchanged) == (0, 2)

    docs({"copom/a": "um (corrigido)"})  # b saiu da tabela, a mudou
    third = export_documents(conn, tmp_path)
    assert (third.written, third.unchanged, third.removed) == (1, 0, 1)
    assert not (tmp_path / "copom" / "b.md").exists()


def test_parses_assistant_output():
    out = (
        "Arquivos: 264 | indexados: 1 | sem mudança: 263 | chunks novos: 4 | "
        "total de chunks no banco: 1105"
    )
    stats = {k: int(v) for k, v in INGEST_LINE.search(out).groupdict().items()}
    assert stats == {
        "files": 264,
        "indexed": 1,
        "unchanged": 263,
        "chunks": 4,
        "total_chunks": 1105,
    }


def test_missing_assistant_env_is_a_clear_error(tmp_path: Path):
    with pytest.raises(FileNotFoundError, match="ambiente do assistente"):
        index_documents(tmp_path, assistant_dir=tmp_path, database_url="postgresql://x")

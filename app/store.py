"""SQLite persistence for the harvester.

The published catalogue and the harvest watermark live in the same SQLite
database.  A harvest round writes into ``run_records`` (a per-run staging
area) and only the final commit transaction copies staging into ``catalog``
and advances the watermark.  This is what makes a failed or interrupted
round a no-op for readers of the published catalogue.
"""

from __future__ import annotations

import sqlite3
import threading
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Sequence

from .duplicates import canonical_identifiers, grouping_key


def utc_now_millis() -> str:
    return (
        datetime.now(timezone.utc)
        .isoformat(timespec="milliseconds")
        .replace("+00:00", "Z")
    )


class Store:
    def __init__(self, db_path: str | Path) -> None:
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        self._conn = sqlite3.connect(
            str(self.db_path), check_same_thread=False, isolation_level=None
        )
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA synchronous=FULL")
        self._conn.execute("PRAGMA foreign_keys=ON")
        self._conn.execute("PRAGMA busy_timeout=5000")
        self._init_schema()
        self._recover_interrupted_runs()
        self._ensure_invariants()

    # ------------------------------------------------------------------ schema
    def _init_schema(self) -> None:
        with self._lock:
            self._conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS meta (
                    key TEXT PRIMARY KEY,
                    value TEXT
                );

                CREATE TABLE IF NOT EXISTS published_state (
                    id INTEGER PRIMARY KEY CHECK (id = 1),
                    watermark TEXT,
                    updated_at TEXT,
                    success_count INTEGER NOT NULL DEFAULT 0,
                    last_run_id TEXT
                );

                CREATE TABLE IF NOT EXISTS runs (
                    id TEXT PRIMARY KEY,
                    status TEXT NOT NULL CHECK (
                        status IN ('running', 'succeeded', 'failed', 'cancelled')
                    ),
                    started_at TEXT NOT NULL,
                    finished_at TEXT,
                    window_from TEXT,
                    window_until TEXT,
                    pages_fetched INTEGER NOT NULL DEFAULT 0,
                    records_seen INTEGER NOT NULL DEFAULT 0,
                    token_restarts INTEGER NOT NULL DEFAULT 0,
                    attempt INTEGER NOT NULL DEFAULT 1,
                    complete_list_size INTEGER,
                    phase TEXT,
                    last_error TEXT,
                    cancel_reason TEXT,
                    watermark_after TEXT
                );

                CREATE TABLE IF NOT EXISTS run_records (
                    run_id TEXT NOT NULL,
                    identifier TEXT NOT NULL,
                    title TEXT,
                    creator TEXT,
                    datestamp TEXT NOT NULL,
                    deleted INTEGER NOT NULL DEFAULT 0,
                    seq INTEGER NOT NULL,
                    PRIMARY KEY (run_id, identifier),
                    FOREIGN KEY (run_id) REFERENCES runs(id) ON DELETE CASCADE
                );

                CREATE TABLE IF NOT EXISTS catalog (
                    identifier TEXT PRIMARY KEY,
                    title TEXT NOT NULL,
                    creator TEXT,
                    source_datestamp TEXT NOT NULL,
                    harvested_at TEXT NOT NULL,
                    content_token TEXT
                );

                CREATE INDEX IF NOT EXISTS idx_catalog_title ON catalog(title);

                CREATE TABLE IF NOT EXISTS duplicate_judgements (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    left_identifier TEXT NOT NULL,
                    right_identifier TEXT NOT NULL,
                    left_token TEXT NOT NULL,
                    right_token TEXT NOT NULL,
                    verdict TEXT NOT NULL CHECK (verdict IN ('same', 'different')),
                    created_at TEXT NOT NULL,
                    CHECK (left_identifier < right_identifier)
                );

                CREATE INDEX IF NOT EXISTS idx_duplicate_pair
                    ON duplicate_judgements(left_identifier, right_identifier, id);

                INSERT OR IGNORE INTO published_state
                    (id, watermark, updated_at, success_count, last_run_id)
                VALUES (1, NULL, NULL, 0, NULL);
                """
            )
            self._migrate_catalog_tokens()

    def _migrate_catalog_tokens(self) -> None:
        """Add the content token to catalogues created before duplicates existed.

        Every existing row gets a fresh token; from then on a token only
        changes when the published title or creator actually changes.  Tokens
        are never reused, so a withdrawn-then-restored record can never make an
        old judgement valid again.
        """
        columns = {
            row["name"]
            for row in self._conn.execute("PRAGMA table_info(catalog)")
        }
        if "content_token" not in columns:
            self._conn.execute("ALTER TABLE catalog ADD COLUMN content_token TEXT")
        missing = self._conn.execute(
            "SELECT identifier FROM catalog "
            "WHERE content_token IS NULL OR content_token = ''"
        ).fetchall()
        for row in missing:
            self._conn.execute(
                "UPDATE catalog SET content_token = ? WHERE identifier = ?",
                (uuid.uuid4().hex, row["identifier"]),
            )

    def _recover_interrupted_runs(self) -> None:
        """A previous process died.  No in-memory task can own these runs."""
        with self._lock:
            now = utc_now_millis()
            rows = self._conn.execute(
                "SELECT id FROM runs WHERE status = 'running'"
            ).fetchall()
            for row in rows:
                self._conn.execute(
                    """
                    UPDATE runs
                       SET status = 'failed',
                           finished_at = ?,
                           phase = 'failed',
                           last_error = COALESCE(
                               last_error,
                               '采集进程中断，本轮未提交（已发布目录与水位保持不变）'
                           )
                     WHERE id = ? AND status = 'running'
                    """,
                    (now, row["id"]),
                )
                self._conn.execute(
                    "DELETE FROM run_records WHERE run_id = ?", (row["id"],)
                )
            self._conn.execute(
                "DELETE FROM meta WHERE key = 'active_run_id'"
            )

    def _ensure_invariants(self) -> None:
        """At most one round may be ``running``, even across processes.

        Created after recovery so a legacy database with stray ``running``
        rows cannot make startup fail.
        """
        with self._lock:
            self._conn.execute(
                """
                CREATE UNIQUE INDEX IF NOT EXISTS idx_runs_one_active
                    ON runs(status) WHERE status = 'running'
                """
            )

    # ----------------------------------------------------------------- helpers
    def _meta_get(self, key: str) -> str | None:
        row = self._conn.execute(
            "SELECT value FROM meta WHERE key = ?", (key,)
        ).fetchone()
        return row["value"] if row else None

    def _meta_set(self, key: str, value: str | None) -> None:
        if value is None:
            self._conn.execute("DELETE FROM meta WHERE key = ?", (key,))
        else:
            self._conn.execute(
                """
                INSERT INTO meta(key, value) VALUES(?, ?)
                ON CONFLICT(key) DO UPDATE SET value = excluded.value
                """,
                (key, value),
            )

    # -------------------------------------------------------------------- runs
    def create_run(self) -> dict[str, Any]:
        with self._lock:
            run_id = uuid.uuid4().hex
            now = utc_now_millis()
            state = self._conn.execute(
                "SELECT watermark FROM published_state WHERE id = 1"
            ).fetchone()
            watermark = state["watermark"] if state else None
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                self._conn.execute(
                    """
                    INSERT INTO runs
                        (id, status, started_at, window_from, phase)
                    VALUES (?, 'running', ?, ?, 'connecting')
                    """,
                    (run_id, now, watermark),
                )
                self._meta_set("active_run_id", run_id)
                self._conn.execute("COMMIT")
            except sqlite3.IntegrityError as exc:
                self._conn.execute("ROLLBACK")
                raise RuntimeError("已有采集轮次在运行") from exc
            except BaseException:
                self._conn.execute("ROLLBACK")
                raise
            run = self.get_run(run_id)
            assert run is not None
            return run

    def get_run(self, run_id: str) -> dict[str, Any] | None:
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM runs WHERE id = ?", (run_id,)
            ).fetchone()
            if row is None:
                return None
            run = dict(row)
            staged = self._conn.execute(
                "SELECT COUNT(*) AS n FROM run_records WHERE run_id = ?",
                (run_id,),
            ).fetchone()
            run["staged_count"] = int(staged["n"])
            return run

    def get_active_run(self) -> dict[str, Any] | None:
        with self._lock:
            run_id = self._meta_get("active_run_id")
            if run_id:
                run = self.get_run(run_id)
                if run and run["status"] == "running":
                    return run
            row = self._conn.execute(
                """
                SELECT * FROM runs WHERE status = 'running'
                 ORDER BY started_at DESC LIMIT 1
                """
            ).fetchone()
            return self.get_run(row["id"]) if row else None

    def list_runs(self, limit: int = 15) -> list[dict[str, Any]]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT * FROM runs ORDER BY started_at DESC LIMIT ?", (limit,)
            ).fetchall()
            return [dict(r) for r in rows]

    def is_run_running(self, run_id: str) -> bool:
        with self._lock:
            row = self._conn.execute(
                "SELECT 1 FROM runs WHERE id = ? AND status = 'running'",
                (run_id,),
            ).fetchone()
            return row is not None

    def set_window(self, run_id: str, until: str) -> bool:
        with self._lock:
            cur = self._conn.execute(
                """
                UPDATE runs SET window_until = ?, phase = 'fetching'
                 WHERE id = ? AND status = 'running'
                """,
                (until, run_id),
            )
            return cur.rowcount > 0

    def update_progress(
        self,
        run_id: str,
        *,
        pages_fetched: int,
        records_seen: int,
        token_restarts: int,
        attempt: int,
        complete_list_size: int | None,
        phase: str | None = None,
    ) -> bool:
        with self._lock:
            if phase is None:
                cur = self._conn.execute(
                    """
                    UPDATE runs
                       SET pages_fetched = ?,
                           records_seen = ?,
                           token_restarts = ?,
                           attempt = ?,
                           complete_list_size = ?
                     WHERE id = ? AND status = 'running'
                    """,
                    (
                        pages_fetched,
                        records_seen,
                        token_restarts,
                        attempt,
                        complete_list_size,
                        run_id,
                    ),
                )
            else:
                cur = self._conn.execute(
                    """
                    UPDATE runs
                       SET pages_fetched = ?,
                           records_seen = ?,
                           token_restarts = ?,
                           attempt = ?,
                           complete_list_size = ?,
                           phase = ?
                     WHERE id = ? AND status = 'running'
                    """,
                    (
                        pages_fetched,
                        records_seen,
                        token_restarts,
                        attempt,
                        complete_list_size,
                        phase,
                        run_id,
                    ),
                )
            return cur.rowcount > 0

    def set_last_error(self, run_id: str, message: str) -> None:
        with self._lock:
            self._conn.execute(
                """
                UPDATE runs SET last_error = ?
                 WHERE id = ? AND status = 'running'
                """,
                (message[:4000], run_id),
            )

    def finish_cancelled(self, run_id: str, reason: str) -> bool:
        """Finalise a cancelled run; safe to call twice."""
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                cur = self._conn.execute(
                    """
                    UPDATE runs
                       SET status = 'cancelled',
                           finished_at = COALESCE(finished_at, ?),
                           phase = 'cancelled',
                           cancel_reason = COALESCE(cancel_reason, ?)
                     WHERE id = ? AND status = 'running'
                    """,
                    (utc_now_millis(), reason[:1000], run_id),
                )
                changed = cur.rowcount > 0
                self._delete_stage(run_id)
                if changed:
                    self._clear_active_if(run_id)
                self._conn.execute("COMMIT")
                return changed
            except BaseException:
                self._conn.execute("ROLLBACK")
                raise

    def finish_failed(self, run_id: str, error: str) -> bool:
        """Finalise a failed run.  Staging is discarded, published data not touched."""
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                cur = self._conn.execute(
                    """
                    UPDATE runs
                       SET status = 'failed',
                           finished_at = COALESCE(finished_at, ?),
                           phase = 'failed',
                           last_error = ?
                     WHERE id = ? AND status = 'running'
                    """,
                    (utc_now_millis(), error[:4000], run_id),
                )
                changed = cur.rowcount > 0
                self._delete_stage(run_id)
                if changed:
                    self._clear_active_if(run_id)
                self._conn.execute("COMMIT")
                return changed
            except BaseException:
                self._conn.execute("ROLLBACK")
                raise

    def reset_stage(self, run_id: str) -> bool:
        """Discard all pages collected for the current attempt."""
        with self._lock:
            if not self.is_run_running(run_id):
                return False
            self._conn.execute(
                "DELETE FROM run_records WHERE run_id = ?", (run_id,)
            )
            return True

    def stage_records(
        self, run_id: str, records: Sequence[dict[str, Any]], seq_start: int
    ) -> int:
        """Insert one page into staging, keeping one row per identifier.

        If the same identifier occurs again (whether because of a malformed
        source or a repeated page), the record with the newer OAI datestamp
        wins; the later observation wins a tie.
        """
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                if not self.is_run_running(run_id):
                    self._conn.execute("ROLLBACK")
                    return 0
                for offset, record in enumerate(records):
                    seq = seq_start + offset
                    self._conn.execute(
                        """
                        INSERT INTO run_records
                            (run_id, identifier, title, creator, datestamp,
                             deleted, seq)
                        VALUES (?, ?, ?, ?, ?, ?, ?)
                        ON CONFLICT(run_id, identifier) DO UPDATE SET
                            title = excluded.title,
                            creator = excluded.creator,
                            datestamp = excluded.datestamp,
                            deleted = excluded.deleted,
                            seq = excluded.seq
                        WHERE excluded.datestamp > run_records.datestamp
                           OR (excluded.datestamp = run_records.datestamp
                               AND excluded.seq > run_records.seq)
                        """,
                        (
                            run_id,
                            record["identifier"],
                            record.get("title"),
                            record.get("creator"),
                            record["datestamp"],
                            1 if record.get("deleted") else 0,
                            seq,
                        ),
                    )
                self._conn.execute("COMMIT")
            except BaseException:
                self._conn.execute("ROLLBACK")
                raise
            return len(records)

    def _delete_stage(self, run_id: str) -> None:
        self._conn.execute("DELETE FROM run_records WHERE run_id = ?", (run_id,))

    def _clear_active_if(self, run_id: str) -> None:
        if self._meta_get("active_run_id") == run_id:
            self._meta_set("active_run_id", None)

    def commit_run(self, run_id: str) -> dict[str, Any]:
        """Atomically publish staging and advance the watermark.

        The check for ownership (``active_run_id``) happens inside the same
        IMMEDIATE transaction as the catalogue writes.  A cancelled or
        superseded round can therefore never publish, even if its HTTP
        response arrives late.
        """
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                run = self._conn.execute(
                    "SELECT * FROM runs WHERE id = ?", (run_id,)
                ).fetchone()
                active = self._meta_get("active_run_id")
                if (
                    run is None
                    or run["status"] != "running"
                    or active != run_id
                    or not run["window_until"]
                ):
                    self._conn.execute("ROLLBACK")
                    return {"committed": False, "reason": "run is not active"}

                rows = self._conn.execute(
                    """
                    SELECT identifier, title, creator, datestamp, deleted
                      FROM run_records WHERE run_id = ?
                     ORDER BY seq
                    """,
                    (run_id,),
                ).fetchall()

                now = utc_now_millis()
                upserts = 0
                deletions = 0
                for row in rows:
                    if row["deleted"]:
                        # The row is gone; any duplicate judgement that named
                        # it stops being valid because its content token can no
                        # longer be resolved.
                        self._conn.execute(
                            "DELETE FROM catalog WHERE identifier = ?",
                            (row["identifier"],),
                        )
                        deletions += 1
                    else:
                        # A fresh token is minted for inserts and for content
                        # changes.  Re-reading identical text keeps the old
                        # token, so judgements survive inclusive-window
                        # re-reads.  Tokens are never reused, so restored
                        # identical text cannot revive an invalidated
                        # judgement.
                        self._conn.execute(
                            """
                            INSERT INTO catalog
                                (identifier, title, creator, source_datestamp,
                                 harvested_at, content_token)
                            VALUES (?, ?, ?, ?, ?, ?)
                            ON CONFLICT(identifier) DO UPDATE SET
                                content_token = CASE
                                    WHEN catalog.title = excluded.title
                                     AND COALESCE(catalog.creator, '')
                                         = COALESCE(excluded.creator, '')
                                    THEN catalog.content_token
                                    ELSE excluded.content_token
                                END,
                                title = excluded.title,
                                creator = excluded.creator,
                                source_datestamp = excluded.source_datestamp,
                                harvested_at = excluded.harvested_at
                            """,
                            (
                                row["identifier"],
                                row["title"] or "",
                                row["creator"],
                                row["datestamp"],
                                now,
                                uuid.uuid4().hex,
                            ),
                        )
                        upserts += 1

                watermark = run["window_until"]
                self._conn.execute(
                    """
                    UPDATE published_state
                       SET watermark = ?,
                           updated_at = ?,
                           success_count = success_count + 1,
                           last_run_id = ?
                     WHERE id = 1
                    """,
                    (watermark, now, run_id),
                )
                self._conn.execute(
                    """
                    UPDATE runs
                       SET status = 'succeeded',
                           finished_at = ?,
                           phase = 'published',
                           watermark_after = ?,
                           last_error = NULL
                     WHERE id = ?
                    """,
                    (now, watermark, run_id),
                )
                self._clear_active_if(run_id)
                self._delete_stage(run_id)
                self._conn.execute("COMMIT")
                return {
                    "committed": True,
                    "upserts": upserts,
                    "deletions": deletions,
                    "watermark": watermark,
                }
            except BaseException:
                self._conn.execute("ROLLBACK")
                raise

    def fail_run(self, run_id: str, error: str) -> bool:
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                cur = self._conn.execute(
                    """
                    UPDATE runs
                       SET status = 'failed',
                           finished_at = ?,
                           phase = 'failed',
                           last_error = ?
                     WHERE id = ? AND status = 'running'
                    """,
                    (utc_now_millis(), error[:4000], run_id),
                )
                self._delete_stage(run_id)
                self._clear_active_if(run_id)
                self._conn.execute("COMMIT")
                return cur.rowcount > 0
            except BaseException:
                self._conn.execute("ROLLBACK")
                raise

    def cancel_run(self, run_id: str, reason: str) -> bool:
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                cur = self._conn.execute(
                    """
                    UPDATE runs
                       SET status = 'cancelled',
                           finished_at = ?,
                           phase = 'cancelled',
                           cancel_reason = ?
                     WHERE id = ? AND status = 'running'
                    """,
                    (utc_now_millis(), reason[:1000], run_id),
                )
                self._delete_stage(run_id)
                self._clear_active_if(run_id)
                self._conn.execute("COMMIT")
                return cur.rowcount > 0
            except BaseException:
                self._conn.execute("ROLLBACK")
                raise

    # -------------------------------------------------------------- duplicates
    def _catalog_snapshot(self) -> dict[str, dict[str, Any]]:
        rows = self._conn.execute(
            """
            SELECT identifier, title, creator, content_token
              FROM catalog ORDER BY identifier
            """
        ).fetchall()
        return {
            row["identifier"]: {
                "identifier": row["identifier"],
                "title": row["title"],
                "creator": row["creator"],
                "content_token": row["content_token"],
            }
            for row in rows
        }

    def _latest_judgements(self) -> list[dict[str, Any]]:
        rows = self._conn.execute(
            """
            SELECT j.*
              FROM duplicate_judgements j
              JOIN (
                    SELECT left_identifier, right_identifier, MAX(id) AS latest_id
                      FROM duplicate_judgements
                     GROUP BY left_identifier, right_identifier
                   ) latest
                ON latest.latest_id = j.id
             ORDER BY j.id DESC
            """
        ).fetchall()
        return [dict(row) for row in rows]

    @staticmethod
    def _judgement_row(
        judgement: dict[str, Any],
        snapshot: dict[str, dict[str, Any]],
    ) -> dict[str, Any] | None:
        """The judgement with its two records, or ``None`` when it is expired.

        A judgement stays valid only while both identifiers still exist in the
        published catalogue *and* their content tokens are unchanged.  A
        content correction or a withdrawal therefore expires it; a later
        harvest that restores the same text mints a new token, so the old
        judgement does not come back to life.
        """
        left = snapshot.get(judgement["left_identifier"])
        right = snapshot.get(judgement["right_identifier"])
        if left is None or right is None:
            return None
        if (
            left["content_token"] != judgement["left_token"]
            or right["content_token"] != judgement["right_token"]
        ):
            return None
        return {
            "id": judgement["id"],
            "verdict": judgement["verdict"],
            "created_at": judgement["created_at"],
            "left": {
                "identifier": left["identifier"],
                "title": left["title"],
                "creator": left["creator"],
            },
            "right": {
                "identifier": right["identifier"],
                "title": right["title"],
                "creator": right["creator"],
            },
        }

    def duplicate_overview(self) -> dict[str, Any]:
        """Pending candidate pairs plus judgements that are still valid."""
        with self._lock:
            snapshot = self._catalog_snapshot()
            latest = self._latest_judgements()
            valid: list[dict[str, Any]] = []
            decided: set[tuple[str, str]] = set()
            for judgement in latest:
                row = self._judgement_row(judgement, snapshot)
                if row is not None:
                    valid.append(row)
                    decided.add(
                        (judgement["left_identifier"], judgement["right_identifier"])
                    )

            groups: dict[tuple[str, str], list[dict[str, Any]]] = {}
            for item in snapshot.values():
                groups.setdefault(
                    grouping_key(item["title"], item["creator"]), []
                ).append(item)

            pending: list[dict[str, Any]] = []
            for key in sorted(groups):
                members = groups[key]
                if len(members) < 2:
                    continue
                for i, left in enumerate(members):
                    for right in members[i + 1 :]:
                        pair = canonical_identifiers(
                            left["identifier"], right["identifier"]
                        )
                        if pair in decided:
                            continue
                        pending.append(
                            {
                                "left": {
                                    "identifier": left["identifier"],
                                    "title": left["title"],
                                    "creator": left["creator"],
                                    "content_token": left["content_token"],
                                },
                                "right": {
                                    "identifier": right["identifier"],
                                    "title": right["title"],
                                    "creator": right["creator"],
                                    "content_token": right["content_token"],
                                },
                                "key_title": key[0],
                                "key_creator": key[1],
                            }
                        )
            return {
                "pending": pending,
                "valid": valid,
                "counts": {
                    "pending": len(pending),
                    "valid": len(valid),
                },
            }

    def record_judgement(
        self,
        left_identifier: str,
        right_identifier: str,
        left_token: str,
        right_token: str,
        verdict: str,
    ) -> dict[str, Any]:
        """Store a reviewer's verdict, rejecting stale or invalid submissions.

        The caller must pass the content tokens it saw when rendering the pair.
        Inside one ``BEGIN IMMEDIATE`` transaction the pair is re-read from the
        published catalogue: if either record disappeared, its content changed,
        or the two are no longer a candidate pair, the submission is rejected
        as stale and the current overview is returned for redisplay.
        """
        if verdict not in ("same", "different"):
            raise ValueError("判定只能是 same 或 different")
        submitted = {left_identifier: left_token, right_identifier: right_token}
        a, b = canonical_identifiers(left_identifier, right_identifier)
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                rows = self._conn.execute(
                    """
                    SELECT identifier, title, creator, content_token
                      FROM catalog WHERE identifier IN (?, ?)
                    """,
                    (a, b),
                ).fetchall()
                current = {row["identifier"]: dict(row) for row in rows}
                reason: str | None = None
                if a == b or set(current) != {a, b}:
                    reason = "候选对已不存在（书目已撤回或标识变化）"
                elif any(
                    current[identifier]["content_token"] != token
                    for identifier, token in submitted.items()
                ):
                    reason = "书目内容已更新，提交的判定已过期"
                elif grouping_key(
                    current[a]["title"], current[a]["creator"]
                ) != grouping_key(current[b]["title"], current[b]["creator"]):
                    reason = "两条书目已不再属于同一候选组"

                if reason is not None:
                    self._conn.execute("ROLLBACK")
                    return {
                        "accepted": False,
                        "reason": reason,
                        "overview": self.duplicate_overview(),
                    }

                now = utc_now_millis()
                cur = self._conn.execute(
                    """
                    INSERT INTO duplicate_judgements
                        (left_identifier, right_identifier, left_token,
                         right_token, verdict, created_at)
                    VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (
                        a,
                        b,
                        current[a]["content_token"],
                        current[b]["content_token"],
                        verdict,
                        now,
                    ),
                )
                self._conn.execute("COMMIT")
                return {
                    "accepted": True,
                    "judgement": {
                        "id": cur.lastrowid,
                        "verdict": verdict,
                        "created_at": now,
                        "left": {
                            "identifier": a,
                            "title": current[a]["title"],
                            "creator": current[a]["creator"],
                        },
                        "right": {
                            "identifier": b,
                            "title": current[b]["title"],
                            "creator": current[b]["creator"],
                        },
                    },
                    "overview": self.duplicate_overview(),
                }
            except BaseException:
                self._conn.execute("ROLLBACK")
                raise

    # --------------------------------------------------------------- published
    def published_state(self) -> dict[str, Any]:
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM published_state WHERE id = 1"
            ).fetchone()
            result = dict(row) if row else {
                "watermark": None,
                "updated_at": None,
                "success_count": 0,
                "last_run_id": None,
            }
            count = self._conn.execute(
                "SELECT COUNT(*) AS n FROM catalog"
            ).fetchone()
            result["catalog_size"] = int(count["n"])
            return result

    def catalog_page(
        self, query: str = "", page: int = 1, page_size: int = 25
    ) -> dict[str, Any]:
        query = (query or "").strip()
        page = max(1, int(page or 1))
        page_size = min(200, max(1, int(page_size or 25)))
        like = f"%{query}%"
        where = ""
        params: list[Any] = []
        if query:
            where = (
                "WHERE identifier LIKE ? OR title LIKE ? "
                "OR COALESCE(creator, '') LIKE ?"
            )
            params = [like, like, like]
        with self._lock:
            total = self._conn.execute(
                f"SELECT COUNT(*) AS n FROM catalog {where}", params
            ).fetchone()["n"]
            total = int(total)
            pages = max(1, (total + page_size - 1) // page_size)
            if page > pages:
                page = pages
            rows = self._conn.execute(
                f"""
                SELECT identifier, title, creator, source_datestamp,
                       harvested_at
                  FROM catalog {where}
                 ORDER BY title COLLATE NOCASE, identifier
                 LIMIT ? OFFSET ?
                """,
                [*params, page_size, (page - 1) * page_size],
            ).fetchall()
            return {
                "records": [dict(r) for r in rows],
                "total": total,
                "page": page,
                "page_size": page_size,
                "pages": pages,
                "query": query,
            }

    def close(self) -> None:
        with self._lock:
            self._conn.close()

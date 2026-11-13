"""
Metrics Service

Provides comprehensive metrics collection and analysis for:
- Test execution analytics and performance tracking
- Quality analysis trends and statistics
- System performance monitoring
- User activity tracking
"""

from __future__ import annotations

import json
import logging
import os
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import uuid7

import aiosqlite

logger = logging.getLogger(__name__)


class MetricsService:
    """Service for collecting and analyzing system metrics."""

    def __init__(self, metrics_db_path: str | None = None) -> None:
        """Initialize the MetricsService service.

        Args:
            metrics_db_path: Path to the metrics database. If None, uses ./data/metrics.db.
        """
        self.metrics_db_path = metrics_db_path or os.path.join("./data", "metrics.db")
        self._ensure_metrics_database()

    def _ensure_metrics_database(self) -> None:
        """Ensure metrics database exists with required tables."""
        try:
            os.makedirs(os.path.dirname(self.metrics_db_path), exist_ok=True)
            self._initialize_database_schema_sync()
        except Exception as e:
            logger.error("Error ensuring metrics database: %s", e)

    def _initialize_database_schema_sync(self) -> None:
        """Initialize the metrics database schema synchronously."""
        try:
            import sqlite3

            with sqlite3.connect(self.metrics_db_path) as db:
                db.execute(
                    "\n                    CREATE TABLE IF NOT EXISTS test_executions (\n                        id TEXT PRIMARY KEY,\n                        user_id TEXT NOT NULL,\n                        test_id TEXT NOT NULL,\n                        success BOOLEAN NOT NULL,\n                        language TEXT NOT NULL,\n                        execution_time REAL NOT NULL,\n                        test_type TEXT,\n                        framework TEXT,\n                        file_count INTEGER,\n                        error_message TEXT,\n                        metadata TEXT,\n                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP\n                    )\n                "
                )
                db.execute(
                    "\n                    CREATE TABLE IF NOT EXISTS quality_analyses (\n                        id TEXT PRIMARY KEY,\n                        user_id TEXT NOT NULL,\n                        analysis_id TEXT NOT NULL,\n                        overall_score REAL,\n                        total_issues INTEGER NOT NULL,\n                        language TEXT NOT NULL,\n                        analysis_type TEXT,\n                        file_count INTEGER,\n                        lines_analyzed INTEGER,\n                        critical_issues INTEGER DEFAULT 0,\n                        major_issues INTEGER DEFAULT 0,\n                        minor_issues INTEGER DEFAULT 0,\n                        metadata TEXT,\n                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP\n                    )\n                "
                )
                db.execute(
                    "\n                    CREATE TABLE IF NOT EXISTS system_metrics (\n                        id TEXT PRIMARY KEY,\n                        metric_type TEXT NOT NULL,\n                        metric_name TEXT NOT NULL,\n                        metric_value REAL NOT NULL,\n                        unit TEXT,\n                        tags TEXT,\n                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP\n                    )\n                "
                )
                db.execute(
                    "\n                    CREATE TABLE IF NOT EXISTS user_activities (\n                        id TEXT PRIMARY KEY,\n                        user_id TEXT NOT NULL,\n                        activity_type TEXT NOT NULL,\n                        endpoint TEXT,\n                        duration REAL,\n                        success BOOLEAN,\n                        metadata TEXT,\n                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP\n                    )\n                "
                )
                db.execute(
                    "CREATE INDEX IF NOT EXISTS idx_test_executions_user_id ON test_executions(user_id)"
                )
                db.execute(
                    "CREATE INDEX IF NOT EXISTS idx_test_executions_created_at ON test_executions(created_at)"
                )
                db.execute(
                    "CREATE INDEX IF NOT EXISTS idx_quality_analyses_user_id ON quality_analyses(user_id)"
                )
                db.execute(
                    "CREATE INDEX IF NOT EXISTS idx_quality_analyses_created_at ON quality_analyses(created_at)"
                )
                db.execute(
                    "CREATE INDEX IF NOT EXISTS idx_system_metrics_type ON system_metrics(metric_type)"
                )
                db.execute(
                    "CREATE INDEX IF NOT EXISTS idx_user_activities_user_id ON user_activities(user_id)"
                )
                db.commit()
                logger.info("Metrics database schema initialized successfully")
        except Exception as e:
            logger.error("Error initializing metrics database schema: %s", e)

    async def _initialize_database_schema(self) -> None:
        """Initialize the metrics database schema."""
        try:
            async with aiosqlite.connect(self.metrics_db_path) as db:
                await db.execute(
                    "\n                    CREATE TABLE IF NOT EXISTS test_executions (\n                        id TEXT PRIMARY KEY,\n                        user_id TEXT NOT NULL,\n                        test_id TEXT NOT NULL,\n                        success BOOLEAN NOT NULL,\n                        language TEXT NOT NULL,\n                        execution_time REAL NOT NULL,\n                        test_type TEXT,\n                        framework TEXT,\n                        file_count INTEGER,\n                        error_message TEXT,\n                        metadata TEXT,\n                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP\n                    )\n                "
                )
                await db.execute(
                    "\n                    CREATE TABLE IF NOT EXISTS quality_analyses (\n                        id TEXT PRIMARY KEY,\n                        user_id TEXT NOT NULL,\n                        analysis_id TEXT NOT NULL,\n                        overall_score REAL,\n                        total_issues INTEGER NOT NULL,\n                        language TEXT NOT NULL,\n                        analysis_type TEXT,\n                        file_count INTEGER,\n                        lines_analyzed INTEGER,\n                        critical_issues INTEGER DEFAULT 0,\n                        major_issues INTEGER DEFAULT 0,\n                        minor_issues INTEGER DEFAULT 0,\n                        metadata TEXT,\n                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP\n                    )\n                "
                )
                await db.execute(
                    "\n                    CREATE TABLE IF NOT EXISTS system_metrics (\n                        id TEXT PRIMARY KEY,\n                        metric_type TEXT NOT NULL,\n                        metric_name TEXT NOT NULL,\n                        metric_value REAL NOT NULL,\n                        unit TEXT,\n                        tags TEXT,\n                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP\n                    )\n                "
                )
                await db.execute(
                    "\n                    CREATE TABLE IF NOT EXISTS user_activities (\n                        id TEXT PRIMARY KEY,\n                        user_id TEXT NOT NULL,\n                        activity_type TEXT NOT NULL,\n                        endpoint TEXT,\n                        duration REAL,\n                        success BOOLEAN,\n                        metadata TEXT,\n                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP\n                    )\n                "
                )
                await db.execute(
                    "CREATE INDEX IF NOT EXISTS idx_test_executions_user_id ON test_executions(user_id)"
                )
                await db.execute(
                    "CREATE INDEX IF NOT EXISTS idx_test_executions_created_at ON test_executions(created_at)"
                )
                await db.execute(
                    "CREATE INDEX IF NOT EXISTS idx_quality_analyses_user_id ON quality_analyses(user_id)"
                )
                await db.execute(
                    "CREATE INDEX IF NOT EXISTS idx_quality_analyses_created_at ON quality_analyses(created_at)"
                )
                await db.execute(
                    "CREATE INDEX IF NOT EXISTS idx_system_metrics_type ON system_metrics(metric_type)"
                )
                await db.execute(
                    "CREATE INDEX IF NOT EXISTS idx_user_activities_user_id ON user_activities(user_id)"
                )
                await db.commit()
                logger.info("Metrics database schema initialized successfully")
        except Exception as e:
            logger.error("Error initializing metrics database schema: %s", e)

    async def record_test_execution(self, execution_data: dict[str, Any]) -> str:
        """
        Record test execution metrics.

        Args:
            execution_data: Dictionary containing test execution information

        Returns:
            Unique ID of the recorded metric
        """
        try:
            record_id = str(uuid7())
            async with aiosqlite.connect(self.metrics_db_path) as db:
                await db.execute(
                    "\n                    INSERT INTO test_executions (\n                        id, user_id, test_id, success, language, execution_time,\n                        test_type, framework, file_count, error_message, metadata\n                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)\n                ",
                    (
                        record_id,
                        execution_data.get("user_id", "unknown"),
                        execution_data.get("test_id", ""),
                        execution_data.get("success", False),
                        execution_data.get("language", "unknown"),
                        execution_data.get("execution_time", 0.0),
                        execution_data.get("test_type", ""),
                        execution_data.get("framework", ""),
                        execution_data.get("file_count", 0),
                        execution_data.get("error_message"),
                        json.dumps(execution_data.get("metadata", {})),
                    ),
                )
                await db.commit()
            logger.debug("Recorded test execution metric: %s", record_id)
            return record_id
        except Exception as e:
            logger.error("Error recording test execution: %s", e)
            raise

    async def record_quality_analysis(self, analysis_data: dict[str, Any]) -> str:
        """
        Record quality analysis metrics.

        Args:
            analysis_data: Dictionary containing quality analysis information

        Returns:
            Unique ID of the recorded metric
        """
        try:
            record_id = str(uuid7())
            async with aiosqlite.connect(self.metrics_db_path) as db:
                await db.execute(
                    "\n                    INSERT INTO quality_analyses (\n                        id, user_id, analysis_id, overall_score, total_issues, language,\n                        analysis_type, file_count, lines_analyzed, critical_issues,\n                        major_issues, minor_issues, metadata\n                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)\n                ",
                    (
                        record_id,
                        analysis_data.get("user_id", "unknown"),
                        analysis_data.get("analysis_id", ""),
                        analysis_data.get("overall_score"),
                        analysis_data.get("total_issues", 0),
                        analysis_data.get("language", "unknown"),
                        analysis_data.get("analysis_type", ""),
                        analysis_data.get("file_count", 0),
                        analysis_data.get("lines_analyzed", 0),
                        analysis_data.get("critical_issues", 0),
                        analysis_data.get("major_issues", 0),
                        analysis_data.get("minor_issues", 0),
                        json.dumps(analysis_data.get("metadata", {})),
                    ),
                )
                await db.commit()
            logger.debug("Recorded quality analysis metric: %s", record_id)
            return record_id
        except Exception as e:
            logger.error("Error recording quality analysis: %s", e)
            raise

    async def get_test_execution_analytics(
        self, user_id: str | None = None, days: int = 30
    ) -> dict[str, Any]:
        """Get test execution analytics for specified period."""
        try:
            since_date = datetime.now(UTC) - timedelta(days=days)
            async with aiosqlite.connect(self.metrics_db_path) as db:
                where_clause = "WHERE created_at >= ?"
                params: list[Any] = [since_date.isoformat()]
                if user_id:
                    where_clause += " AND user_id = ?"
                    params.append(user_id)
                cursor = await db.execute(
                    f"SELECT COUNT(*) FROM test_executions {where_clause}",
                    params,
                )
                row = await cursor.fetchone()
                total_executions = row[0] if row else 0
                await cursor.close()
                cursor = await db.execute(
                    f"""
                    SELECT
                        SUM(CASE WHEN success = 1 THEN 1 ELSE 0 END) as successful,
                        COUNT(*) as total
                    FROM test_executions {where_clause}
                    """,
                    params,
                )
                result = await cursor.fetchone()
                await cursor.close()
                if result:
                    successful, total = result
                else:
                    successful, total = (0, 0)
                success_rate = successful / total * 100 if total > 0 else 0
                cursor = await db.execute(
                    f"SELECT AVG(execution_time) FROM test_executions {where_clause}",
                    params,
                )
                time_row = await cursor.fetchone()
                avg_execution_time = time_row[0] if time_row and time_row[0] else 0
                await cursor.close()
                cursor = await db.execute(
                    f"""
                    SELECT language, COUNT(*)
                    FROM test_executions {where_clause}
                    GROUP BY language
                    ORDER BY COUNT(*) DESC
                    """,
                    params,
                )
                language_results = await cursor.fetchall() or []
                language_breakdown: dict[str, int] = {
                    lang: count for lang, count in language_results
                }
                await cursor.close()
                return {
                    "total_executions": total_executions,
                    "success_rate": round(success_rate, 2),
                    "avg_execution_time": round(avg_execution_time, 4),
                    "language_breakdown": language_breakdown,
                    "period_days": days,
                    "user_id": user_id,
                }
        except Exception as e:
            logger.error("Error getting test execution analytics: %s", e)
            return {"error": str(e)}

    async def get_quality_analysis_analytics(
        self, user_id: str | None = None, days: int = 30
    ) -> dict[str, Any]:
        """Get quality analysis analytics for specified period."""
        try:
            since_date = datetime.now(UTC) - timedelta(days=days)
            async with aiosqlite.connect(self.metrics_db_path) as db:
                where_clause = "WHERE created_at >= ?"
                params: list[Any] = [since_date.isoformat()]
                if user_id:
                    where_clause += " AND user_id = ?"
                    params.append(user_id)
                cursor = await db.execute(
                    f"SELECT COUNT(*) FROM quality_analyses {where_clause}",
                    params,
                )
                analyses_row = await cursor.fetchone()
                total_analyses = analyses_row[0] if analyses_row else 0
                await cursor.close()
                cursor = await db.execute(
                    f"""
                    SELECT AVG(overall_score) FROM quality_analyses
                    {where_clause} AND overall_score IS NOT NULL
                    """,
                    params,
                )
                score_row = await cursor.fetchone()
                avg_score = score_row[0] if score_row and score_row[0] else 0
                await cursor.close()
                cursor = await db.execute(
                    f"""
                    SELECT
                        SUM(critical_issues) as critical,
                        SUM(major_issues) as major,
                        SUM(minor_issues) as minor,
                        SUM(total_issues) as total
                    FROM quality_analyses {where_clause}
                    """,
                    params,
                )
                issues_row = await cursor.fetchone()
                await cursor.close()
                if issues_row and len(issues_row) >= 4:
                    critical, major, minor, total = issues_row
                else:
                    critical, major, minor, total = (0, 0, 0, 0)
                return {
                    "total_analyses": total_analyses,
                    "avg_quality_score": round(avg_score, 2),
                    "issue_breakdown": {
                        "critical": critical or 0,
                        "major": major or 0,
                        "minor": minor or 0,
                        "total": total or 0,
                    },
                    "period_days": days,
                    "user_id": user_id,
                }
        except Exception as e:
            logger.error("Error getting quality analysis analytics: %s", e)
            return {"error": str(e)}

    def get_metrics_service(self) -> MetricsService:
        """Return self for compatibility."""
        return self


_metrics_service: MetricsService | None = None


def get_metrics_service(metrics_db_path: str | None = None) -> MetricsService:
    """Get or create the metrics service instance."""
    global _metrics_service
    if _metrics_service is None:
        _metrics_service = MetricsService(metrics_db_path=metrics_db_path)
    return _metrics_service

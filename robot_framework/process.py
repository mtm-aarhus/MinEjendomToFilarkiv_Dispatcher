"""This module contains the main process of the robot."""

from OpenOrchestrator.orchestrator_connection.connection import OrchestratorConnection
from OpenOrchestrator.database.queues import QueueElement
import pyodbc
import sqlite3
import os
import json

# pylint: disable-next=unused-argument
def process(orchestrator_connection: OrchestratorConnection, queue_element: QueueElement | None = None) -> None:
    orchestrator_connection.log_trace("Running process.")
    
    
    SQLITE_PATH = r"C:\Users\az72987\Desktop\minejendom2filarkiv.db"
    conn = sqlite3.connect(SQLITE_PATH)
    cur = conn.cursor()

    # Select candidates that meet your filter
    cur.execute("""
        SELECT
            d.Id AS DocumentId,
            d.Title AS DocumentTitle,
            d.FileName,
            d.FileExtension,
            d.IsScannedPage,
            c.Id AS CaseId,
            c.FilArkivCaseId,
            c.CaseNumber,
            c.CaseTitle,
            c.IgnoreCase,
            d.FilePath
        FROM MinEjendom_Documents d
        JOIN MinEjendom_Cases c ON d.CaseId = c.Id
        WHERE c.IgnoreCase = 0
        AND (d.FilArkivFileId IS NULL AND d.MergedDocumentId IS NULL)
        ORDER BY c.Id, d.Id;
    """)
    rows = cur.fetchall()
    print(f"Fetched rows: {len(rows)}")

    # --- rules ----------------------------------------------------
    column_names = [desc[0] for desc in cur.description]

    lock_words = [
        "fortrolig", "høring", "høringssvar", "kviterring for hørringssvar",
        "høringspart", "indsiger", "indsigelser", "indsige",
        "klage", "nabo", "intern"
    ]

    lock_word_count = 0
    lock_extension_count = 0
    remove_count = 0
    remove_words = ["fletteliste", "flettelister","flette"]
    intern_exception = "intern færdigmelding"
    excel_extensions = {"xls", "xlsx", "xlsm", "xlsb"}
    queue_items = []
    # --- processing -----------------------------------------------


    for row in rows:
        # Convert full row → dict (ALL columns)
        row_dict = dict(zip(column_names, row))

        title = (row_dict.get("DocumentTitle") or "").lower()
        extension = (row_dict.get("FileExtension") or "").lower()

        # --- Skip fletteliste completely --------------------------
        if any(word in title for word in remove_words):
            continue

        # --- Determine securityClassificationLevel ----------------
        securityClassificationLevel = 0

        if intern_exception not in title:
            if any(word in title for word in lock_words):
                securityClassificationLevel = 1

        if extension in excel_extensions:
            securityClassificationLevel = 1

        # --- Add classification column ----------------------------
        row_dict["securityClassificationLevel"] = securityClassificationLevel

        # --- Add to queue -----------------------------------------
        queue_items.append({
            "Reference": str(row_dict["DocumentId"]),
            "SpecificContent": row_dict
        })

    references = tuple(item["Reference"] for item in queue_items)
    data = tuple(json.dumps(item["SpecificContent"]) for item in queue_items)

    queue_name = "MinEjendomToFilarkiv"

    try:
        orchestrator_connection.bulk_create_queue_elements(
            queue_name,
            references,
            data,
            created_by="MinEjendomToFilarkiv_Dispatcher"
        )
        orchestrator_connection.log_info(
            f"Successfully added {len(queue_items)} items to the queue."
        )
    except Exception as e:
        print(f"An error occurred while adding items to the queue: {str(e)}")



    # --- summary --------------------------------------------------

    print(f"Antal dokumenter der skal fjernes: {remove_count}")
    print(f"Antal dokumenter der skal låses pga. titel: {lock_word_count}")
    print(f"Antal dokumenter der skal låses pga. filtype (Excel): {lock_extension_count}")
    print(
        f"Samlet antal låste dokumenter: "
        f"{lock_word_count + lock_extension_count}")


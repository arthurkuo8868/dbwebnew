import os
import re
import urllib.parse

import pandas as pd
from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()

app = FastAPI()

# -----------------------
# DB config from .env
# -----------------------
DB_SERVER = os.getenv("DB_SERVER")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_DRIVER = os.getenv("DB_DRIVER", "ODBC Driver 18 for SQL Server")
DB_ENCRYPT = os.getenv("DB_ENCRYPT", "yes")  # Azure SQL 建議 yes
DB_TRUST_CERT = os.getenv("DB_TRUST_CERT", "no")

if not all([DB_SERVER, DB_NAME, DB_USER, DB_PASSWORD]):
    raise RuntimeError("❌ Database environment variables are not fully set")

# SQLAlchemy URL (mssql+pyodbc)
# 重要：driver 需要 URL encode
driver_q = urllib.parse.quote_plus(DB_DRIVER)

CONN_STR = (
    f"mssql+pyodbc://{urllib.parse.quote_plus(DB_USER)}:{urllib.parse.quote_plus(DB_PASSWORD)}"
    f"@{DB_SERVER}/{DB_NAME}"
    f"?driver={driver_q}"
    f"&Encrypt={DB_ENCRYPT}"
    f"&TrustServerCertificate={DB_TRUST_CERT}"
)

engine = create_engine(CONN_STR, pool_pre_ping=True)

# -----------------------
# Helpers
# -----------------------
# 允許：字母數字底線 + 空格（給 'Order Details'）+ 點（schema.table）
# 仍然會拆 schema/table 並用 QUOTENAME 防注入
SAFE_NAME_RE = re.compile(r"^[A-Za-z0-9_ .-]+$")

def parse_schema_table(full: str) -> tuple[str, str]:
    """
    full examples:
      'dbo.Orders'
      'dbo.Order Details'
    """
    full = full.strip()
    # URL 上可能會帶 %20，前端若已 encode 也 ok
    full = urllib.parse.unquote(full)

    if not SAFE_NAME_RE.match(full):
        raise ValueError("table 名稱格式不合法")

    if "." in full:
        schema, table = full.split(".", 1)
    else:
        schema, table = "dbo", full

    schema = schema.strip()
    table = table.strip()

    if not schema or not table:
        raise ValueError("schema/table 不可為空")

    return schema, table


def fetch_table_comments(schema: str, table: str) -> dict[str, str]:
    """
    讀取 SQL Server extended property (MS_Description) 作為中文欄位名
    - 如果沒有註解，就回傳空 dict
    """
    sql = text("""
    SELECT
        c.name AS column_name,
        CAST(ep.value AS NVARCHAR(4000)) AS column_comment
    FROM sys.columns c
    INNER JOIN sys.objects o ON c.object_id = o.object_id
    INNER JOIN sys.schemas s ON o.schema_id = s.schema_id
    LEFT JOIN sys.extended_properties ep
        ON ep.major_id = c.object_id
        AND ep.minor_id = c.column_id
        AND ep.name = 'MS_Description'
    WHERE s.name = :schema AND o.name = :table
    ORDER BY c.column_id
    """)
    df = pd.read_sql(sql, engine, params={"schema": schema, "table": table})
    mapping = {}
    for _, row in df.iterrows():
        if row["column_comment"] and str(row["column_comment"]).strip():
            mapping[str(row["column_name"])] = str(row["column_comment"]).strip()
    return mapping


@app.get("/", response_class=HTMLResponse)
def home():
    # 你的 index.html 如果是內嵌就放這裡；若你有獨立檔案也可讀檔回傳
    return """
<!doctype html>
<html lang="zh-Hant">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width,initial-scale=1" />
  <title>DB Web Viewer</title>
  <style>
    body { font-family: Arial, sans-serif; margin: 20px; }
    select, input { padding: 6px; margin-right: 8px; }
    table { border-collapse: collapse; width: 100%; margin-top: 12px; }
    th, td { border: 1px solid #ddd; padding: 8px; font-size: 14px; }
    th { background: #f5f5f5; position: sticky; top: 0; }
    .row { margin-bottom: 12px; }
    .hint { color: #666; font-size: 13px; }
  </style>
</head>
<body>
  <h2>資料庫瀏覽器</h2>

  <div class="row">
    <label>選擇資料表：</label>
    <select id="tableSelect"></select>

    <label>筆數：</label>
    <input id="limitInput" type="number" value="100" min="1" max="1000" />
    <button id="loadBtn">載入</button>
    <span class="hint">（下一步我可改成「選表就自動載入」）</span>
  </div>

  <div id="status"></div>
  <div style="overflow:auto; max-height: 70vh;">
    <table id="dataTable"></table>
  </div>

<script>
async function fetchJSON(url) {
  const r = await fetch(url);
  if (!r.ok) throw new Error(await r.text());
  return r.json();
}

function setStatus(msg) {
  document.getElementById("status").innerText = msg || "";
}

function renderTable(columns, rows) {
  const table = document.getElementById("dataTable");
  table.innerHTML = "";

  const thead = document.createElement("thead");
  const trh = document.createElement("tr");
  columns.forEach(c => {
    const th = document.createElement("th");
    th.innerText = c;
    trh.appendChild(th);
  });
  thead.appendChild(trh);

  const tbody = document.createElement("tbody");
  rows.forEach(r => {
    const tr = document.createElement("tr");
    columns.forEach(c => {
      const td = document.createElement("td");
      td.innerText = (r[c] === null || r[c] === undefined) ? "" : String(r[c]);
      tr.appendChild(td);
    });
    tbody.appendChild(tr);
  });

  table.appendChild(thead);
  table.appendChild(tbody);
}

async function loadTables() {
  setStatus("載入資料表清單中...");
  const data = await fetchJSON("/api/tables");
  const sel = document.getElementById("tableSelect");
  sel.innerHTML = "";
  data.tables.forEach(t => {
    const opt = document.createElement("option");
    opt.value = t;
    opt.innerText = t;
    sel.appendChild(opt);
  });
  setStatus("");
}

async function loadData() {
  const table = document.getElementById("tableSelect").value;
  const limit = document.getElementById("limitInput").value || 100;

  setStatus("讀取資料中...");
  const encoded = encodeURIComponent(table);
  const data = await fetchJSON(`/api/table/${encoded}?limit=${limit}`);
  renderTable(data.columns, data.rows);
  setStatus(`完成：${table}（${data.rows.length} 筆）`);
}

document.getElementById("loadBtn").addEventListener("click", loadData);

loadTables().catch(e => setStatus("錯誤：" + e.message));
</script>
</body>
</html>
"""


@app.get("/api/tables")
def list_tables():
    # SQL Server: 列出使用者資料表
    sql = text("""
    SELECT
        s.name AS schema_name,
        t.name AS table_name
    FROM sys.tables t
    INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
    ORDER BY s.name, t.name
    """)
    df = pd.read_sql(sql, engine)
    tables = [f"{r['schema_name']}.{r['table_name']}" for _, r in df.iterrows()]
    return {"tables": tables}


@app.get("/api/table/{table_full}")
def read_table(table_full: str, limit: int = 100):
    if limit < 1 or limit > 5000:
        raise HTTPException(status_code=400, detail="limit 需介於 1~5000")

    try:
        schema, table = parse_schema_table(table_full)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

    # 1) 先抓欄位註解 mapping（英文→中文）
    comment_map = fetch_table_comments(schema, table)

    # 2) 安全拼出 SELECT：使用 QUOTENAME 包 schema/table
    # 注意：table 名稱含空格 OK，會被 QUOTENAME 變成 [Order Details]
    sql = text(f"""
    DECLARE @sql NVARCHAR(MAX) =
        N'SELECT TOP ({limit}) * FROM ' + QUOTENAME(:schema) + N'.' + QUOTENAME(:table) + N'';
    EXEC sp_executesql @sql, N'@schema sysname, @table sysname', @schema=:schema, @table=:table;
    """)

    # pandas read_sql 不支援直接拿 EXEC 結果，用 connection 執行後再讀
    with engine.connect() as conn:
        result = conn.execute(sql, {"schema": schema, "table": table})
        rows = result.fetchall()
        cols = list(result.keys())

    # 3) 欄位名稱轉中文（有註解就用註解）
    display_cols = [comment_map.get(c, c) for c in cols]

    # 4) 回傳 rows 以「顯示欄位名」當 key
    out_rows = []
    for row in rows:
        d = {}
        for i, c in enumerate(display_cols):
            d[c] = row[i]
        out_rows.append(d)

    return {"table": f"{schema}.{table}", "columns": display_cols, "rows": out_rows}

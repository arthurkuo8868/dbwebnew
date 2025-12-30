from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import pyodbc

# ===============================
# 欄位中英文對照表（Northwind）
# ===============================
COLUMN_NAME_MAP = {

    "Orders": {
        "OrderID": "訂單編號",
        "CustomerID": "客戶代碼",
        "EmployeeID": "員工編號",
        "OrderDate": "訂單日期",
        "RequiredDate": "需求日期",
        "ShippedDate": "出貨日期",
        "ShipVia": "運送方式",
        "Freight": "運費",
        "ShipName": "收貨人",
        "ShipAddress": "收貨地址",
        "ShipCity": "收貨城市",
        "ShipRegion": "收貨地區",
        "ShipPostalCode": "郵遞區號",
        "ShipCountry": "收貨國家"
    },

    "Order Details": {
        "OrderID": "訂單編號",
        "ProductID": "產品編號",
        "UnitPrice": "單價",
        "Quantity": "數量",
        "Discount": "折扣"
    },

    "Customers": {
        "CustomerID": "客戶代碼",
        "CompanyName": "公司名稱",
        "ContactName": "聯絡人",
        "ContactTitle": "職稱",
        "Address": "地址",
        "City": "城市",
        "Region": "地區",
        "PostalCode": "郵遞區號",
        "Country": "國家",
        "Phone": "電話",
        "Fax": "傳真"
    },

    "Employees": {
        "EmployeeID": "員工編號",
        "LastName": "姓",
        "FirstName": "名",
        "Title": "職稱",
        "TitleOfCourtesy": "稱謂",
        "BirthDate": "生日",
        "HireDate": "到職日",
        "Address": "地址",
        "City": "城市",
        "Region": "地區",
        "PostalCode": "郵遞區號",
        "Country": "國家",
        "HomePhone": "住家電話",
        "Extension": "分機",
        "Notes": "備註"
    },

    "Products": {
        "ProductID": "產品編號",
        "ProductName": "產品名稱",
        "SupplierID": "供應商編號",
        "CategoryID": "產品類別",
        "QuantityPerUnit": "包裝規格",
        "UnitPrice": "單價",
        "UnitsInStock": "庫存數量",
        "UnitsOnOrder": "訂購中數量",
        "ReorderLevel": "補貨水位",
        "Discontinued": "是否停售"
    },

    "Categories": {
        "CategoryID": "類別編號",
        "CategoryName": "類別名稱",
        "Description": "類別說明"
    },

    "Suppliers": {
        "SupplierID": "供應商編號",
        "CompanyName": "公司名稱",
        "ContactName": "聯絡人",
        "ContactTitle": "職稱",
        "Address": "地址",
        "City": "城市",
        "Region": "地區",
        "PostalCode": "郵遞區號",
        "Country": "國家",
        "Phone": "電話",
        "Fax": "傳真",
        "HomePage": "公司網站"
    },

    "Shippers": {
        "ShipperID": "貨運商編號",
        "CompanyName": "貨運公司",
        "Phone": "電話"
    }
}

# ===============================
# FastAPI 初始化
# ===============================
app = FastAPI(title="Quick DB Viewer")

# 允許前端 HTML 呼叫
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# ===============================
# SQL Server 連線設定
# ⚠️ 若你不是本機，請改 SERVER / DATABASE
# ===============================
conn_str = (
    "DRIVER={ODBC Driver 17 for SQL Server};"
    "SERVER=localhost;"
    "DATABASE=Northwind;"
    "Trusted_Connection=yes;"
)

# ===============================
# 共用查詢函式（含中文轉換）
# ===============================
def run_query(sql, table_name=None):
    with pyodbc.connect(conn_str) as conn:
        cursor = conn.cursor()
        cursor.execute(sql)

        columns = [c[0] for c in cursor.description]
        rows = cursor.fetchall()

        col_map = COLUMN_NAME_MAP.get(table_name, {})

        result = []
        for row in rows:
            item = {}
            for col, val in zip(columns, row):
                zh_col = col_map.get(col, col)  # 找不到就用英文
                item[zh_col] = val
            result.append(item)

        return result

# ===============================
# API：列出所有資料表
# ===============================
@app.get("/tables")
def get_tables():
    return run_query("""
        SELECT TABLE_NAME
        FROM INFORMATION_SCHEMA.TABLES
        WHERE TABLE_TYPE = 'BASE TABLE'
        ORDER BY TABLE_NAME
    """)

# ===============================
# API：取得指定資料表資料（前 50 筆）
# ===============================
@app.get("/data/{table}")
def get_table_data(table: str):
    sql = f"SELECT TOP 100 * FROM dbo.[{table}]"
    return run_query(sql, table_name=table)

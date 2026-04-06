import os
def get_jdbc_url():
    db_host = "postgres-gold"
    db_port = "5432" # Gọi nội bộ giữa các container trong Docker luôn dùng cổng gốc 5432
    db_name = "gold_layer"
    return f"jdbc:postgresql://{db_host}:{db_port}/{db_name}"

def get_postgres_properties():
    return {
        "user": "admin",       # Thay bằng user DB của bạn
        "password": "password",   # Thay bằng password DB của bạn
        "driver": "org.postgresql.Driver",
        "stringtype": "unspecified" # Giúp Spark tự động ép kiểu string sang varchar/text
    }

def write_to_postgres(df, table_name, mode="append"):
    jdbc_url = get_jdbc_url()
    properties = get_postgres_properties()
    
    print(f"[*] Đang đẩy dữ liệu vào bảng {table_name}...")
    df.write.jdbc(
        url=jdbc_url,
        table=table_name,
        mode=mode,
        properties=properties
    )
    print(f"[+] Thành công ghi vào bảng: {table_name}")
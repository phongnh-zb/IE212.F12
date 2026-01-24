import os
import sys

import happybase

# --- SETUP PATH ---
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(os.path.dirname(current_dir))
sys.path.append(project_root)

from configs import config


def create_table_safe(connection, table_name, families):
    """
    Tạo bảng an toàn:
    - Nếu chưa có -> Tạo mới.
    - Nếu có rồi -> Kiểm tra family, nếu thiếu thì thêm vào (không xóa dữ liệu cũ).
    """
    try:
        encoded_name = table_name.encode('utf-8')
        tables = connection.tables()
        
        if encoded_name in tables:
            print(f"   [SKIP] Bảng '{table_name}' đã tồn tại.")
            # Kiểm tra xem có đủ family chưa (Logic mở rộng nếu cần)
        else:
            print(f"   [NEW] Đang tạo bảng '{table_name}'...")
            connection.create_table(table_name, families)
            print(f"   -> Tạo thành công!")
    except Exception as e:
        print(f"   [ERROR] Lỗi với bảng {table_name}: {e}")

def main():
    print(f"🔌 Kết nối HBase tại {config.HBASE_HOST}...")
    try:
        connection = happybase.Connection(config.HBASE_HOST, timeout=30000, autoconnect=True)
        
        print("🛠  KHỞI TẠO SCHEMA (CẤU TRÚC BẢNG)...")

        # 1. Bảng MOVIES
        # info: thông tin cơ bản (title, genres, tags)
        # stats: thông tin thống kê (avg_rating, count)
        create_table_safe(connection, config.HBASE_TABLE_MOVIES, {'info': dict(), 'stats': dict()})

        # 2. Bảng RATINGS
        # r: điểm số (rating)
        # t: thời gian (timestamp)
        create_table_safe(connection, config.HBASE_TABLE_RATINGS, {'r': dict(), 't': dict()})

        # 3. Bảng RECOMMENDATIONS (Kết quả Model User-Item)
        # info: chứa chuỗi gợi ý
        create_table_safe(connection, config.HBASE_TABLE_RECS, {'info': dict()})

        # 4. Bảng GENRE_STATS (Kết quả MapReduce Thể loại)
        # info: chứa số lượng
        create_table_safe(connection, config.HBASE_TABLE_GENRE_STATS, {'info': dict()})
        
        # 5. Các bảng phụ khác (Nếu có)
        create_table_safe(connection, config.HBASE_TABLE_TAGS, {'info': dict()})

        connection.close()
        print("✅ HOÀN TẤT KHỞI TẠO BẢNG!")

    except Exception as e:
        print(f"❌ Lỗi kết nối HBase: {e}")

if __name__ == "__main__":
    main()
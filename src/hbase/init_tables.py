import os
import sys

import happybase

# --- SETUP PATH ---
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(os.path.dirname(current_dir))
sys.path.append(project_root)

from configs import config


def create_or_recreate_table(connection, table_name, desired_families):
    """
    Chiến lược khởi tạo bảng Mạnh tay (Robust Initialization):
    1. Nếu chưa có -> Tạo mới.
    2. Nếu đã có nhưng thiếu family -> XÓA BẢNG CŨ và TẠO LẠI (để tránh lỗi thư viện Thrift).
    """
    try:
        encoded_name = table_name.encode('utf-8')
        existing_tables = connection.tables()
        
        if encoded_name in existing_tables:
            print(f"   ℹ️ [CHECK] Bảng '{table_name}' đã tồn tại. Kiểm tra schema...")
            
            # Lấy thông tin schema hiện tại
            table_instance = connection.table(encoded_name)
            existing_families = set(f.decode('utf-8') for f in table_instance.families().keys())
            
            # Kiểm tra xem có thiếu family nào không
            missing_families = []
            for needed_fam in desired_families.keys():
                if needed_fam not in existing_families:
                    missing_families.append(needed_fam)
            
            if missing_families:
                print(f"   ⚠️ [MISMATCH] Bảng '{table_name}' thiếu family: {missing_families}.")
                print(f"   ♻️ [ACTION] Đang xóa bảng cũ và tạo lại để cập nhật schema...")
                
                # Vô hiệu hóa và xóa bảng
                connection.disable_table(encoded_name)
                connection.delete_table(encoded_name)
                
                # Tạo lại bảng với đầy đủ schema
                connection.create_table(encoded_name, desired_families)
                print(f"   ✅ [DONE] Đã tái tạo bảng '{table_name}' thành công!")
            else:
                print(f"   ✅ [OK] Schema hợp lệ.")
                
        else:
            # Tạo mới hoàn toàn
            print(f"   🆕 [NEW] Đang tạo mới bảng '{table_name}'...")
            connection.create_table(table_name, desired_families)
            print(f"   ✅ [DONE] Tạo thành công!")

    except Exception as e:
        print(f"   ❌ [ERROR] Lỗi khi xử lý bảng {table_name}: {e}")

def main():
    port = getattr(config, 'HBASE_PORT', 9090)
    print(f"🔌 Đang kết nối HBase Thrift Server tại {config.HBASE_HOST}:{port}...")
    connection = None
    try:
        connection = happybase.Connection(config.HBASE_HOST, port=port, timeout=60000, autoconnect=True)
        
        print("\n🛠  BẮT ĐẦU KHỞI TẠO BẢNG HBASE...")
        print("="*60)

        # 1. Bảng MOVIES
        create_or_recreate_table(connection, config.HBASE_TABLE_MOVIES, {'info': dict(), 'stats': dict()})

        # 2. Bảng RATINGS
        create_or_recreate_table(connection, config.HBASE_TABLE_RATINGS, {'r': dict(), 't': dict()})

        # 3. Bảng RECOMMENDATIONS
        create_or_recreate_table(connection, config.HBASE_TABLE_RECS, {'info': dict()})

        # 4. Bảng GENRE_STATS
        create_or_recreate_table(connection, config.HBASE_TABLE_GENRE_STATS, {'info': dict()})
        
        # 5. Bảng MODEL_METRICS (QUAN TRỌNG: Có thêm family 'b')
        create_or_recreate_table(connection, config.HBASE_TABLE_MODEL_METRICS, {
            'info': dict(),
            'b': dict()
        })
        
        # 6. Bảng RATING_STATS
        create_or_recreate_table(connection, config.HBASE_TABLE_RATING_STATS, {'info': dict()})
        
        # 7. Bảng SYSTEM_STATS
        create_or_recreate_table(connection, config.HBASE_TABLE_SYSTEM_STATS, {'info': dict()})

        # 8. Bảng TAGS
        create_or_recreate_table(connection, config.HBASE_TABLE_TAGS, {'info': dict()})

        print("="*60)
        print("\n✅ HOÀN TẤT! HỆ THỐNG ĐÃ SẴN SÀNG.")

    except Exception as e:
        print(f"\n❌ Lỗi kết nối HBase: {e}")
    finally:
        if connection:
            connection.close()

if __name__ == "__main__":
    main()
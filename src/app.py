import os
import sys
import time

import pandas as pd
import streamlit as st

# --- SETUP PATH (Để import được config & utils) ---
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(current_dir)
sys.path.append(project_root)

# Import module nội bộ
from configs import config
from src.utils.hbase_utils import HBaseProvider

# --- 1. CONFIG TRANG ---
st.set_page_config(
    page_title="MovieLens Big Data System",
    page_icon="🎬",
    layout="wide",
    initial_sidebar_state="expanded"
)

# --- 2. CACHE CONNECTION (Quan trọng) ---
# Dùng @st.cache_resource cho các object kết nối (Database, Socket, Model)
# Giúp không phải connect lại mỗi khi user bấm nút.
@st.cache_resource
def get_provider():
    try:
        provider = HBaseProvider()
        # Test connect nhẹ một cái để chắc chắn
        provider.connect()
        return provider
    except Exception as e:
        st.error(f"❌ Không thể kết nối HBase: {e}")
        return None

# --- 3. CACHE DATA (Quan trọng) ---
# Dùng @st.cache_data cho dữ liệu tải về (DataFrame, List, Json)
# TTL=600 nghĩa là cache này sống 10 phút, sau đó sẽ tự clear để lấy data mới.
@st.cache_data(ttl=600)
def load_recommendations(user_id):
    provider = get_provider()
    if provider:
        return provider.get_recommendations(user_id)
    return []

# --- 4. GIAO DIỆN (UI) ---
def main():
    st.title("🎬 MovieLens Recommender System")
    st.caption("Powered by: Hadoop HDFS + Spark ALS + HBase")

    # Sidebar
    with st.sidebar:
        st.header("🔍 User Control")
        user_input = st.text_input("Nhập User ID:", value="1")
        btn_reload = st.button("Lấy Gợi Ý (Refresh)")
        
        st.markdown("---")
        st.info("💡 **Note:** Data được lấy trực tiếp từ HBase và cache trong 10 phút.")

    # Logic hiển thị
    if user_input:
        if not user_input.isdigit():
            st.error("Vui lòng nhập User ID là số.")
            return

        # Hiển thị loading bar
        with st.spinner(f"Đang truy vấn HBase cho User {user_input}..."):
            start_time = time.time()
            
            # Gọi hàm có cache
            recs = load_recommendations(user_input)
            
            duration = time.time() - start_time

        # Hiển thị kết quả
        if recs:
            col1, col2 = st.columns([3, 1])
            with col1:
                st.success(f"✅ Tìm thấy {len(recs)} phim gợi ý (Thời gian: {duration:.3f}s)")
            with col2:
                # Nút clear cache nếu muốn lấy dữ liệu nóng hổi ngay lập tức
                if st.button("Xóa Cache"):
                    load_recommendations.clear()
                    st.rerun()

            # Chuyển list dict thành DataFrame đẹp
            df = pd.DataFrame(recs)
            
            df = df.rename(columns={
                "movieId": "ID",
                "title": "Tên Phim",
                "genres": "Thể Loại",
                "avg_rating": "Điểm TB (Hadoop)",
                "pred_rating": "Độ Phù Hợp"
            })

            # Hiển thị bảng
            st.dataframe(
                df,
                column_config={
                    "Điểm TB (Hadoop)": st.column_config.NumberColumn(
                        format="%.1f ⭐",
                        help="Điểm đánh giá trung bình của cộng đồng"
                    ),
                    "Độ Phù Hợp": st.column_config.NumberColumn(
                        format="%.1f 🔥",
                        help="Hệ thống dự đoán bạn sẽ thích phim này bao nhiêu điểm"
                    ),
                },
                use_container_width=True,
                hide_index=True
            )
            
            # (Option) Vẽ biểu đồ đơn giản nếu có điểm
            if 'Điểm TB (Hadoop)' in df.columns:
                # Convert sang số để vẽ (vì từ HBase ra là string)
                df["rating_num"] = pd.to_numeric(df["Điểm TB (Hadoop)"], errors='coerce')
                st.bar_chart(df.set_index("Tên Phim")["rating_num"])
                
            if 'Độ Phù Hợp' in df.columns: # <--- Sửa cả ở đây nữa
                df["pred_num"] = pd.to_numeric(df["Độ Phù Hợp"], errors='coerce')
                st.caption("Biểu đồ phân bố độ phù hợp:")
                st.bar_chart(df.set_index("Tên Phim")["pred_num"])

        else:
            st.warning(f"⚠️ Không tìm thấy gợi ý nào cho User ID: {user_input}")
            st.markdown("""
            **Nguyên nhân có thể:**
            1. User này chưa có trong tập train.
            2. Bạn chưa chạy `python src/run_training.py`.
            3. HBase chưa khởi động xong.
            """)

if __name__ == "__main__":
    main()
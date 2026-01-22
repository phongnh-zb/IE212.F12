import os
import sys

import altair as alt
import pandas as pd
import streamlit as st

# --- SETUP PATH ---
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(current_dir)
sys.path.append(project_root)

from configs import config
from src.utils.hbase_utils import HBaseProvider

# --- CONFIG TRANG ---
st.set_page_config(page_title="Hệ Thống Gợi Ý Phim MovieLens", page_icon="🎬", layout="wide")

# --- CACHE CONNECTION ---
@st.cache_resource
def get_provider():
    try:
        provider = HBaseProvider()
        provider.connect()
        return provider
    except Exception as e:
        st.error(f"❌ Không thể kết nối HBase: {e}")
        return None

# --- CACHE DATA ---
@st.cache_data(ttl=600)
def load_recommendations(user_id):
    provider = get_provider()
    if provider: return provider.get_recommendations(user_id)
    return []

@st.cache_data(ttl=60)
def load_all_system_data(limit=100):
    provider = get_provider()
    if provider: return provider.scan_recommendations(limit=limit)
    return []

# --- UI MAIN ---
def main():
    st.title("🎬 Hệ Thống Gợi Ý Phim MovieLens")
    st.caption("Nền tảng: Hadoop HDFS + Spark ALS + HBase")

    # TABS
    tab1, tab2 = st.tabs(["🔍 Gợi Ý Cá Nhân", "📊 Dữ Liệu Hệ Thống"])

    # ==========================================
    # TAB 1: USER VIEW (LAYOUT MỚI)
    # ==========================================
    with tab1:
        # Layout: 2 cột trên, 2 cột dưới
        col_top_left, col_top_right = st.columns([1, 2])
        
        recs = [] 
        
        # --- CỘT TRÁI: INPUT ---
        with col_top_left:
            st.info("Nhập ID của bạn để nhận gợi ý phim phù hợp nhất.")
            user_input = st.text_input("Nhập User ID:", value="1")
            
            if user_input and user_input.isdigit():
                with st.spinner(f"AI đang phân tích sở thích User {user_input}..."):
                    recs = load_recommendations(user_input)
                
                if recs:
                    st.success(f"✅ Tìm thấy {len(recs)} phim phù hợp!")
                    if st.button("Xóa Cache User"):
                        load_recommendations.clear()
                        st.rerun()
                else:
                    st.warning("⚠️ Không tìm thấy dữ liệu gợi ý.")
            elif user_input:
                st.error("Vui lòng nhập User ID là số.")

        # --- CỘT PHẢI: INTERACTIVE TABLE ---
        selected_movie_data = None # Biến lưu phim đang được chọn

        with col_top_right:
            if recs:
                df = pd.DataFrame(recs)
                # Đổi tên cột hiển thị
                df_display = df.rename(columns={
                    "movieId": "ID", "title": "Tên Phim", "genres": "Thể Loại",
                    "avg_rating": "Điểm Cộng Đồng", "pred_rating": "Độ Phù Hợp"
                })
                
                # Convert số liệu hiển thị
                df_display["Điểm Cộng Đồng"] = pd.to_numeric(df_display["Điểm Cộng Đồng"], errors='coerce').fillna(0)
                df_display["Độ Phù Hợp"] = pd.to_numeric(df_display["Độ Phù Hợp"], errors='coerce').clip(0, 10)

                st.caption("📋 Danh sách phim đề xuất (Click vào dòng để xem chi tiết):")
                
                # [QUAN TRỌNG] Bảng tương tác
                event = st.dataframe(
                    df_display,
                    column_config={
                        "Điểm Cộng Đồng": st.column_config.NumberColumn(format="%.1f ⭐"),
                        "Độ Phù Hợp": st.column_config.NumberColumn(format="%.1f 🔥"),
                    },
                    use_container_width=True, 
                    hide_index=True,
                    on_select="rerun",           # Rerun app khi click
                    selection_mode="single-row"  # Chỉ cho chọn 1 dòng
                )
                
                # --- LOGIC LẤY PHIM ĐƯỢC CHỌN ---
                # Nếu user click chọn dòng -> Lấy dòng đó
                if len(event.selection.rows) > 0:
                    selected_index = event.selection.rows[0]
                    # Lấy data gốc từ list 'recs' dựa theo index dòng
                    selected_movie_data = recs[selected_index]
                else:
                    # Mặc định: Nếu chưa chọn gì thì lấy phim đầu tiên (Index 0)
                    selected_movie_data = recs[0]

            else:
                st.info("👈 Kết quả sẽ hiển thị tại đây sau khi bạn nhập User ID.")

        # --- HÀNG DƯỚI: CHI TIẾT & BIỂU ĐỒ ---
        if recs and selected_movie_data:
            st.markdown("---")
            col_bot_left, col_bot_right = st.columns([1, 2])
            
            # 3. GÓC DƯỚI TRÁI: CHI TIẾT PHIM (Tự động update theo table)
            with col_bot_left:
                st.subheader(f"🎬 {selected_movie_data['title']}") # Hiện tên phim lên title luôn
                
                # Query chi tiết từ HBase (Lấy data tươi nhất)
                details = get_provider().get_movie_details(selected_movie_data['movieId'])
                
                if details:
                    st.write(f"**Thể loại:** {details['genres']}")
                    # (Có thể thêm Đạo diễn, Năm SX nếu có trong bảng movies)
                    
                    m1, m2 = st.columns(2)
                    with m1:
                        st.metric("Điểm Cộng Đồng", f"{float(details['avg_rating']):.1f} ⭐")
                    with m2:
                        pred_score = float(selected_movie_data.get('pred_rating', 0))
                        st.metric("Độ Phù Hợp", f"{pred_score:.1f} 🔥")
                        
                    with st.expander("📝 Xem mô tả nội dung", expanded=True):
                         # Giả lập mô tả
                        st.caption(f"Bạn đang xem thông tin chi tiết của bộ phim '{details['title']}'. Đây là một trong những bộ phim được thuật toán gợi ý dựa trên lịch sử đánh giá của bạn.")
                else:
                    st.error("Không tải được thông tin chi tiết.")

            # 4. GÓC DƯỚI PHẢI: BIỂU ĐỒ (Highlight phim đang chọn)
            with col_bot_right:
                st.subheader("📈 Phân Tích Độ Phù Hợp")
                
                # Tạo bản sao DataFrame để vẽ
                df_chart = df_display.copy()
                
                # Tạo cột màu sắc: Phim đang chọn màu Đỏ, còn lại màu Xám
                df_chart['color'] = 'Các phim khác'
                # Dùng Tên Phim làm key để đánh dấu (hoặc dùng ID nếu muốn chính xác tuyệt đối)
                df_chart.loc[df_chart['ID'] == selected_movie_data['movieId'], 'color'] = 'Phim Đang Chọn'

                chart = alt.Chart(df_chart).mark_circle(size=150).encode(
                    x=alt.X('Điểm Cộng Đồng', scale=alt.Scale(domain=[0, 5]), title='Điểm Cộng Đồng'),
                    y=alt.Y('Độ Phù Hợp', scale=alt.Scale(domain=[0, 10]), title='Độ Phù Hợp'),
                    
                    # Tô màu theo trạng thái chọn
                    color=alt.Color('color', scale=alt.Scale(domain=['Phim Đang Chọn', 'Các phim khác'], range=['#ff2b2b', '#d3d3d3']), legend=None),
                    
                    # Tooltip
                    tooltip=['Tên Phim', 'Thể Loại', 'Điểm Cộng Đồng', 'Độ Phù Hợp']
                ).interactive()

                st.altair_chart(chart, use_container_width=True)
                

    # ==========================================
    # TAB 2: ADMIN VIEW (Giữ nguyên)
    # ==========================================
    with tab2:
        st.header("📊 Giám Sát Dữ Liệu Trực Tiếp")
        
        col_search, col_btn = st.columns([3, 1], vertical_alignment="bottom")
        
        with col_search:
            search_query = st.text_input("🔎 Lọc (User ID / Tên Phim):", value="", placeholder="Nhập từ khóa...")
            
        with col_btn:
            if st.button("🔄 Làm Mới Dữ Liệu", use_container_width=True):
                load_all_system_data.clear()
                st.rerun()

        with st.spinner("Đang tải dữ liệu hệ thống..."):
            all_data = load_all_system_data(limit=100)
        
        if all_data:
            df_all = pd.DataFrame(all_data)
            
            if search_query:
                try:
                    if "Recommendations (Details)" in df_all.columns:
                        mask = (
                            df_all["User ID"].astype(str).str.contains(search_query, case=False) | 
                            df_all["Recommendations (Details)"].astype(str).str.contains(search_query, case=False)
                        )
                        df_filtered = df_all[mask]
                    else:
                        df_filtered = df_all
                except: df_filtered = df_all
            else:
                df_filtered = df_all

            if not df_filtered.empty:
                st.dataframe(
                    df_filtered,
                    use_container_width=True, 
                    column_config={
                        "User ID": st.column_config.TextColumn("User ID", width=80),
                        "Total": st.column_config.NumberColumn("Số Lượng Phim", format="%d", width=80),
                        "Recommendations (Details)": st.column_config.TextColumn("Chi Tiết Gợi Ý", width=800)
                    },
                    hide_index=True
                )
                st.caption(f"Đang hiển thị {len(df_filtered)} bản ghi.")
            else:
                st.warning(f"🚫 Không tìm thấy kết quả nào khớp với: '{search_query}'")
        else:
            load_all_system_data.clear()
            st.info("📭 Hệ thống chưa có dữ liệu. Vui lòng bấm 'Làm Mới Dữ Liệu'.")

if __name__ == "__main__":
    main()
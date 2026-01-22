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
        col_top_left, col_top_right = st.columns([1, 2])
        
        recs = [] 
        user_history = {} # [MỚI] Biến lưu lịch sử đánh giá
        
        # --- CỘT TRÁI: INPUT ---
        with col_top_left:
            st.info("Nhập ID của bạn để nhận gợi ý phim phù hợp nhất.")
            user_input = st.text_input("Nhập ID Người Dùng:", value="1")
            
            if user_input and user_input.isdigit():
                with st.spinner(f"AI đang phân tích sở thích User {user_input}..."):
                    # 1. Lấy gợi ý (AI)
                    recs = load_recommendations(user_input)
                    # 2. [MỚI] Lấy lịch sử đánh giá thật của User (HBase)
                    # (Lưu ý: Không cache cái này lâu vì user có thể vừa mới rate xong)
                    user_history = get_provider().get_user_ratings(user_input)
                
                if recs:
                    st.success(f"✅ Tìm thấy {len(recs)} phim phù hợp!")
                else:
                    st.warning("⚠️ Không tìm thấy dữ liệu gợi ý.")
            elif user_input:
                st.error("Vui lòng nhập User ID là số.")

        # --- CỘT PHẢI: INTERACTIVE TABLE ---
        selected_movie_data = None 

        with col_top_right:
            st.subheader("📋 Danh sách phim đề xuất")

            if recs:
                df = pd.DataFrame(recs)
                df_display = df.rename(columns={
                    "movieId": "ID", "title": "Tên Phim", "genres": "Thể Loại",
                    "avg_rating": "Điểm Cộng Đồng", "pred_rating": "Độ Phù Hợp"
                })
                
                # Convert số liệu
                df_display["Điểm Cộng Đồng"] = pd.to_numeric(df_display["Điểm Cộng Đồng"], errors='coerce').fillna(0)
                df_display["Độ Phù Hợp"] = pd.to_numeric(df_display["Độ Phù Hợp"], errors='coerce').clip(0, 5)

                # --- [LOGIC MỚI] MAP ĐIỂM CỦA CHÍNH USER VÀO BẢNG ---
                def format_my_rating(mid):
                    val = user_history.get(str(mid))
                    if val:
                        # Nếu có điểm -> Format số + Emoji (Ví dụ: "4.5 👤")
                        return f"{float(val):.1f} 👤"
                    # Nếu không có -> Trả về "--"
                    return "--"

                df_display["Điểm Của Bạn"] = df_display["ID"].apply(format_my_rating)

                st.caption("Click vào dòng để xem chi tiết của phim")
                
                event = st.dataframe(
                    df_display,
                    column_config={
                        "Điểm Cộng Đồng": st.column_config.NumberColumn(format="%.1f ⭐"),
                        "Độ Phù Hợp": st.column_config.NumberColumn(format="%.1f 🔥", help="AI dự đoán bạn sẽ thích"),
                        "Điểm Của Bạn": st.column_config.TextColumn(
                            "Điểm Của Bạn",
                            help="Điểm thực tế bạn đã chấm (hiển thị '--' nếu chưa chấm)",
                            width="small" # Thu gọn cột này lại cho đẹp
                        )
                    },
                    use_container_width=True, 
                    hide_index=True,
                    on_select="rerun",           
                    selection_mode="single-row"  
                )
                
                # ... (Phần xử lý selected_movie_data giữ nguyên như cũ) ...
                if len(event.selection.rows) > 0:
                    selected_index = event.selection.rows[0]
                    selected_movie_data = recs[selected_index]
                    # Bổ sung thông tin "Điểm Của Bạn" vào data selected để dùng bên dưới
                    my_rate = user_history.get(str(selected_movie_data['movieId']))
                    selected_movie_data['my_rating'] = my_rate if my_rate else "Chưa xem"
                else:
                    selected_movie_data = recs[0]
                    my_rate = user_history.get(str(selected_movie_data['movieId']))
                    selected_movie_data['my_rating'] = my_rate if my_rate else "Chưa xem"

            else:
                st.info("👈 Kết quả sẽ hiển thị tại đây sau khi bạn nhập User ID.")

        # --- HÀNG DƯỚI: CHI TIẾT & BIỂU ĐỒ ---
        if recs and selected_movie_data:
            st.markdown("---")
            col_bot_left, col_bot_right = st.columns([1, 2])
            
            with col_bot_left:
                st.subheader(f"🎬 {selected_movie_data['title']}")
                
                details = get_provider().get_movie_details(selected_movie_data['movieId'])
                
                if details:
                    st.write(f"**Thể loại:** {details['genres']}")
                    
                    # [UPDATE] Hiển thị 3 chỉ số thay vì 2
                    m1, m2, m3 = st.columns(3)
                    with m1:
                        st.metric("Điểm Cộng Đồng", f"{float(details['avg_rating']):.1f} ⭐")
                    with m2:
                        pred_score = float(selected_movie_data.get('pred_rating', 0))
                        st.metric("Độ Phù Hợp", f"{pred_score:.1f} 🔥")
                    with m3:
                        # Hiển thị điểm thật của user
                        my_r = selected_movie_data.get('my_rating')
                        val_str = f"{float(my_r):.1f} 👤" if my_r != "Chưa xem" else "--"
                        st.metric("Điểm Của Bạn", val_str)
                        
                    with st.expander("📝 Xem mô tả nội dung", expanded=True):
                        st.caption(f"Thông tin chi tiết phim '{details['title']}'...")
                else:
                    st.error("Không tải được thông tin chi tiết.")

            # 4. GÓC DƯỚI PHẢI: BIỂU ĐỒ (DUMBBELL CHART)
            with col_bot_right:
                st.subheader("📊 So Sánh: Bạn vs Cộng Đồng")
                
                # Chuẩn bị dữ liệu cho Altair
                # Chúng ta cần highlight phim đang chọn
                df_chart = df_display.copy()
                
                # Tạo màu sắc: Phim đang chọn thì đậm hơn, phim khác thì mờ đi
                df_chart['opacity'] = 0.3
                df_chart.loc[df_chart['ID'] == selected_movie_data['movieId'], 'opacity'] = 1.0
                
                # Sắp xếp theo Độ phù hợp giảm dần để phim hợp nhất nằm trên cùng
                
                # --- VẼ BIỂU ĐỒ DUMBBELL (QUẢ TẠ) ---
                
                # 1. Tạo trục Y là Tên Phim
                base = alt.Chart(df_chart).encode(
                    y=alt.Y('Tên Phim', sort='-x', axis=alt.Axis(title=None, labelLimit=200)),
                )

                # 2. Vẽ đường nối (Thanh ngang)
                rule = base.mark_rule(color="#525252").encode(
                    x=alt.X('Điểm Cộng Đồng', scale=alt.Scale(domain=[0, 5]), title=''),
                    x2='Độ Phù Hợp',
                    opacity='opacity'
                )

                # 3. Vẽ điểm Cộng Đồng (Màu Xám)
                p_community = base.mark_circle(size=100, color='#bdc3c7', opacity=1).encode(
                    x='Điểm Cộng Đồng',
                    tooltip=['Tên Phim', 'Điểm Cộng Đồng']
                )

                # 4. Vẽ điểm AI Dự Đoán (Màu Đỏ/Cam)
                p_ai = base.mark_circle(size=150, color='#e74c3c', opacity=1).encode(
                    x='Độ Phù Hợp',
                    tooltip=['Tên Phim', 'Độ Phù Hợp'],
                    opacity='opacity' # Chỉ làm mờ điểm đỏ nếu không được chọn
                )
                
                # 5. (Tùy chọn) Highlight phim đang chọn bằng mũi tên hoặc text
                # Ở đây ta dùng opacity đã set ở trên để làm nổi bật

                # Kết hợp các layer
                chart = (rule + p_community + p_ai).properties(height=400) # Tăng chiều cao để dễ đọc tên phim

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
                        "User ID": st.column_config.TextColumn("ID Người Dùng", width=80),
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
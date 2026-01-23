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
st.set_page_config(page_title="Hệ thống gợi ý phim thông minh sử dụng Big Data", page_icon="🎬", layout="wide")

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

@st.cache_data(ttl=600)
def load_user_history(user_id):
    provider = get_provider()
    if provider: return provider.get_user_history_detailed(user_id)
    return []

@st.cache_data(ttl=60)
def load_all_system_data(limit=100):
    provider = get_provider()
    if provider: return provider.scan_recommendations(limit=limit)
    return []

@st.cache_data(ttl=600)
def load_genre_stats():
    provider = get_provider()
    if provider: return provider.get_genre_stats()
    return []

# --- UI MAIN ---
def main():
    st.title("🎬 Hệ thống gợi ý phim thông minh sử dụng Big Data")
    st.caption("Ứng dụng công nghệ xử lý dữ liệu lớn (Spark ALS, Hadoop HDFS, HBase) để phân tích hành vi người dùng và đưa ra các đề xuất điện ảnh cá nhân hóa.")

    # TABS
    tab1, tab2, tab3 = st.tabs(["🔍 Gợi Ý Cá Nhân", "📜 Lịch Sử Đánh Giá", "📊 Dữ Liệu Hệ Thống"])

    # ==========================================
    # TAB 1: USER VIEW
    # ==========================================
    with tab1:
        col_top_left, col_top_right = st.columns([1, 2])
        
        recs = [] 
        user_history = {}
        
        with col_top_left:
            st.info("Nhập ID của bạn để nhận gợi ý phim phù hợp nhất.")
            user_input = st.text_input("Nhập ID Người Dùng (User ID):", value="1")
            
            # [LOGIC MỚI] Kiểm tra đầu vào
            if not user_input:
                st.error("⚠️ Vui lòng nhập User ID (không được để trống).")
            elif not user_input.isdigit():
                st.error("⚠️ Vui lòng nhập User ID là số (Ví dụ: 1, 100).")
            else:
                # Chỉ chạy khi input hợp lệ
                with st.spinner(f"AI đang phân tích sở thích người dùng {user_input}..."):
                    recs = load_recommendations(user_input)
                    user_history = get_provider().get_user_ratings(user_input)
                
                if recs:
                    st.success(f"✅ Tìm thấy {len(recs)} phim phù hợp!")
                else:
                    st.warning("⚠️ Không tìm thấy dữ liệu gợi ý cho User này.")

        selected_movie_data = None 

        with col_top_right:
            st.subheader("📋 Danh sách phim đề xuất")

            if recs:
                df = pd.DataFrame(recs)
                df.reset_index(drop=True, inplace=True)
                df.index += 1 
                df["STT"] = df.index

                df_display = df.rename(columns={
                    "movieId": "ID", "title": "Tên Phim", "genres": "Thể Loại",
                    "avg_rating": "Điểm Cộng Đồng", "pred_rating": "Độ Phù Hợp"
                })
                
                df_display["Điểm Cộng Đồng"] = pd.to_numeric(df_display["Điểm Cộng Đồng"], errors='coerce').fillna(0)
                df_display["Độ Phù Hợp"] = pd.to_numeric(df_display["Độ Phù Hợp"], errors='coerce').clip(0, 5)

                def format_my_rating(mid):
                    val = user_history.get(str(mid))
                    if val: return f"{float(val):.1f} 👤"
                    return "--"

                df_display["Điểm Của Bạn"] = df_display["ID"].apply(format_my_rating)

                cols = ["STT", "ID", "Tên Phim", "Thể Loại", "Điểm Cộng Đồng", "Độ Phù Hợp", "Điểm Của Bạn"]
                df_final = df_display[cols]

                event = st.dataframe(
                    df_final,
                    column_config={
                        "STT": st.column_config.NumberColumn("STT", width="small", format="%d"),
                        "ID": st.column_config.TextColumn("ID", width="small"),
                        "Điểm Cộng Đồng": st.column_config.NumberColumn(format="%.1f ⭐"),
                        "Độ Phù Hợp": st.column_config.NumberColumn(format="%.1f 🔥", help="AI dự đoán bạn sẽ thích"),
                        "Điểm Của Bạn": st.column_config.TextColumn("Điểm Của Bạn", width="small")
                    },
                    width='stretch', 
                    hide_index=True,
                    on_select="rerun",           
                    selection_mode="single-row"  
                )
                
                if len(event.selection.rows) > 0:
                    selected_index = event.selection.rows[0]
                    selected_movie_data = recs[selected_index]
                    my_rate = user_history.get(str(selected_movie_data['movieId']))
                    selected_movie_data['my_rating'] = my_rate if my_rate else "Chưa xem"
                else:
                    selected_movie_data = recs[0]
                    my_rate = user_history.get(str(selected_movie_data['movieId']))
                    selected_movie_data['my_rating'] = my_rate if my_rate else "Chưa xem"

            else:
                # Thông báo hướng dẫn khi chưa có dữ liệu (hoặc đang lỗi input)
                if not user_input or not user_input.isdigit():
                    st.info("👈 Vui lòng nhập ID hợp lệ bên trái để xem kết quả.")
                else:
                    st.info("📭 Không có dữ liệu hiển thị.")

        if recs and selected_movie_data:
            st.markdown("---")
            col_bot_left, col_bot_right = st.columns([1, 2])
            
            with col_bot_left:
                st.subheader(f"🎬 {selected_movie_data['title']}")
                details = get_provider().get_movie_details(selected_movie_data['movieId'])
                
                if details:
                    st.write(f"**Thể loại:** {details['genres']}")
                    
                    m1, m2, m3 = st.columns(3)
                    with m1:
                        st.metric("Điểm Cộng Đồng", f"{float(details['avg_rating']):.1f} ⭐")
                    with m2:
                        pred_score = float(selected_movie_data.get('pred_rating', 0))
                        st.metric("Độ Phù Hợp", f"{pred_score:.1f} 🔥")
                    with m3:
                        my_r = selected_movie_data.get('my_rating')
                        val_str = f"{float(my_r):.1f} 👤" if my_r != "Chưa xem" else "--"
                        st.metric("Điểm Của Bạn", val_str)
                        
                    with st.expander("📝 Xem mô tả nội dung", expanded=True):
                        st.caption(f"Thông tin chi tiết phim '{details['title']}'...")
                        r_count = details.get('rating_count', 0)
                        if int(r_count) > 0:
                            st.caption(f"*(Được đánh giá bởi {r_count} người dùng)*")

            with col_bot_right:
                st.subheader("📊 So Sánh: Bạn vs Cộng Đồng")
                
                df_chart = df_display.copy()
                df_chart['opacity'] = 0.3
                df_chart.loc[df_chart['ID'] == selected_movie_data['movieId'], 'opacity'] = 1.0
                
                base = alt.Chart(df_chart).encode(
                    y=alt.Y('Tên Phim', sort='-x', axis=alt.Axis(title=None, labelLimit=200)),
                )

                rule = base.mark_rule(color="#525252").encode(
                    x=alt.X('Điểm Cộng Đồng', scale=alt.Scale(domain=[0, 5]), title=''),
                    x2='Độ Phù Hợp',
                    opacity='opacity'
                )

                p_community = base.mark_circle(size=100, color='#bdc3c7', opacity=1).encode(
                    x='Điểm Cộng Đồng',
                    tooltip=['Tên Phim', 'Điểm Cộng Đồng']
                )

                p_ai = base.mark_circle(size=150, color='#e74c3c', opacity=1).encode(
                    x='Độ Phù Hợp',
                    tooltip=['Tên Phim', 'Độ Phù Hợp'],
                    opacity='opacity'
                )
                
                chart = (rule + p_community + p_ai).properties(height=500)
                st.altair_chart(chart, use_container_width=True)
            
    # ==========================================
    # TAB 2: LỊCH SỬ ĐÁNH GIÁ
    # ==========================================
    with tab2:
        col_hist_left, col_hist_right = st.columns([1, 3])
        
        with col_hist_left:
            st.info("Xem lại các phim người dùng đã xem.")
            hist_user_input = st.text_input("Nhập ID Người Dùng (History):", value="1")
            
            history_data = []
            
            # [LOGIC MỚI] Kiểm tra đầu vào Tab 2
            if not hist_user_input:
                st.error("⚠️ Vui lòng nhập User ID (không được để trống).")
            elif not hist_user_input.isdigit():
                st.error("⚠️ Vui lòng nhập User ID là số.")
            else:
                 with st.spinner("Đang tải lịch sử từ HBase..."):
                    history_data = load_user_history(hist_user_input)
            
            if history_data:
                df_hist = pd.DataFrame(history_data)
                avg_score = df_hist['rating'].mean()
                
                st.markdown("### 🌟 Tổng Quan")
                st.metric("Đã Đánh Giá", f"{len(df_hist)} phim")
                st.metric("Điểm Trung Bình", f"{avg_score:.1f} / 5.0")
            elif hist_user_input and hist_user_input.isdigit():
                st.warning("📭 Không tìm thấy lịch sử đánh giá cho User này.")

        with col_hist_right:
            st.subheader(f"📋 Danh sách phim đã xem")

            if history_data:
                df_hist = pd.DataFrame(history_data)
                df_hist.reset_index(drop=True, inplace=True)
                df_hist.index += 1
                df_hist["STT"] = df_hist.index
                
                hist_chart = alt.Chart(df_hist).mark_bar().encode(
                    x=alt.X('rating:O', title='Số Sao'),
                    y=alt.Y('count()', title='Số lượng phim'),
                    color=alt.Color('rating:O', scale=alt.Scale(scheme='magma'), legend=None),
                    tooltip=['rating', 'count()']
                ).properties(height=250)
                st.altair_chart(hist_chart, use_container_width=True)

                cols = ["STT", "movieId", "title", "genres", "rating"]
                df_hist = df_hist[cols]

                st.dataframe(
                    df_hist,
                    column_config={
                        "STT": st.column_config.NumberColumn("STT", width="small", format="%d"),
                        "movieId": st.column_config.TextColumn("ID", width="small"),
                        "title": "Tên Phim",
                        "genres": "Thể Loại",
                        "rating": st.column_config.NumberColumn("Điểm Chấm", format="%.1f ⭐")
                    },
                    width='stretch',
                    height=500,
                    hide_index=True
                )
            else:
                st.info("👈 Nhập User ID để xem dữ liệu.")           

    # ==========================================
    # TAB 3: DỮ LIỆU HỆ THỐNG
    # ==========================================
    with tab3:
        st.header("📊 Giám Sát Dữ Liệu Trực Tiếp")
        
        st.subheader("🍰 Phân Bố Thể Loại Phim")
        
        with st.spinner("Đang tải thống kê thể loại..."):
            genre_data = load_genre_stats()
            
        if genre_data:
            df_genre = pd.DataFrame(genre_data)
            
            # 1. Thêm cột STT cho bảng
            df_genre.reset_index(drop=True, inplace=True)
            df_genre.index += 1
            df_genre["STT"] = df_genre.index
            
            # 2. Xử lý nhãn biểu đồ
            total_movies = df_genre['count'].sum()
            threshold = total_movies * 0.03 # Ngưỡng 3%
            
            df_genre['label'] = df_genre.apply(
                lambda x: str(x['count']) if x['count'] > threshold else "", 
                axis=1
            )
            
            col_chart, col_data = st.columns([1, 1])
            
            with col_chart:
                base = alt.Chart(df_genre).encode(
                    theta=alt.Theta("count", stack=True)
                )
                
                pie = base.mark_arc(outerRadius=160).encode(
                    color=alt.Color("genre", legend=alt.Legend(title="Thể Loại", orient='left')),
                    order=alt.Order("count", sort="descending"),
                    tooltip=["genre", "count", alt.Tooltip("count", format=",")]
                )
                
                # Sử dụng cột 'label' đã lọc thay vì 'count' gốc
                text = base.mark_text(radius=180).encode(
                    text=alt.Text("label"), 
                    order=alt.Order("count", sort="descending"),
                    color=alt.value("black")  
                )
                
                st.altair_chart((pie + text).properties(height=500), use_container_width=True)
                
            with col_data:
                st.caption("Chi tiết số lượng từng thể loại:")
                
                # Reorder để STT lên đầu
                cols_genre = ["STT", "genre", "count"]
                df_genre_display = df_genre[cols_genre]

                st.dataframe(
                    df_genre_display,
                    column_config={
                        "STT": st.column_config.NumberColumn("STT", width="small", format="%d"),
                        "genre": "Thể Loại",
                        "count": st.column_config.NumberColumn("Số Phim", format="%d 🎬")
                    },
                    hide_index=True,
                    height=500
                )
        else:
            st.warning("⚠️ Chưa có dữ liệu thống kê thể loại. Hãy chạy Pipeline Bước 2.")

        st.divider()

        st.subheader("🔎 Chi Tiết Gợi Ý Phim Theo Người Dùng")
        
        search_query = st.text_input("Tìm kiếm trong bảng (User ID / Tên Phim):", placeholder="Nhập từ khóa...")
            
        with st.spinner("Đang tải dữ liệu bảng..."):
            all_data = load_all_system_data(limit=100)
        
        if all_data:
            df_all = pd.DataFrame(all_data)
            df_all.reset_index(drop=True, inplace=True)
            df_all.index += 1
            df_all["STT"] = df_all.index
            
            if search_query:
                try:
                    mask = (
                        df_all["User ID"].astype(str).str.contains(search_query, case=False) | 
                        df_all["Recommendations (Details)"].astype(str).str.contains(search_query, case=False)
                    )
                    df_filtered = df_all[mask]
                except: df_filtered = df_all
            else:
                df_filtered = df_all

            if not df_filtered.empty:
                cols = ["STT"] + [c for c in df_filtered.columns if c != "STT"]
                df_filtered = df_filtered[cols]

                st.dataframe(
                    df_filtered,
                    width='stretch', 
                    column_config={
                        "STT": st.column_config.NumberColumn("STT", width="small", format="%d"),
                        "User ID": st.column_config.TextColumn("ID Người Dùng", width=80),
                        "Total": st.column_config.NumberColumn("Số Lượng Phim", format="%d", width=80),
                        "Recommendations (Details)" : st.column_config.TextColumn("Chi Tiết Gợi Ý", width=800)
                    },
                    hide_index=True
                )
                st.caption(f"Đang hiển thị {len(df_filtered)} bản ghi mới nhất.")
            else:
                st.warning(f"🚫 Không tìm thấy kết quả nào khớp với: '{search_query}'")
        else:
            st.info("📭 Hệ thống chưa có dữ liệu.")
            
if __name__ == "__main__":
    main()
import os
import sys
from datetime import datetime

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

# --- CUSTOM CSS FOR CARDS ---
st.markdown("""
    <style>
    .plot-container {
        border: 1px solid #e6e9ef;
        border-radius: 10px;
        padding: 20px;
        background-color: #ffffff;
        box-shadow: 0 4px 6px rgba(0, 0, 0, 0.05);
    }
    </style>
""", unsafe_allow_html=True)

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

# Hàm load lịch sử
@st.cache_data(ttl=600)
def load_user_history(user_id):
    provider = get_provider()
    if provider: return provider.get_user_history_detailed(user_id)
    return []

@st.cache_data(ttl=600)
def load_all_system_data(limit=100):
    provider = get_provider()
    if provider: return provider.scan_recommendations(limit=limit)
    return []

@st.cache_data(ttl=600)
def load_genre_stats():
    provider = get_provider()
    if provider: return provider.get_genre_stats()
    return []

@st.cache_data(ttl=600)
def load_all_metrics():
    provider = get_provider()
    if provider: return provider.get_all_model_metrics()
    return []

@st.cache_data(ttl=600)
def load_latest_run_info():
    provider = get_provider()
    if provider: return provider.get_latest_run_info()
    return {}

# --- UI MAIN ---
def main():
    st.title("🎬 Hệ thống gợi ý phim thông minh sử dụng Big Data")
    
    col_t1, col_t2 = st.columns([4, 1])
    with col_t1:
        st.caption("Phân tích hành vi người dùng và đưa ra các đề xuất điện ảnh cá nhân hóa.")
    with col_t2:
        if st.button("🔄 Làm mới dữ liệu", use_container_width=True):
            st.cache_data.clear()
            st.rerun()

    # TABS
    tab0, tab1, tab2, tab3, tab4 = st.tabs(["🏠 Tổng Quan", "🔍 Gợi Ý Cá Nhân", "📜 Lịch Sử Đánh Giá", "📊 Dữ Liệu Hệ Thống", "⚖️ So Sánh Model"])

    # ==========================================
    # TAB 0: TỔNG QUAN (OVERVIEW)
    # ==========================================
    with tab0:
        st.header("📊 Hệ Thống Gợi Ý Phim (MovieLens 10M Dataset)")
        
        # --- CẬP NHẬT PHẦN NÀY ---
        # 1. Gọi hàm lấy dữ liệu thực tế từ HBase
        with st.spinner('Đang tải số liệu tổng quan từ hệ thống...'):
            overview_metrics = get_provider().get_system_overview()
            latest_run = get_provider().get_latest_run_info()
            
        # 2. Hiển thị các metric bằng dữ liệu vừa lấy được
        col1, col2, col3, col4 = st.columns(4)
        
        # Sử dụng hàm st.metric để hiển thị đẹp mắt
        col1.metric("Người Dùng", overview_metrics.get('user_count', 'N/A'))
        
        col2.metric("Tổng Số Phim", overview_metrics.get('movie_count', 'N/A'))
        
        col3.metric("Lượt Đánh Giá", overview_metrics.get('rating_count', 'N/A'))
        
        label_rmse = f"Độ Chính Xác ({latest_run.get('winner_model', 'N/A')})"
        rmse_val = latest_run.get('rmse', 'N/A')
        
        col4.metric(
            label=label_rmse, 
            value=rmse_val, 
            delta="Model Tốt Nhất", # Dòng chữ nhỏ bên dưới
            delta_color="off" # Màu xám trung tính
        )

        st.divider()

        # HÀNG 2: PHÂN TÍCH DỮ LIỆU THÔ
        col_raw1, col_raw2 = st.columns(2)
        
        with col_raw1:
            st.subheader("🔥 Top 10 Phim Phổ Biến")
            with st.container(border=True):
                top_movies = get_provider().get_top_rated_movies(limit=10)
                if not top_movies.empty:
                    chart_top = alt.Chart(top_movies).mark_bar(color='#2ecc71').encode(
                        x=alt.X('count:Q', title='Lượt đánh giá'),
                        y=alt.Y('title:N', sort='-x', title=None),
                        tooltip=['title', 'count']
                    ).properties(height=300)
                    st.altair_chart(chart_top, use_container_width=True)

        with col_raw2:
            st.subheader("⭐ Phân Bố Điểm Đánh Giá")
            with st.container(border=True):
                rating_data = get_provider().get_rating_distribution() 
                df_r = pd.DataFrame(rating_data)
                
                chart_r = alt.Chart(df_r).mark_bar(color='#f1c40f').encode(
                    # labelAngle=0 giúp chữ nằm ngang, dễ đọc hơn
                    x=alt.X('rating:N', title='Số sao', axis=alt.Axis(labelAngle=0)), 
                    y=alt.Y('count:Q', title='Số lượt đánh giá'),
                    tooltip=['rating', 'count']
                ).properties(height=300)
                st.altair_chart(chart_r, use_container_width=True)

        # HÀNG 3: KẾT QUẢ XỬ LÝ & HIỆU NĂNG
        col_res1, col_res2 = st.columns(2)

        with col_res1:
            st.subheader("🎯 Hiệu Năng Các Model")
            with st.container(border=True):
                metrics = load_all_metrics()
                if metrics:
                    df_m = pd.DataFrame(metrics)
                    df_m = df_m[df_m['model'] != 'LATEST_RUN']
                    
                    if not df_m.empty:
                        chart_m = alt.Chart(df_m).mark_line(point=True, color='#e74c3c').encode(
                            x=alt.X('model:N', title='Mô hình', axis=alt.Axis(labelAngle=0)),
                            y=alt.Y('rmse:Q', title='$RMSE$', scale=alt.Scale(zero=False)),
                            tooltip=['model', 'rmse']
                        ).properties(height=300)
                        st.altair_chart(chart_m, use_container_width=True)
                    else:
                        st.info("💡 Chưa có số liệu so sánh. Hãy huấn luyện thêm mô hình.")
                else:
                    st.warning("⚠️ Không tìm thấy dữ liệu metrics.")

        with col_res2:
            st.subheader("📄 Báo Cáo Tổng Quan Hệ Thống")
            if st.button("🛠️ Khởi tạo dữ liệu PDF"):
                # Load dữ liệu
                metrics = load_all_metrics()
                genres = load_genre_stats()
                # Lưu ý: Hàm get_system_overview() trả về User/Movie count
                sys_info = get_provider().get_system_overview() 

                try:
                    pdf_data = get_provider().generate_pdf_report(metrics, genres, sys_info)
                    
                    st.download_button(
                        label="📥 Tải Báo cáo (PDF)",
                        data=pdf_data,
                        file_name=f"Bao_Cao_{datetime.now().strftime('%Y%m%d')}.pdf",
                        mime="application/pdf"
                    )
                except Exception as e:
                    st.error(f"Lỗi: {e}")
        
    # ==========================================
    # TAB 1: USER VIEW (Gợi Ý)
    # ==========================================
    with tab1:
        col_top_left, col_top_right = st.columns([1, 2])
        
        recs = [] 
        
        with col_top_left:
            st.info("Nhập ID của bạn để nhận gợi ý phim phù hợp nhất")
            user_input = st.text_input("Nhập ID Người Dùng (User ID):", value="1")
            
            if not user_input:
                st.error("⚠️ Vui lòng nhập User ID.")
            elif not user_input.isdigit():
                st.error("⚠️ Vui lòng nhập User ID là số.")
            else:
                with st.spinner(f"AI đang phân tích sở thích người dùng {user_input}..."):
                    recs = load_recommendations(user_input)
                
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
                
                df_display["Điểm Cộng Đồng"] = df_display["Điểm Cộng Đồng"].astype(float)
                df_display["Độ Phù Hợp"] = pd.to_numeric(df_display["Độ Phù Hợp"], errors='coerce').clip(0, 5)

                cols = ["STT", "ID", "Tên Phim", "Thể Loại", "Điểm Cộng Đồng", "Độ Phù Hợp"]
                df_final = df_display[cols]

                event = st.dataframe(
                    df_final,
                    column_config={
                        "STT": st.column_config.NumberColumn("STT", width="small", format="%d"),
                        "ID": st.column_config.TextColumn("ID", width="small"),
                        "Điểm Cộng Đồng": st.column_config.NumberColumn(width="small", format="%.1f ⭐"),
                        "Độ Phù Hợp": st.column_config.NumberColumn(width="small", format="%.1f 🔥", help="AI dự đoán bạn sẽ thích"),
                    },
                    width='stretch', 
                    hide_index=True,
                    on_select="rerun",           
                    selection_mode="single-row"  
                )
                
                if len(event.selection.rows) > 0:
                    selected_index = event.selection.rows[0]
                    selected_movie_data = recs[selected_index]
                else:
                    selected_movie_data = recs[0]

            else:
                if not user_input or not user_input.isdigit():
                    st.info("👈 Vui lòng nhập ID hợp lệ.")
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
                    
                    if details.get('tags'):
                        st.caption(f"🏷️ **Từ khóa:** {details['tags']}")
                    else:
                        st.caption(f"🏷️ **Từ khóa:** Không có")
                    
                    m1, m2 = st.columns(2)
                    with m1:
                        st.metric("Điểm Cộng Đồng", f"{float(details['avg_rating']):.1f} ⭐")
                    with m2:
                        pred_score = float(selected_movie_data.get('pred_rating', 0))
                        st.metric("Độ Phù Hợp", f"{pred_score:.1f} 🔥")
                        
                    with st.expander("📝 Xem mô tả nội dung", expanded=True):
                        st.caption(f"Thông tin chi tiết phim '{details['title']}'...")
                        r_count = details.get('rating_count', 0)
                        if int(r_count) > 0:
                            st.caption(f"*(Được đánh giá bởi {r_count} người dùng)*")

            with col_bot_right:
                st.subheader("📊 So Sánh: Người Dùng vs Cộng Đồng")
                
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
            st.info("Xem lại các phim người dùng đã xem")
            hist_user_input = st.text_input("Nhập ID Người Dùng (History):", value="1")
            
            history_data = []
            
            if not hist_user_input:
                st.error("⚠️ Vui lòng nhập User ID.")
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
                
                # Thống kê nhanh Top thể loại yêu thích (Text)
                if not df_hist.empty:
                    # Tách thể loại để đếm
                    all_genres = df_hist['genres'].str.split('|').explode()
                    top_genre = all_genres.value_counts().head(1)
                    if not top_genre.empty:
                        st.metric("Thể Loại Hay Xem Nhất", top_genre.index[0], f"{top_genre.values[0]} phim")

            elif hist_user_input and hist_user_input.isdigit():
                st.warning("📭 Không tìm thấy lịch sử đánh giá của Người Dùng.")

        with col_hist_right:
            st.subheader(f"📊 Phân Tích Gu Điện Ảnh Của Người Dùng")

            if history_data:
                df_hist = pd.DataFrame(history_data)
                
                # 1. Tách chuỗi thể loại "Action|Sci-Fi" thành các dòng riêng biệt
                # Copy để không ảnh hưởng dataframe gốc
                df_exploded = df_hist.copy()
                df_exploded['genre_split'] = df_exploded['genres'].str.split('|')
                df_exploded = df_exploded.explode('genre_split')

                # 2. Tính điểm trung bình theo từng thể loại
                genre_stats = df_exploded.groupby('genre_split').agg(
                    Avg_Rating=('rating', 'mean'),
                    Count=('rating', 'count')
                ).reset_index()

                # 3. Lọc những thể loại xuất hiện ít (ví dụ < 2 lần) để chart đỡ rối (Optional)
                # genre_stats = genre_stats[genre_stats['Count'] >= 2]

                # 4. Vẽ Chart: Điểm trung bình theo thể loại
                base = alt.Chart(genre_stats).encode(
                    y=alt.Y('genre_split', sort='-x', title=None), # Sắp xếp theo điểm cao nhất
                    tooltip=['genre_split', alt.Tooltip('Avg_Rating', format='.1f'), 'Count']
                )

                bars = base.mark_bar().encode(
                    x=alt.X('Avg_Rating', title='Điểm Trung Bình', scale=alt.Scale(domain=[0, 5])),
                    color=alt.Color('Avg_Rating', scale=alt.Scale(scheme='viridis'), legend=None)
                )

                text = base.mark_text(align='left', dx=2).encode(
                    x='Avg_Rating',
                    text=alt.Text('Avg_Rating', format='.1f')
                )

                st.altair_chart((bars + text).properties(height=300, title="Điểm Đánh Giá Trung Bình Theo Thể Loại"), use_container_width=True)
                
                st.divider()
                st.subheader("📋 Chi Tiết Lịch Sử")

                df_hist.reset_index(drop=True, inplace=True)
                df_hist.index += 1
                df_hist["STT"] = df_hist.index
                
                cols = ["STT", "movieId", "title", "genres", "rating", "date"]
                df_display = df_hist[cols]

                st.dataframe(
                    df_display,
                    column_config={
                        "STT": st.column_config.NumberColumn("STT", width="small", format="%d"),
                        "movieId": st.column_config.TextColumn("ID", width="small"),
                        "title": "Tên Phim",
                        "genres": "Thể Loại",
                        "rating": st.column_config.NumberColumn("Điểm Chấm", format="%.1f ⭐"),
                        "date": st.column_config.DateColumn("Ngày Đánh Giá", format="DD/MM/YYYY")
                    },
                    width='stretch',
                    height=500,
                    hide_index=True
                )
            else:
                st.info("👈 Nhập User ID để xem phân tích.")

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
            df_genre.reset_index(drop=True, inplace=True)
            df_genre.index += 1
            df_genre["STT"] = df_genre.index
            
            # --- [MỚI] THÊM METRICS TỔNG QUAN ---
            # Tính toán các chỉ số quan trọng
            total_assignments = df_genre['count'].sum() # Tổng lượt gán
            top_genre = df_genre.iloc[0]['genre']       # Thể loại top 1 (Do data đã sort)
            top_count = df_genre.iloc[0]['count']
            avg_per_genre = df_genre['count'].mean()    # Trung bình

            # Hiển thị 3 cột chỉ số đẹp mắt
            m1, m2, m3 = st.columns(3)
            with m1:
                st.metric(
                    label="Tổng Lượt Phân Loại", 
                    value=f"{total_assignments:,.0f}",
                    help="Tổng số lần các bộ phim được gán nhãn thể loại (Một phim có thể thuộc nhiều thể loại)."
                )
            with m2:
                st.metric(
                    label="Thể Loại Phổ Biến Nhất", 
                    value=top_genre,
                    delta=f"{top_count:,.0f} phim"
                )
            with m3:
                st.metric(
                    label="Trung Bình/Thể Loại", 
                    value=f"{avg_per_genre:,.0f}",
                    help="Số lượng phim trung bình cho mỗi thể loại."
                )
            
            st.divider() # Đường kẻ phân cách
            # ----------------------------------------
            
            total_movies = df_genre['count'].sum()
            threshold = total_movies * 0.03 
            
            df_genre['label'] = df_genre.apply(
                lambda x: str(x['count']) if x['count'] > threshold else "", 
                axis=1
            )
            
            col_chart, col_data = st.columns([1, 1])
            with col_chart:
                chart = alt.Chart(df_genre).mark_arc(outerRadius=140).encode(
                    theta=alt.Theta("count:Q"),
                    color=alt.Color("genre:N", legend=alt.Legend(title="Thể Loại")),
                    tooltip=["genre:N", "count:Q"]
                ).properties(height=400)
                
                st.altair_chart(chart, use_container_width=True)
                
            with col_data:
                st.caption("Chi tiết số lượng từng thể loại:")
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
            st.warning("⚠️ Chưa có dữ liệu thống kê thể loại.")

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
            
    # ==========================================
    # TAB 4: SO SÁNH MODEL
    # ==========================================
    with tab4:
        st.header("⚖️ So Sánh Hiệu Năng Các Model")
        st.info("Biểu đồ so sánh RMSE và MAE của các mô hình đã huấn luyện")
        
        metrics_data = load_all_metrics()
        if metrics_data:
            df_metrics = pd.DataFrame(metrics_data)
            df_metrics = df_metrics[df_metrics['model'] != 'LATEST_RUN']
            
            # Show raw metrics table first for visibility      
            st.subheader("📋 Chi Tiết Số Liệu")
            st.caption("Chi tiết đánh giá của từng loại mô hình")
            st.dataframe(df_metrics, width='stretch', hide_index=True)
                   
            st.divider()
            
            c1, c2 = st.columns(2)
            with c1:
                st.subheader("📉 RMSE (Lower is better)")
                rmse_chart = alt.Chart(df_metrics).mark_bar().encode(
                    x=alt.X('model:N', title='Model'),
                    y=alt.Y('rmse:Q', title='RMSE'),
                    color=alt.Color('model:N', legend=None),
                    tooltip=['model', 'rmse']
                ).properties(height=300)
                st.altair_chart(rmse_chart, use_container_width=True)
                
            with c2:
                st.subheader("📉 MAE (Lower is better)")
                mae_chart = alt.Chart(df_metrics).mark_bar().encode(
                    x=alt.X('model:N', title='Model'),
                    y=alt.Y('mae:Q', title='MAE'),
                    color=alt.Color('model:N', legend=None),
                    tooltip=['model', 'mae']
                ).properties(height=300)
                st.altair_chart(mae_chart, use_container_width=True)
                
            # Phần dự đoán thủ công
            st.subheader("🔮 Dự Đoán Theo Model Tùy Chọn")
            st.caption("Lấy kết quả pre-calculated từ HBase cho model được chọn")
                
            selected_model = st.selectbox("Chọn Model để dự đoán:", ["als", "cbf", "hybrid"])
            u_id = st.text_input("Nhập User ID để test:", value="1")
            btn_predict = st.button("🚀 Chạy Dự Đoán")
                    
            if btn_predict:
                st.write(f"Kết quả dự đoán từ model **{selected_model.upper()}** cho User **{u_id}**:")
                        
                # Gọi get_recommendations với model_name
                test_recs = get_provider().get_recommendations(u_id, model_name=selected_model)
                        
                if test_recs:
                    st.success(f"Tìm thấy {len(test_recs)} phim gợi ý.")
                    df_pred = pd.DataFrame(test_recs)
                    st.dataframe(df_pred[["movieId", "title", "genres", "avg_rating", "pred_rating"]].rename(columns={
                        "movieId": "ID", "title": "Tên Phim", "genres": "Thể Loại", "avg_rating": "Điểm Cộng Đồng", "pred_rating": "Độ Phù Hợp"
                    }), hide_index=True)
                else:
                    st.warning(f"Không tìm thấy dữ liệu cho User {u_id} với model {selected_model.upper()}.")
                    st.info("💡 Bạn có thể cần chạy pipeline training cho model này trước.")
        else:
            st.warning("⚠️ Chưa có dữ liệu metrics trong HBase. Vui lòng chạy pipeline training.")
            
if __name__ == "__main__":
    main()

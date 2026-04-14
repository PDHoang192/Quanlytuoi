import streamlit as st
import polars as pl
import json
import re
import ast
import datetime
import plotly.express as px
import plotly.graph_objects as go

# ==============================================================================
# 🎨 CẤU HÌNH GIAO DIỆN & THẨM MỸ (CSS)
# ==============================================================================
st.set_page_config(page_title="Hệ Thống Phân Tích Tưới Thông Minh", layout="wide", page_icon="🌱")

# CSS để ứng dụng trông chuyên nghiệp và hiện đại hơn
st.markdown("""
<style>
    /* Làm đẹp các khối chỉ số (KPI) */
    [data-testid="stMetricValue"] {
        font-size: 2rem;
        color: #2E7D32;
    }
    /* Bo góc và đổ bóng cho các bảng/biểu đồ */
    .stTable, .js-plotly-plot {
        border-radius: 12px;
        overflow: hidden;
        box-shadow: 0 4px 6px rgba(0,0,0,0.1);
    }
    /* Header tùy chỉnh */
    .main-header {
        font-size: 2.5rem;
        font-weight: 800;
        color: #1B5E20;
        text-align: center;
        margin-bottom: 2rem;
        text-shadow: 1px 1px 2px rgba(0,0,0,0.1);
    }
</style>
""", unsafe_allow_html=True)

st.markdown('<p class="main-header">🌱 HỆ THỐNG PHÂN TÍCH TƯỚI THÔNG MINH</p>', unsafe_allow_html=True)

# ==============================================================================
# 🧩 CÁC HÀM XỬ LÝ DỮ LIỆU (THEO DÂY CHUYỀN)
# ==============================================================================

@st.cache_data
def loader_doc_file_tho(file_bytes):
    """
    TRẠM 1: TIẾP NHẬN & DỌN DẸP DỮ LIỆU
    Mục đích: Chuyển văn bản thô từ file thành danh sách Python sạch.
    """
    # 1. Chuyển byte thành chữ (UTF-8 để đọc được tiếng Việt)
    van_ban = file_bytes.decode("utf-8").strip()
    
    # 2. Sửa lỗi cú pháp JSON thường gặp (dùng Regex - Biểu thức chính quy)
    van_ban = re.sub(r'"\s*\n\s*"', '",\n"', van_ban)  # Sửa lỗi xuống dòng giữa các chuỗi
    van_ban = re.sub(r',\s*\}', '}', van_ban)          # Xóa dấu phẩy thừa cuối đối tượng
    van_ban = re.sub(r',\s*\]', ']', van_ban)          # Xóa dấu phẩy thừa cuối danh sách
    van_ban = re.sub(r'\}\s*\{', '},{', van_ban)       # Thêm dấu phẩy giữa các cục dữ liệu rời rạc
    
    # 3. Đảm bảo toàn bộ là một danh sách JSON chuẩn (bọc trong [ ])
    if not van_ban.startswith('['):
        van_ban = f"[{van_ban}]"
        
    try:
        # Cách 1: Thử giải mã JSON chuẩn
        return json.loads(van_ban)
    except:
        # Cách 2: Nếu thất bại (do true/false viết thường kiểu Python), dùng literal_eval
        van_ban_python = van_ban.replace('true', 'True').replace('false', 'False').replace('null', 'None')
        return ast.literal_eval(van_ban_python)

def matcher_ghep_cap_bat_tat(df_raw):
    """
    TRẠM 2: GHÉP CẶP BẬT/TẮT
    Mục đích: Tìm xem mỗi lần 'Bật' thì sau đó bao lâu máy sẽ 'Tắt'.
    """
    if df_raw.is_empty(): return None

    # Tách dữ liệu thành 2 bảng riêng: Bảng Bật và Bảng Tắt
    df_bat = df_raw.filter(pl.col("Trạng thái").str.to_uppercase() == "BẬT")
    df_tat = df_raw.filter(pl.col("Trạng thái").str.to_uppercase() == "TẮT")
    
    # Đặt tên lại cho cột thời gian ở bảng Tắt để tránh trùng
    df_tat = df_tat.select([
        pl.col("dt").alias("dt_tat"),
        pl.col("TBEC").alias("ec_tat")
    ])

    # SỬ DỤNG PHÉP GHÉP CẶP TIẾN (Forward Join): 
    # Mỗi lần BẬT sẽ tìm lần TẮT sớm nhất xuất hiện ngay sau đó
    df_pairs = df_bat.join_asof(df_tat, left_on="dt", right_on="dt_tat", strategy="forward")

    # Tính số giây chạy = Thời gian Tắt - Thời gian Bật
    df_pairs = df_pairs.with_columns([
        ((pl.col("dt_tat") - pl.col("dt")).dt.total_seconds()).alias("giay_chay"),
        pl.col("dt").dt.date().alias("ngay_chot")
    ])

    # BỘ LỌC CHẤT LƯỢNG: Chỉ lấy các lần chạy từ 20 giây đến 5 phút (tránh dữ liệu rác)
    df_pairs = df_pairs.filter((pl.col("giay_chay") >= 20) & (pl.col("giay_chay") <= 300))
    
    return df_pairs

def analytics_chia_vu_va_ngay(df_matches):
    """
    TRẠM 3: PHÂN TÍCH VỤ MÙA & TỔNG HỢP NGÀY
    Mục đích: Gom nhóm dữ liệu theo ngày và tự động nhận diện Vụ mới.
    """
    # 1. Gom dữ liệu theo từng ngày
    df_daily = df_matches.group_by("ngay_chot").agg([
        pl.count().alias("so_lan"),
        (pl.col("giay_chay").sum() / 60).round(1).alias("tong_phut"),
        pl.col("TBEC").mean().alias("ec_thuc")
    ]).sort("ngay_chot")

    # 2. Thuật toán nhận diện Vụ mới:
    # Nếu khoảng cách giữa 2 ngày tưới liên tiếp > 2 ngày -> Coi như sang Vụ mới
    df_daily = df_daily.with_columns([
        (pl.col("ngay_chot").diff().dt.total_days() > 2).fill_null(False).alias("is_new_season")
    ])
    # Đánh số ID cho Vụ bằng cách cộng dồn các dấu hiệu "Vụ mới"
    df_daily = df_daily.with_columns(pl.col("is_new_season").cum_sum().alias("season_id"))

    # 3. Tạo bảng thông tin các Vụ
    df_seasons = df_daily.group_by("season_id").agg([
        pl.col("ngay_chot").min().alias("start_date"),
        pl.col("ngay_chot").max().alias("end_date"),
        ((pl.col("ngay_chot").max() - pl.col("ngay_chot").min()).dt.total_days() + 1).alias("total_days")
    ]).filter(pl.col("total_days") >= 10).sort("start_date") # Chỉ lấy vụ dài trên 10 ngày

    return df_daily, df_seasons

# ==============================================================================
# 🖥️ GIAO DIỆN NGƯỜI DÙNG (UI)
# ==============================================================================

with st.sidebar:
    st.image("https://cdn-icons-png.flaticon.com/512/2942/2942531.png", width=100)
    st.title("⚙️ Cài Đặt")
    khu_vuc = st.selectbox("Chọn STT Khu vực:", [1, 2, 3, 4])
    
    st.divider()
    file_tuoi = st.file_uploader("📂 Tải Log Tưới (Chính)", type=['txt', 'json'])
    file_phan = st.file_uploader("🧪 Tải Log Châm Phân", type=['txt', 'json'])

if file_tuoi:
    # --- BƯỚC 1: LOAD DỮ LIỆU ---
    raw_data = loader_doc_file_tho(file_tuoi.getvalue())
    df = pl.DataFrame(raw_data)
    
    # Lọc khu vực và làm sạch thời gian
    if "STT" in df.columns:
        df = df.filter(pl.col("STT").cast(pl.Utf8).str.contains(str(khu_vuc)))
        
    if not df.is_empty():
        df = df.with_columns([
            pl.col("Thời gian").str.to_datetime("%Y-%m-%d %H-%M-%S", strict=False).alias("dt"),
            pl.col("TBEC").cast(pl.Utf8).str.replace(",", ".").cast(pl.Float64, strict=False)
        ]).drop_nulls("dt").sort("dt")

        # --- BƯỚC 2: GHÉP CẶP & PHÂN TÍCH ---
        df_matches = matcher_ghep_cap_bat_tat(df)
        if df_matches is not None:
            df_daily, df_seasons = analytics_chia_vu_va_ngay(df_matches)
            
            # --- XỬ LÝ FILE PHÂN BÓN (NẾU CÓ) ---
            df_daily = df_daily.with_columns(pl.lit(None).cast(pl.Float64).alias("ec_target"))
            if file_phan:
                try:
                    phan_data = pl.DataFrame(loader_doc_file_tho(file_phan.getvalue()))
                    if "EC yêu cầu" in phan_data.columns:
                        phan_clean = phan_data.with_columns([
                            pl.col("Thời gian").str.to_datetime("%Y-%m-%d %H-%M-%S", strict=False).dt.date().alias("ngay_chot"),
                            pl.col("EC yêu cầu").cast(pl.Utf8).str.replace(",", ".").cast(pl.Float64, strict=False)
                        ]).group_by("ngay_chot").agg(pl.col("EC yêu cầu").mean().alias("ec_target_val"))
                        
                        df_daily = df_daily.join(phan_clean, on="ngay_chot", how="left")
                        df_daily = df_daily.with_columns(pl.coalesce(["ec_target_val", "ec_target"]).alias("ec_target")).drop("ec_target_val")
                except: pass

            # --- HIỂN THỊ KẾT QUẢ ---
            seasons_list = df_seasons.to_dicts()
            if seasons_list:
                options = {f"🌱 Vụ {i+1} ({s['start_date']} -> {s['end_date']})": s['season_id'] for i, s in enumerate(seasons_list)}
                selected_season_label = st.selectbox("📅 Chọn Vụ để xem chi tiết:", list(options.keys()))
                sid = options[selected_season_label]

                # Lọc dữ liệu cho vụ được chọn
                df_view = df_daily.filter(pl.col("season_id") == sid)

                # 📊 KHU VỰC CHỈ SỐ TỔNG QUÁT (KPIs)
                col1, col2, col3, col4 = st.columns(4)
                with col1: st.metric("Tổng ngày", f"{df_seasons.filter(pl.col('season_id') == sid)['total_days'][0]} ngày")
                with col2: st.metric("TB lần tưới/ngày", f"{df_view['so_lan'].mean():.1f} lần")
                with col3: st.metric("TB phút tưới/ngày", f"{df_view['tong_phut'].mean():.1f} phút")
                with col4: st.metric("EC Trung bình", f"{df_view['ec_thuc'].mean():.2f}")

                tab1, tab2 = st.tabs(["📈 Biểu Đồ Diễn Biến", "🔍 Phân Tích Giai Đoạn"])

                with tab1:
                    # Biểu đồ 1: Số lần và Số phút
                    fig1 = go.Figure()
                    fig1.add_trace(go.Bar(x=df_view["ngay_chot"], y=df_view["so_lan"], name="Số lần tưới", marker_color='#81C784'))
                    fig1.add_trace(go.Scatter(x=df_view["ngay_chot"], y=df_view["tong_phut"], name="Số phút tưới", yaxis="y2", line=dict(color='#E57373', width=3)))
                    fig1.update_layout(
                        title="Tần suất & Thời gian tưới mỗi ngày",
                        yaxis2=dict(title="Phút", overlaying="y", side="right"),
                        hovermode="x unified",
                        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)
                    )
                    st.plotly_chart(fig1, use_container_width=True)

                    # Biểu đồ 2: So sánh EC
                    fig2 = px.line(df_view, x="ngay_chot", y=["ec_thuc", "ec_target"], 
                                 title="Nồng độ phân bón (EC) Thực tế vs Yêu cầu",
                                 color_discrete_map={"ec_thuc": "#FFB74D", "ec_target": "#90CAF9"})
                    fig2.update_traces(line=dict(width=3))
                    st.plotly_chart(fig2, use_container_width=True)

                with tab2:
                    st.info("💡 **Hệ thống tự động phát hiện giai đoạn:** Nếu số lần tưới hoặc EC thay đổi đột ngột vượt ngưỡng, một giai đoạn mới sẽ được tính.")
                    
                    # Ngưỡng phát hiện (Có thể tùy chỉnh)
                    threshold_lan = st.slider("Ngưỡng thay đổi lần tưới:", 0.5, 5.0, 2.0)
                    
                    # Thuật toán chia giai đoạn đơn giản:
                    # So sánh ngày hôm nay với trung bình tích lũy của giai đoạn hiện tại
                    phase_labels = []
                    current_phase = 1
                    accumulator = []
                    
                    for val in df_view["so_lan"].to_list():
                        if accumulator:
                            avg_so_far = sum(accumulator) / len(accumulator)
                            if abs(val - avg_so_far) > threshold_lan:
                                current_phase += 1
                                accumulator = []
                        accumulator.append(val)
                        phase_labels.append(f"GĐ {current_phase}")
                    
                    df_stage = df_view.with_columns(pl.Series("Giai đoạn", phase_labels))
                    
                    # Biểu đồ phân màu theo giai đoạn
                    fig3 = px.bar(df_stage, x="ngay_chot", y="so_lan", color="Giai đoạn", 
                                title="Phân đoạn phát triển dựa trên tần suất tưới")
                    st.plotly_chart(fig3, use_container_width=True)
                    
                    # Bảng tổng kết giai đoạn
                    summary = df_stage.group_by("Giai đoạn").agg([
                        pl.col("ngay_chot").min().alias("Bắt đầu"),
                        pl.col("ngay_chot").max().alias("Kết thúc"),
                        pl.col("so_lan").mean().round(1).alias("TB Lần"),
                        pl.col("tong_phut").mean().round(1).alias("TB Phút"),
                        pl.col("ec_thuc").mean().round(2).alias("TB EC")
                    ]).sort("Bắt đầu")
                    st.table(summary)
            else:
                st.warning("⚠️ Không tìm thấy vụ mùa nào dài trên 10 ngày.")
        else:
            st.error("❌ Không thể ghép cặp dữ liệu Bật/Tắt. Vui lòng kiểm tra file log.")
else:
    # Màn hình chờ khi chưa có file
    st.info("👋 Chào mừng! Hãy tải file Log ở cột bên trái để bắt đầu phân tích.")
    st.image("https://img.freepik.com/free-vector/smart-farming-concept-illustration_114360-7055.jpg", width=600)

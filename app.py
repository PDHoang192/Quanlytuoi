import streamlit as st
import polars as pl
import json
import re
import ast
import datetime
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots

# ==========================================
# 1. CẤU HÌNH TRANG (Bắt buộc để app tràn viền)
# ==========================================
st.set_page_config(page_title="Hệ Thống Phân Tích Tưới", layout="wide", page_icon="🌱")

# Ẩn menu mặc định của Streamlit và đổi màu nút bấm chính
st.markdown("""
<style>
    #MainMenu {visibility: hidden;}
    footer {visibility: hidden;}
    .stButton button {
        background-color: #0c613c !important; 
        color: white !important; 
        border-radius: 8px !important;
        font-weight: bold !important;
    }
    .stButton button:hover {
        background-color: #0a4d30 !important;
    }
    /* Đổi màu viền container */
    [data-testid="stVerticalBlockBorderWrapper"] {
        border-radius: 12px !important;
        background-color: #ffffff;
    }
</style>
""", unsafe_allow_html=True)

# ==========================================
# 2. HÀM XỬ LÝ DỮ LIỆU CỦA BẠN (Paste lại code cũ vào đây)
# ==========================================
@st.cache_data
def parse_log_file_cached(file_content_bytes):
    raw_text = file_content_bytes.decode("utf-8").strip()
    raw_text_clean = re.sub(r'"\s*\n\s*"', '",\n"', raw_text)
    raw_text_clean = re.sub(r',\s*\}', '}', raw_text_clean)
    raw_text_clean = re.sub(r',\s*\]', ']', raw_text_clean)
    raw_text_clean = re.sub(r'\}\s*\{', '},{', raw_text_clean)
    json_text = raw_text_clean if raw_text_clean.startswith('[') else f"[{raw_text_clean}]"
    try:
        return json.loads(json_text)
    except:
        py_text = json_text.replace('true', 'True').replace('false', 'False').replace('null', 'None')
        return ast.literal_eval(py_text)

def process_data(df, start_d, end_d):
    gap_limit = 2
    min_season_days = 10
    if start_d and end_d:
        df = df.filter((pl.col("dt").dt.date() >= start_d) & (pl.col("dt").dt.date() <= end_d))
    if df.is_empty(): return None, "Không có dữ liệu."

    df_on = df.filter(pl.col("Trạng thái").str.to_uppercase().str.strip_chars() == "BẬT")
    df_off = df.filter(pl.col("Trạng thái").str.to_uppercase().str.strip_chars() == "TẮT").with_columns(pl.col("dt").alias("dt_end"))
    
    df_pairs = df_on.join_asof(df_off, on="dt", strategy="forward", suffix="_end")
    df_pairs = df_pairs.with_columns([
        ((pl.col("dt_end") - pl.col("dt")).dt.total_seconds()).alias("duration_s"),
        pl.col("dt").dt.date().alias("Date"),
        pl.coalesce(["TBEC_end", "TBEC"]).alias("val_ec_goc")
    ]).filter((pl.col("duration_s") > 0) & (pl.col("duration_s") < 3600))

    daily = df_pairs.group_by("Date").agg([
        pl.count().alias("turns"),
        (pl.col("duration_s").sum() / 60).round(1).alias("total_time_min"),
        pl.col("val_ec_goc").mean().alias("avg_ec")
    ]).sort("Date")

    daily = daily.with_columns([(pl.col("Date").diff().dt.total_days() > gap_limit).fill_null(False).alias("is_new_season")])
    daily = daily.with_columns(pl.col("is_new_season").cum_sum().alias("s_id"))
    
    seasons = daily.group_by("s_id").agg([
        pl.col("Date").min().alias("Bắt đầu"),
        pl.col("Date").max().alias("Kết thúc"),
        ((pl.col("Date").max() - pl.col("Date").min()).dt.total_days() + 1).alias("Số ngày")
    ]).sort("Bắt đầu")
    
    seasons = seasons.filter(pl.col("Số ngày") >= min_season_days)
    return (df_pairs, seasons, daily), "Thành công"


# ==========================================
# 3. GIAO DIỆN HEADER (Phần đầu trang)
# ==========================================
st.markdown("""
    <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 20px; padding-bottom: 15px; border-bottom: 2px solid #f0f2f6;">
        <h1 style="color: #111; font-size: 26px; margin: 0; font-family: sans-serif;">Hệ thống Phân Tích Tưới Tiêu</h1>
        <div>
            <span style="background-color: #e6f9f0; color: #28a745; padding: 6px 15px; border-radius: 20px; font-weight: 700; font-size: 12px; margin-right: 10px; letter-spacing: 0.5px;">HỆ THỐNG: ONLINE</span>
            <span style="background-color: #f1f3f8; color: #555; padding: 6px 15px; border-radius: 20px; font-weight: 600; font-size: 12px; border: 1px solid #e1e4eb;">ID: PH-4290</span>
        </div>
    </div>
""", unsafe_allow_html=True)


# ==========================================
# 4. GIAO DIỆN SIDEBAR (Thanh bên trái)
# ==========================================
with st.sidebar:
    # Card trạng thái hệ thống bơm
    st.markdown("""
        <div style="background-color: #ffffff; padding: 20px; border-radius: 12px; margin-bottom: 25px; border-left: 5px solid #0c613c; box-shadow: 0 2px 4px rgba(0,0,0,0.05);">
            <div style="display: flex; align-items: center; gap: 15px;">
                <div style="font-size: 28px;">💧</div>
                <div>
                    <div style="font-size: 11px; font-weight: 700; color: #777; letter-spacing: 1px;">HYDRAULIC SYSTEMS</div>
                    <div style="font-size: 18px; font-weight: 800; color: #111; margin-top: 2px;">Flow: 42m³/h</div>
                </div>
            </div>
        </div>
    """, unsafe_allow_html=True)
    
    st.markdown("<p style='font-size: 13px; font-weight: 700; color: #666;'>NGUỒN DỮ LIỆU</p>", unsafe_allow_html=True)
    target_stt = st.selectbox("STT Khu vực:", ["Khu vực A-01", "Khu vực B-02", "Khu vực C-03"])
    
    st.markdown("<br>", unsafe_allow_html=True)
    uploaded_file = st.file_uploader("📂 Log Tưới (.csv/.txt)", type=['txt', 'json', 'csv'])
    fert_file = st.file_uploader("📂 Log Châm Phân (.csv/.txt)", type=['txt', 'json', 'csv'])
    
    st.markdown("<br>", unsafe_allow_html=True)
    date_mode = st.radio("Khoảng thời gian:", ["Toàn bộ", "Tùy chọn"])
    
    process_btn = st.button("CẬP NHẬT DỮ LIỆU", use_container_width=True)


# ==========================================
# 5. BỐ CỤC NỘI DUNG CHÍNH (Tabs & Columns)
# ==========================================
# Dùng CSS để căn chỉnh màu sắc của Tab mặc định Streamlit
tab1, tab2, tab3 = st.tabs(["📋 Danh sách Vụ", "📈 Biểu đồ tổng quan", "🧠 Phân tích giai đoạn đa biến"])

with tab1:
    rows = []
    for i, s in enumerate(s_dicts):
                        if i > 0:
                            r_s, r_e = s_dicts[i-1]["Kết thúc"] + datetime.timedelta(days=1), s["Bắt đầu"] - datetime.timedelta(days=1)
                            if (r_e - r_s).days >= 0:
                                rows.append({"Đối tượng": "⏳ Nghỉ đất", "Từ": r_s, "Đến": r_e, "Số ngày": (r_e - r_s).days + 1})
                        rows.append({"Đối tượng": f"🌱 Vụ {i+1}", "Từ": s["Bắt đầu"], "Đến": s["Kết thúc"], "Số ngày": s["Số ngày"]})
                    st.table(rows)

with tab2:
    sel_v = st.selectbox("Chọn Vụ:", list(s_opts.keys()), key="v2")
                    df_v = daily.filter(pl.col("s_id") == s_opts[sel_v]["s_id"]).sort("Date")
                    f1 = go.Figure()
                    f1.add_trace(go.Bar(x=df_v["Date"], y=df_v["turns"], name="Lần", marker_color='#3366CC', yaxis='y1'))
                    f1.add_trace(go.Scatter(x=df_v["Date"], y=df_v["total_time_min"], name="Phút", marker_color='#FF3366', yaxis='y2'))
                    f1.update_layout(yaxis2=dict(overlaying='y', side='right'), hovermode="x unified")
                    st.plotly_chart(f1, use_container_width=True)
                    
                    f2 = go.Figure()
                    f2.add_trace(go.Scatter(x=df_v["Date"], y=df_v["avg_ec"], name="EC Thực", line=dict(color='#FF9900')))
                    if "avg_req_ec" in df_v.columns and not df_v["avg_req_ec"].null_count() == len(df_v):
                        f2.add_trace(go.Scatter(x=df_v["Date"], y=df_v["avg_req_ec"], name="EC Yêu cầu", line=dict(dash='dash')))
                    st.plotly_chart(f2, use_container_width=True)
with tab3:
    p_map = {"Số lần tưới": "turns", "TBEC thực tế": "avg_ec", "EC yêu cầu": "avg_req_ec"}
                    valid_opts = ["Số lần tưới", "TBEC thực tế"]
                    if "avg_req_ec" in daily.columns and daily["avg_req_ec"].null_count() < len(daily):
                        valid_opts.append("EC yêu cầu")

                    c1, c2 = st.columns(2)
                    with c1:
                        sel_v3 = st.selectbox("Chọn Vụ:", list(s_opts.keys()), key="v3")
                        cols = st.multiselect("Thông số xét duyệt:", valid_opts, default=["Số lần tưới", "TBEC thực tế"])
                        mode = st.radio("Logic:", ["OR", "AND"], horizontal=True)
                    with c2:
                        th_t = st.number_input("Ngưỡng Lần", value=2.0)
                        th_e = st.number_input("Ngưỡng TBEC", value=30.0)
                        th_req = st.number_input("Ngưỡng EC yêu cầu", value=10.0)
                        th_map = {"Số lần tưới": th_t, "TBEC thực tế": th_e, "EC yêu cầu": th_req}

                    df_t3 = daily.filter(pl.col("s_id") == s_opts[sel_v3]["s_id"]).sort("Date")
                    if cols:
                        df_clean = df_t3.drop_nulls(subset=[p_map[c] for c in cols])
                        if not df_clean.is_empty():
                            dts, labels, stgs = df_clean["Date"].to_list(), [], []
                            v_data = {c: {"d": df_clean[p_map[c]].to_list(), "g": []} for c in cols}
                            c_s, idx = dts[0], 1
                            for i in range(len(dts)):
                                conds = []
                                for c in cols:
                                    if v_data[c]["g"]:
                                        avg = sum(v_data[c]["g"]) / len(v_data[c]["g"])
                                        conds.append(abs(v_data[c]["d"][i] - avg) > th_map[c])
                                    else: conds.append(False)
                                if (any(conds) if mode == "OR" else all(conds)) and len(v_data[cols[0]]["g"]) >= 2:
                                    stgs.append({"Giai đoạn": f"GĐ {idx}", "Bắt đầu": c_s, "Kết thúc": dts[i-1]})
                                    c_s, idx = dts[i], idx + 1
                                    for c in cols: v_data[c]["g"] = []
                                for c in cols: v_data[c]["g"].append(v_data[c]["d"][i])
                                labels.append(f"GĐ {idx}")
                            
                            stgs.append({"Giai đoạn": f"GĐ {idx}", "Bắt đầu": c_s, "Kết thúc": dts[-1]})
                            df_p3 = df_clean.with_columns(pl.Series("Giai đoạn", labels))
                            st.plotly_chart(px.bar(df_p3, x="Date", y=p_map[cols[0]], color='Giai đoạn'))

                            st.divider()
                            sel_g = st.selectbox("Chọn Giai đoạn:", [s["Giai đoạn"] for s in stgs])
                            
                            # --- FIX TẠI ĐÂY: Ép kiểu toàn bộ về Float64 ---
                            det_selects = [
                                pl.col("Date").cast(pl.Utf8).alias("Ngày"),
                                pl.col("turns").cast(pl.Float64).alias("Lần"),
                                pl.col("total_time_min").cast(pl.Float64).alias("Phút"),
                                pl.col("avg_ec").cast(pl.Float64).alias("EC thực")
                            ]
                            
                            avg_selects = [
                                pl.lit("--- TRUNG BÌNH ---").alias("Ngày"),
                                pl.col("Lần").mean().cast(pl.Float64),
                                pl.col("Phút").mean().cast(pl.Float64),
                                pl.col("EC thực").mean().cast(pl.Float64)
                            ]

                            if "avg_req_ec" in df_p3.columns:
                                det_selects.append(pl.col("avg_req_ec").cast(pl.Float64).alias("EC yêu cầu"))
                                avg_selects.append(pl.col("EC yêu cầu").mean().cast(pl.Float64))

                            df_det = df_p3.filter(pl.col("Giai đoạn") == sel_g).select(det_selects)
                            df_avg = df_det.select(avg_selects)

                            df_final = pl.concat([df_det, df_avg])
                            
                            # Hiển thị bảng và format số thập phân gọn gàng hơn
                            st.dataframe(
                                df_final.to_pandas().style.format({
                                    "Lần": "{:.1f}", 
                                    "Phút": "{:.1f}", 
                                    "EC thực": "{:.2f}", 
                                    "EC yêu cầu": "{:.2f}"
                                }, na_rep="-"), 
                                use_container_width=True, 
                                hide_index=True
                            )
                        else:
                            st.warning("Dữ liệu không đủ để phân tích giai đoạn.")
        else:
            st.error(msg)

    with col_side:
        # THẺ 1: ĐỘ ẨM ĐẤT
        st.markdown("""
            <div style="background-color: #0d6e46; color: white; padding: 25px; border-radius: 12px; margin-bottom: 25px; position: relative; overflow: hidden;">
                <div style="font-size: 12px; font-weight: 700; letter-spacing: 1px; opacity: 0.9;">ĐỘ ẨM ĐẤT TRUNG BÌNH</div>
                <div style="font-size: 42px; font-weight: 800; margin-top: 15px;">42.8%</div>
                <div style="background-color: rgba(255,255,255,0.2); height: 4px; width: 100%; border-radius: 2px; margin-top: 15px;">
                    <div style="background-color: white; height: 100%; width: 42.8%; border-radius: 2px;"></div>
                </div>
                <div style="position: absolute; right: -10px; bottom: -20px; font-size: 80px; opacity: 0.1;">💧</div>
            </div>
        """, unsafe_allow_html=True)
        
        # THẺ 2: THÔNG BÁO HỆ THỐNG
        st.markdown("""
            <div style="background-color: white; padding: 20px; border-radius: 12px; margin-bottom: 25px; border-left: 5px solid #0c613c; border-top: 1px solid #eee; border-right: 1px solid #eee; border-bottom: 1px solid #eee;">
                <div style="font-size: 12px; font-weight: 700; color: #555; letter-spacing: 0.5px; margin-bottom: 10px;">THÔNG BÁO HỆ THỐNG</div>
                <p style="margin: 0 0 15px 0; color: #333; font-size: 14px; line-height: 1.6;">Áp suất đường ống khu vực A-01 đang ở mức cao (<strong>2.4 bar</strong>). Đề xuất kiểm tra van xả.</p>
                <a href="#" style="color: #0c613c; font-weight: 700; text-decoration: none; font-size: 12px; letter-spacing: 0.5px;">CHI TIẾT ›</a>
            </div>
        """, unsafe_allow_html=True)
        
        # THẺ 3: HÌNH ẢNH NHÀ MÀNG (Bổ sung sinh động)
        st.markdown("""
            <img src="https://images.unsplash.com/photo-1595841696650-5f212239d1b0?ixlib=rb-4.0.3&auto=format&fit=crop&w=600&q=80" 
                 style="width: 100%; height: 160px; object-fit: cover; border-radius: 12px; border: 1px solid #eee;">
        """, unsafe_allow_html=True)

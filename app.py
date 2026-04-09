import streamlit as st
import polars as pl
import json
import plotly.express as px
from datetime import datetime, timedelta

st.set_page_config(page_title="Hệ Thống Quản Lý Tưới", layout="wide", page_icon="🌱")

def process_data(file_content, target_area, gap_limit, min_season_days):
    raw_text = file_content.getvalue().decode("utf-8").strip()
    if not raw_text.startswith('['):
        raw_text = "[" + raw_text.replace('}{', '},{').replace('}\n{', '},{') + "]"
    
    try:
        data = json.loads(raw_text)
        df = pl.DataFrame(data)
    except Exception as e:
        return None, f"Lỗi đọc file: {e}"

    needed_cols = ["Thời gian", "Tên khu", "TBEC", "TBPH", "Trạng thái"]
    df = df.select(needed_cols).filter(pl.col("Tên khu").str.contains(target_area.upper()))
    
    if df.is_empty():
        return None, f"Không tìm thấy dữ liệu cho khu vực: {target_area}"

    # Đã sửa: Chuẩn hóa dấu phẩy thành dấu chấm trước khi chuyển thành số thập phân
    df = df.with_columns([
        pl.col("Thời gian").str.to_datetime("%Y-%m-%d %H-%M-%S").alias("dt"),
        pl.col("TBEC").cast(pl.Utf8).str.replace(",", ".").cast(pl.Float64, strict=False),
        pl.col("TBPH").cast(pl.Utf8).str.replace(",", ".").cast(pl.Float64, strict=False)
    ]).sort("dt")

    # LOGIC GHÉP CẶP
    df_on = df.filter(pl.col("Trạng thái") == "Bật")
    df_off = df.filter(pl.col("Trạng thái") == "Tắt").with_columns(
        pl.col("dt").alias("dt_end")
    )

    # Ghép dòng Bật với dòng Tắt kế tiếp
    df_pairs = df_on.join_asof(
        df_off,
        on="dt",
        strategy="forward", 
        suffix="_end"
    )

    # Loại bỏ những lần Bật mà không có lần Tắt
    df_pairs = df_pairs.filter(pl.col("dt_end").is_not_null())

    # Đã sửa: Lấy TBEC/TBPH từ dòng "Tắt" (TBEC_end). 
    # Nếu dòng Tắt không có, hàm coalesce sẽ tự động lùi lại tìm ở dòng "Bật" (TBEC).
    df_pairs = df_pairs.with_columns([
        ((pl.col("dt_end") - pl.col("dt")).dt.total_seconds()).alias("duration_s"),
        pl.col("dt").dt.date().alias("Date"),
        pl.coalesce(["TBEC_end", "TBEC"]).alias("val_ec_goc"),
        pl.coalesce(["TBPH_end", "TBPH"]).alias("val_ph_goc")
    ])

    # Lọc bỏ các dữ liệu vô lý (> 600s tương đương 10 phút)
    df_pairs = df_pairs.filter((pl.col("duration_s") > 0) & (pl.col("duration_s") < 600))

    # Xác định Vụ
    daily = df_pairs.group_by("Date").agg([
        pl.count().alias("turns")
    ]).sort("Date")

    daily = daily.with_columns([
        (pl.col("Date").diff().dt.total_days() > gap_limit).fill_null(False).alias("is_new_season")
    ])
    daily = daily.with_columns(pl.col("is_new_season").cum_sum().alias("s_id"))

    seasons = daily.group_by("s_id").agg([
        pl.col("Date").min().alias("Start"),
        pl.col("Date").max().alias("End"),
        ((pl.col("Date").max() - pl.col("Date").min()).dt.total_days() + 1).alias("Days")
    ]).filter(pl.col("Days") >= min_season_days).sort("Start")

    return (df_pairs, seasons, daily), "Thành công"

# --- GIAO DIỆN ---
st.title("🚜 Nhật Ký Vận Hành & Phân Tích Tưới")

with st.sidebar:
    target_area = st.text_input("Khu vực:", "ANT-2").upper()
    gap_limit = st.slider("Ngắt vụ (ngày):", 1, 10, 2)
    min_days = st.number_input("Ngày tối thiểu/vụ:", value=10)
    uploaded_file = st.file_uploader("Tải file log tưới", type=['txt', 'json'])

if uploaded_file:
    res, msg = process_data(uploaded_file, target_area, gap_limit, min_days)
    
    if res:
        df_p, seasons, daily = res
        # Đã thêm tab3 vào danh sách tabs
        tab1, tab2, tab3 = st.tabs(["📋 Báo cáo Vụ & Nghỉ", "🔍 Tra cứu chi tiết đợt tưới", "🧪 Thống kê Châm Phân"])

        with tab1:
            st.subheader("Bảng tổng hợp chu kỳ canh tác")
            s_list = seasons.to_dicts()
            final_report = []
            for i, s in enumerate(s_list):
                final_report.append({
                    "Giai đoạn": f"VỤ MÙA {i+1}", "Bắt đầu": s["Start"], "Kết thúc": s["End"],
                    "Số ngày": s["Days"], "Trạng thái": "Hoàn thành"
                })
                if i < len(s_list) - 1:
                    r_s, r_e = s["End"] + timedelta(days=1), s_list[i+1]["Start"] - timedelta(days=1)
                    final_report.append({
                        "Giai đoạn": "🟢 NGHỈ ĐẤT", "Bắt đầu": r_s, "Kết thúc": r_e,
                        "Số ngày": (r_e - r_s).days + 1, "Trạng thái": "Nghỉ dưỡng"
                    })
            st.table(final_report)

        with tab2:
            st.subheader(f"Thống kê vận hành khu {target_area}")
            daily_min_2 = daily.filter(pl.col("turns") >= 2)
            fig = px.bar(daily_min_2.to_pandas(), x="Date", y="turns", 
                         title="Các ngày có tần suất tưới >= 2 lần/ngày",
                         color="turns", color_continuous_scale="Viridis")
            st.plotly_chart(fig, use_container_width=True)
            
            st.divider()
            search_date = st.selectbox("Chọn ngày để xem chi tiết:", 
                                      options=sorted(df_p["Date"].unique(), reverse=True))
            
            if search_date:
                # Hiển thị chính xác TBEC, TBPH
                day_detail = df_p.filter(pl.col("Date") == search_date).select([
                    pl.col("dt").dt.strftime("%H:%M:%S").alias("Giờ Bật"),
                    pl.col("dt_end").dt.strftime("%H:%M:%S").alias("Giờ Tắt"),
                    pl.col("duration_s").alias("Thời gian (giây)"),
                    pl.col("val_ec_goc").round(2).alias("TBEC"),
                    pl.col("val_ph_goc").round(2).alias("TBPH")
                ])
                st.write(f"Kết quả cho ngày **{search_date.strftime('%d/%m/%Y')}**:")
                st.dataframe(day_detail, use_container_width=True, hide_index=True)

        with tab3:
            st.subheader("Phân tích dữ liệu châm phân (EC Yêu Cầu)")
            
            col1, col2 = st.columns(2)
            with col1:
                uploaded_cp_file = st.file_uploader("Tải file châm phân (JSON/TXT)", type=['txt', 'json'], key="cp_upload")
            with col2:
                target_tank = st.text_input("Tìm kiếm bồn:", "BỒN TG-ANT1").upper()

            if uploaded_cp_file:
                raw_cp = uploaded_cp_file.getvalue().decode("utf-8").strip()
                if not raw_cp.startswith('['):
                    raw_cp = "[" + raw_cp.replace('}{', '},{').replace('}\n{', '},{') + "]"
                
                try:
                    data_cp = json.loads(raw_cp)
                    df_cp = pl.DataFrame(data_cp)
                    
                    # Xác định cột chứa tên bồn (hỗ trợ cả Tên bồn hoặc Tên khu)
                    tank_col = "Tên bồn" if "Tên bồn" in df_cp.columns else "Tên khu" if "Tên khu" in df_cp.columns else None
                    
                    if "EC yêu cầu" not in df_cp.columns or "Thời gian" not in df_cp.columns:
                        st.error("File không hợp lệ: Cần có các trường 'Thời gian' và 'EC yêu cầu'.")
                    elif not tank_col:
                        st.error("File không hợp lệ: Không tìm thấy trường 'Tên bồn' hoặc 'Tên khu'.")
                    else:
                        # Lọc theo tên bồn
                        df_cp_filtered = df_cp.filter(pl.col(tank_col).str.contains(target_tank))
                        
                        if df_cp_filtered.is_empty():
                            st.warning(f"Không tìm thấy dữ liệu châm phân cho bồn: {target_tank}")
                        else:
                            # Làm sạch và tính trung bình theo ngày
                            df_cp_clean = df_cp_filtered.with_columns([
                                pl.col("Thời gian").str.to_datetime("%Y-%m-%d %H-%M-%S").dt.date().alias("Date"),
                                pl.col("EC yêu cầu").cast(pl.Utf8).str.replace(",", ".").cast(pl.Float64, strict=False)
                            ])
                            
                            df_cp_daily = df_cp_clean.group_by("Date").agg([
                                pl.col("EC yêu cầu").mean().round(2).alias("Trung bình EC yêu cầu")
                            ]).sort("Date")
                            
                            st.success(f"Đã xử lý thành công dữ liệu cho **{target_tank}**")
                            
                            # Hiển thị biểu đồ
                            fig_cp = px.line(df_cp_daily.to_pandas(), x="Date", y="Trung bình EC yêu cầu", 
                                             title=f"Biểu đồ EC Yêu cầu trung bình theo ngày - {target_tank}",
                                             markers=True)
                            st.plotly_chart(fig_cp, use_container_width=True)
                            
                            # Hiển thị bảng chi tiết
                            st.write("Bảng thống kê chi tiết:")
                            st.dataframe(df_cp_daily, use_container_width=True, hide_index=True)
                            
                except Exception as e:
                    st.error(f"Lỗi xử lý file châm phân: {e}")

    else:
        st.error(msg)

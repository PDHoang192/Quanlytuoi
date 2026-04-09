import streamlit as st
import polars as pl
import json
import plotly.express as px
from datetime import datetime, timedelta

st.set_page_config(page_title="Hệ Thống Phân Tích Tưới Chuyên Sâu", layout="wide", page_icon="📊")

# --- HÀM ĐỌC VÀ GỘP NHIỀU FILE ---
def load_multiple_files(uploaded_files, target_area=None):
    if not uploaded_files:
        return None
    
    all_dfs = []
    for file in uploaded_files:
        raw_text = file.getvalue().decode("utf-8").strip()
        # Xử lý định dạng JSON lỗi (thiếu dấu phẩy giữa các object)
        if not raw_text.startswith('['):
            raw_text = "[" + raw_text.replace('}{', '},{').replace('}\n{', '},{') + "]"
        
        try:
            data = json.loads(raw_text)
            df = pl.DataFrame(data)
            all_dfs.append(df)
        except Exception as e:
            st.error(f"Lỗi đọc file {file.name}: {e}")
            
    if not all_dfs:
        return None
    
    # Gộp tất cả các file thành 1 DataFrame duy nhất
    combined_df = pl.concat(all_dfs)
    
    # Tiền xử lý các cột chung
    # Lưu ý: Bạn cần kiểm tra tên cột "EC Yêu Cầu" trong file của mình, ở đây giả định là "EC_YeuCau" hoặc tương đương
    # Nếu file của bạn dùng tên khác, hãy đổi lại trong list select bên dưới
    cols = combined_df.columns
    target_ec_col = "EC_YeuCau" if "EC_YeuCau" in cols else ("Yêu Cầu EC" if "Yêu Cầu EC" in cols else None)
    
    combined_df = combined_df.with_columns([
        pl.col("Thời gian").str.to_datetime("%Y-%m-%d %H-%M-%S").alias("dt"),
        pl.col("TBEC").cast(pl.Utf8).str.replace(",", ".").cast(pl.Float64, strict=False),
        pl.col("TBPH").cast(pl.Utf8).str.replace(",", ".").cast(pl.Float64, strict=False),
    ])
    
    if target_ec_col:
        combined_df = combined_df.with_columns(
            pl.col(target_ec_col).cast(pl.Utf8).str.replace(",", ".").cast(pl.Float64, strict=False).alias("EC_Setpoint")
        )
    else:
        # Nếu không tìm thấy cột EC yêu cầu, tạo cột giả định bằng 0 để tránh lỗi
        combined_df = combined_df.with_columns(pl.lit(0.0).alias("EC_Setpoint"))

    if target_area:
        combined_df = combined_df.filter(pl.col("Tên khu").str.contains(target_area.upper()))
        
    return combined_df.sort("dt")

# --- LOGIC XỬ LÝ SỰ KIỆN TƯỚI (MỖI LẦN BẬT/TẮT) ---
def get_irrigation_events(df):
    df_on = df.filter(pl.col("Trạng thái") == "Bật")
    df_off = df.filter(pl.col("Trạng thái") == "Tắt").select([
        pl.col("dt").alias("dt_end"),
        pl.col("TBEC").alias("ec_end")
    ])

    # Ghép đôi Bật - Tắt
    df_pairs = df_on.join_asof(df_off, left_on="dt", right_on="dt_end", strategy="forward")
    df_pairs = df_pairs.filter(pl.col("dt_end").is_not_null())
    
    df_pairs = df_pairs.with_columns([
        ((pl.col("dt_end") - pl.col("dt")).dt.total_seconds()).alias("duration_s"),
        pl.col("dt").dt.date().alias("Date")
    ])
    # Loại bỏ các lần tưới quá ngắn (xả đường ống) hoặc quá dài (quên tắt)
    return df_pairs.filter((pl.col("duration_s") >= 15) & (pl.col("duration_s") < 900))

# --- GIAO DIỆN ---
st.title("🌱 Hệ Thống Phân Tích Dữ Liệu Tưới Đa Nguồn")

with st.sidebar:
    st.header("Cấu Hình")
    area = st.text_input("Khu vực mục tiêu:", "ANT-2").upper()
    
    st.subheader("1. File Châm Phân")
    st.caption("Để lấy EC Yêu Cầu & Chia giai đoạn")
    files_fert = st.file_uploader("Upload log Châm phân (nhiều file)", type=['txt', 'json'], accept_multiple_files=True)
    
    st.subheader("2. File Nhỏ Giọt")
    st.caption("Để lấy EC thực tế & Mùa vụ từng khu")
    files_drip = st.file_uploader("Upload log Nhỏ giọt (nhiều file)", type=['txt', 'json'], accept_multiple_files=True)
    
    st.divider()
    gap_limit = st.slider("Số ngày nghỉ để ngắt vụ:", 1, 7, 2)

# --- XỬ LÝ DỮ LIỆU ---
if files_fert and files_drip:
    df_fert_raw = load_multiple_files(files_fert)
    df_drip_raw = load_multiple_files(files_drip, target_area=area)

    if df_fert_raw is not None and df_drip_raw is not None:
        # 1. Xử lý file Nhỏ giọt: Tính toán mùa vụ và thông số thực tế hàng ngày
        df_events = get_irrigation_events(df_drip_raw)
        
        daily_stats = df_events.group_by("Date").agg([
            pl.count().alias("turns"),
            pl.col("duration_s").mean().round(0).alias("avg_duration"),
            pl.col("TBEC").mean().round(2).alias("avg_ec_real"),
            pl.col("TBPH").mean().round(2).alias("avg_ph_real")
        ]).sort("Date")

        # Xác định Mùa vụ / Nghỉ đất
        daily_stats = daily_stats.with_columns([
            (pl.col("Date").diff().dt.total_days() > gap_limit).fill_null(False).alias("is_new_season")
        ])
        daily_stats = daily_stats.with_columns(pl.col("is_new_season").cum_sum().alias("season_id"))

        # 2. Xử lý file Châm phân: Tìm EC Yêu cầu hàng ngày để chia Giai đoạn
        # Lấy EC_Setpoint phổ biến nhất trong ngày đó
        daily_setpoint = df_fert_raw.with_columns(pl.col("dt").dt.date().alias("Date")) \
            .group_by("Date").agg(pl.col("EC_Setpoint").median().alias("ec_target")) \
            .sort("Date")

        # Kết hợp dữ liệu Thực tế và Yêu cầu
        final_daily = daily_stats.join(daily_setpoint, on="Date", how="left")

        # --- HIỂN THỊ ---
        tab1, tab2, tab3 = st.tabs(["📅 Quản lý Vụ Mùa", "📈 Biểu đồ EC & Vận hành", "📋 Báo cáo chi tiết"])

        with tab1:
            st.subheader("Phân tích chu kỳ canh tác & Nghỉ đất")
            seasons = final_daily.group_by("season_id").agg([
                pl.col("Date").min().alias("Start"),
                pl.col("Date").max().alias("End"),
                ((pl.col("Date").max() - pl.col("Date").min()).dt.total_days() + 1).alias("Days")
            ]).sort("Start")
            
            # Hiển thị bảng vụ mùa và nghỉ đất
            report = []
            s_dicts = seasons.to_dicts()
            for i, s in enumerate(s_dicts):
                report.append({"Loại": f"VỤ {i+1}", "Từ ngày": s["Start"], "Đến ngày": s["End"], "Số ngày": s["Days"]})
                if i < len(s_dicts) - 1:
                    gap = (s_dicts[i+1]["Start"] - s["End"]).days - 1
                    if gap > 0:
                        report.append({"Loại": "🟢 NGHỈ ĐẤT", "Từ ngày": s["End"] + timedelta(days=1), 
                                       "Đến ngày": s_dicts[i+1]["Start"] - timedelta(days=1), "Số ngày": gap})
            st.table(report)

        with tab2:
            st.subheader(f"So sánh EC Yêu Cầu vs Thực Tế - Khu {area}")
            fig_ec = px.line(final_daily.to_pandas(), x="Date", y=["ec_target", "avg_ec_real"],
                             labels={"value": "Giá trị EC", "Date": "Ngày"},
                             title="Tương quan giữa EC Cài đặt (Máy) và EC Thực tế (Vòi)",
                             markers=True)
            st.plotly_chart(fig_ec, use_container_width=True)
            
            st.divider()
            st.subheader("Thời gian tưới trung bình (giây/lần)")
            fig_dur = px.bar(final_daily.to_pandas(), x="Date", y="avg_duration", color="turns")
            st.plotly_chart(fig_dur, use_container_width=True)

        with tab3:
            st.subheader(f"Nhật ký vận hành chi tiết khu {area}")
            # Chia giai đoạn sinh trưởng dựa trên sự thay đổi của EC Target
            final_daily = final_daily.with_columns(
                (pl.col("ec_target").diff().abs() > 0.05).fill_null(False).cum_sum().alias("phase_id")
            )
            
            st.dataframe(
                final_daily.select([
                    pl.col("Date").alias("Ngày"),
                    pl.col("ec_target").alias("EC Yêu Cầu"),
                    pl.col("avg_ec_real").alias("EC Thực Tế"),
                    pl.col("turns").alias("Số lần tưới"),
                    pl.col("avg_duration").alias("Thời gian/lần (s)"),
                ]).to_pandas(),
                use_container_width=True
            )
    else:
        st.warning("Không thể xử lý dữ liệu. Vui lòng kiểm tra định dạng file.")
else:
    st.info("💡 Mẹo: Bạn có thể chọn nhiều file log cùng lúc bằng cách nhấn giữ Ctrl (hoặc Cmd) khi chọn file.")

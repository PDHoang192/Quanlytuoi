import streamlit as st
import polars as pl
import json
import re
import plotly.express as px
from datetime import datetime, timedelta

st.set_page_config(page_title="Hệ Thống Quản Lý Tưới", layout="wide", page_icon="🌱")

# --- HÀM ĐỌC FILE SIÊU CƯỜNG V2 ---
def parse_log_file(file_content):
    raw_text = file_content.getvalue().decode("utf-8").strip()
    
    # 1. Cố gắng dọn dẹp và đọc bằng JSON chuẩn trước
    clean_text = re.sub(r'\}\s*\{', '},{', raw_text)
    clean_text = re.sub(r',\s*\}', '}', clean_text)
    clean_text = re.sub(r',\s*\]', ']', clean_text)
    
    json_text = clean_text if clean_text.startswith('[') else f"[{clean_text}]"
    
    try:
        return json.loads(json_text)
    except Exception:
        pass
        
    # 2. CHẾ ĐỘ QUÉT REGEX V2 (Bất tử trước mọi lỗi cấu trúc)
    # Tách khối trực tiếp bằng key "Thời gian" để đảm bảo KHÔNG BỎ SÓT bất kỳ block sự kiện nào
    records = []
    chunks = re.split(r'"Thời gian"\s*:\s*', raw_text)
    
    for i in range(1, len(chunks)):
        chunk = chunks[i]
        record = {}
        
        # Trích xuất thời gian (luôn nằm ở đầu block sau khi split)
        time_match = re.search(r'^"([^"]+)"', chunk)
        if time_match:
            record["Thời gian"] = time_match.group(1)
        else:
            continue
            
        # Trích xuất linh động MỌI CẶP "Key": "Value" còn lại trong block này
        kv_matches = re.finditer(r'"([^"]+)"\s*:\s*"([^"]*)"', chunk)
        for m in kv_matches:
            record[m.group(1)] = m.group(2)
            
        records.append(record)
        
    if records:
        return records
        
    raise Exception("Không thể đọc được dữ liệu từ file. File hỏng nặng hoặc sai định dạng hoàn toàn.")

def process_data(file_content, target_area, gap_limit, min_season_days):
    try:
        data = parse_log_file(file_content)
        df = pl.DataFrame(data)
    except Exception as e:
        return None, f"Lỗi đọc file log: {e}"

    if "Thời gian" not in df.columns or "Trạng thái" not in df.columns:
        return None, "File thiếu các trường cơ bản (Thời gian, Trạng thái)."
        
    # Lọc bao quát cả "Tên khu" và "Tên bồn"
    target_upper = target_area.upper()
    if "Tên khu" in df.columns and "Tên bồn" in df.columns:
        area_filter = pl.col("Tên khu").str.to_uppercase().str.contains(target_upper) | pl.col("Tên bồn").str.to_uppercase().str.contains(target_upper)
    elif "Tên khu" in df.columns:
        area_filter = pl.col("Tên khu").str.to_uppercase().str.contains(target_upper)
    elif "Tên bồn" in df.columns:
        area_filter = pl.col("Tên bồn").str.to_uppercase().str.contains(target_upper)
    else:
         return None, "Không tìm thấy cột Tên khu/Tên bồn."
         
    df = df.filter(area_filter)
    
    if df.is_empty():
        return None, f"Không tìm thấy dữ liệu cho khu vực: {target_area}"

    df = df.with_columns([
        pl.col("Thời gian").str.to_datetime("%Y-%m-%d %H-%M-%S", strict=False).alias("dt"),
    ])
    
    df = df.filter(pl.col("dt").is_not_null())
    
    if "TBEC" in df.columns:
        df = df.with_columns(pl.col("TBEC").cast(pl.Utf8).str.replace(",", ".").cast(pl.Float64, strict=False))
    else:
        df = df.with_columns(pl.lit(0.0).alias("TBEC"))
        
    if "TBPH" in df.columns:
        df = df.with_columns(pl.col("TBPH").cast(pl.Utf8).str.replace(",", ".").cast(pl.Float64, strict=False))
    else:
        df = df.with_columns(pl.lit(0.0).alias("TBPH"))

    df = df.sort("dt")

    # Lọc bao quát chữ BẬT/TẮT
    df_on = df.filter(pl.col("Trạng thái").str.to_uppercase().str.contains("BẬT"))
    df_off = df.filter(pl.col("Trạng thái").str.to_uppercase().str.contains("TẮT")).with_columns(
        pl.col("dt").alias("dt_end")
    )

    df_pairs = df_on.join_asof(
        df_off,
        on="dt",
        strategy="forward", 
        suffix="_end"
    )

    df_pairs = df_pairs.filter(pl.col("dt_end").is_not_null())

    df_pairs = df_pairs.with_columns([
        ((pl.col("dt_end") - pl.col("dt")).dt.total_seconds()).alias("duration_s"),
        pl.col("dt").dt.date().alias("Date"),
        pl.coalesce(["TBEC_end", "TBEC"]).alias("val_ec_goc"),
        pl.coalesce(["TBPH_end", "TBPH"]).alias("val_ph_goc")
    ])

    df_pairs = df_pairs.filter((pl.col("duration_s") > 0) & (pl.col("duration_s") < 600))

    daily = df_pairs.group_by("Date").agg([
        pl.count().alias("turns")
    ]).sort("Date")

    daily = daily.with_columns([
        (pl.col("Date").diff().dt.total_days() > gap_limit).fill_null(False).alias("is_new_season")
    ])
    daily = daily.with_columns(pl.col("is_new_season").cum_sum().alias("s_id"))

    df_pairs = df_pairs.join(daily.select(["Date", "s_id"]), on="Date")

    seasons = daily.group_by("s_id").agg([
        pl.col("Date").min().alias("Start"),
        pl.col("Date").max().alias("End"),
        ((pl.col("Date").max() - pl.col("Date").min()).dt.total_days() + 1).alias("Days")
    ]).filter(pl.col("Days") >= min_season_days).sort("Start")

    return (df_pairs, seasons, daily), "Thành công"


# --- GIAO DIỆN ---
with st.sidebar:
    target_area = st.text_input("Khu vực canh tác:", "ANT-2").upper()
    gap_limit = st.slider("Ngắt vụ (ngày):", 1, 10, 2)
    min_days = st.number_input("Ngày tối thiểu/vụ:", value=10)
    uploaded_file = st.file_uploader("Tải file log tưới chính", type=['txt', 'json'])

if uploaded_file:
    res, msg = process_data(uploaded_file, target_area, gap_limit, min_days)
    
    if res:
        df_p, seasons, daily = res
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
            st.subheader(f"Thống kê chi tiết từng ngày tưới - Khu {target_area}")
            if not seasons.is_empty():
                season_list = seasons.to_dicts()
                season_names = [f"Vụ {i+1} ({s['Start']} đến {s['End']})" for i, s in enumerate(season_list)]
                selected_season_name = st.selectbox("Chọn Vụ để xem chi tiết:", options=season_names)
                
                selected_idx = season_names.index(selected_season_name)
                selected_s_id = season_list[selected_idx]['s_id']
                season_start = season_list[selected_idx]['Start']
                
                df_season = df_p.filter(pl.col("s_id") == selected_s_id)
                
                if not df_season.is_empty():
                    daily_stats = df_season.group_by("Date").agg([
                        pl.count().alias("Số lần tưới"),
                        pl.col("duration_s").mean().round(0).alias("Thời gian tưới TB (giây)"),
                        pl.col("val_ec_goc").mean().round(2).alias("TBEC"),
                        pl.col("val_ph_goc").mean().round(2).alias("TBPH")
                    ]).sort("Date")
                    
                    daily_stats = daily_stats.with_columns([
                        ((pl.col("Date") - season_start).dt.total_days() + 1).alias("Ngày thứ")
                    ])
                    
                    fig_season_turns = px.bar(
                        daily_stats.to_pandas(), x="Ngày thứ", y="Số lần tưới",
                        title=f"Biểu đồ Số lần tưới theo ngày - {selected_season_name}", text="Số lần tưới",
                        color="Số lần tưới", color_continuous_scale="Blues"
                    )
                    fig_season_turns.update_traces(textposition='outside')
                    st.plotly_chart(fig_season_turns, use_container_width=True)
                    
                    daily_stats_display = daily_stats.select([
                        "Ngày thứ", "Date", "Số lần tưới", "Thời gian tưới TB (giây)", "TBEC", "TBPH"
                    ]).rename({"Date": "Ngày thực tế"})
                    st.dataframe(daily_stats_display, use_container_width=True, hide_index=True)
            else:
                st.warning("Chưa có dữ liệu vụ canh tác nào đạt điều kiện.")

        with tab3:
            st.subheader("Phân tích toàn bộ dữ liệu châm phân")
            col1, col2 = st.columns(2)
            with col1:
                uploaded_cp_file = st.file_uploader("Tải file log châm phân (JSON/TXT)", type=['txt', 'json'], key="cp_upload")
            with col2:
                target_tank = st.text_input("Tìm kiếm bồn:", "BỒN TG-ANT3").upper()

            if uploaded_cp_file:
                try:
                    data_cp = parse_log_file(uploaded_cp_file)
                    df_cp = pl.DataFrame(data_cp)
                    
                    # BAO QUÁT CẢ TÊN KHU VÀ TÊN BỒN
                    if "Tên khu" in df_cp.columns and "Tên bồn" in df_cp.columns:
                        tank_filter = pl.col("Tên khu").str.to_uppercase().str.contains(target_tank) | pl.col("Tên bồn").str.to_uppercase().str.contains(target_tank)
                    elif "Tên khu" in df_cp.columns:
                        tank_filter = pl.col("Tên khu").str.to_uppercase().str.contains(target_tank)
                    elif "Tên bồn" in df_cp.columns:
                        tank_filter = pl.col("Tên bồn").str.to_uppercase().str.contains(target_tank)
                    else:
                        tank_filter = None

                    if tank_filter is None:
                        st.error("File không hợp lệ: Không tìm thấy trường 'Tên bồn' hoặc 'Tên khu'.")
                    elif "EC yêu cầu" not in df_cp.columns or "Thời gian" not in df_cp.columns:
                        st.error("File không hợp lệ: Cần có các trường 'Thời gian' và 'EC yêu cầu'.")
                    else:
                        df_cp_filtered = df_cp.filter(tank_filter)
                        
                        if df_cp_filtered.is_empty():
                            st.warning(f"Không tìm thấy dữ liệu cho bồn: {target_tank}")
                        else:
                            # BAO QUÁT TẤT CẢ TỪ KHÓA CHỨA CHỮ "BẬT" (Bật, Bật tay, Bật tự động...)
                            if "Trạng thái" in df_cp_filtered.columns:
                                df_cp_filtered = df_cp_filtered.filter(pl.col("Trạng thái").str.to_uppercase().str.contains("BẬT"))
                                
                            df_cp_clean = df_cp_filtered.with_columns([
                                # CẮT CHÍNH XÁC 10 KÝ TỰ ĐẦU (YYYY-MM-DD) - CHỐNG LỖI FORMAT THỜI GIAN
                                pl.col("Thời gian").str.slice(0, 10).str.to_date("%Y-%m-%d", strict=False).alias("Date"),
                                (pl.col("EC yêu cầu").cast(pl.Utf8).str.replace(",", ".").cast(pl.Float64, strict=False) / 100).alias("EC_Yeu_Cau_Thuc_Te")
                            ]).drop_nulls(subset=["Date", "EC_Yeu_Cau_Thuc_Te"])
                            
                            if df_cp_clean.is_empty():
                                st.warning("Không trích xuất được dữ liệu hợp lệ. Có thể EC yêu cầu trống hoặc ngày giờ lỗi.")
                            else:
                                df_cp_daily = df_cp_clean.group_by("Date").agg([
                                    pl.col("EC_Yeu_Cau_Thuc_Te").mean().round(2).alias("Trung bình EC yêu cầu")
                                ]).sort("Date")
                                
                                st.success(f"Đã phân tích toàn bộ tiến trình cho **{target_tank}**")
                                
                                fig_cp = px.line(df_cp_daily.to_pandas(), x="Date", y="Trung bình EC yêu cầu", 
                                                 title=f"Biểu đồ mức EC mục tiêu trung bình theo ngày - {target_tank}",
                                                 markers=True)
                                fig_cp.update_layout(yaxis_title="Mức EC Yêu cầu (mS/cm)")
                                st.plotly_chart(fig_cp, use_container_width=True)
                                
                                st.dataframe(df_cp_daily, use_container_width=True, hide_index=True)
                                
                except Exception as e:
                    st.error(f"Lỗi xử lý hệ thống: {e}")
    else:
        st.error(msg)

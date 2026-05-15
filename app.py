import streamlit as st
import pandas as pd
import numpy as np
import json
import os
import requests
import shutil
import re
from datetime import datetime, timedelta
import io
import uuid
from PIL import Image
from github import Github, GithubException

# ------------------------------- الإعدادات الثابتة -------------------------------
APP_CONFIG = {
    "APP_TITLE": "سكاي- CMMS",
    "APP_ICON": "🏭",
    "REPO_NAME": "mahmedabdallh123/sky",
    "BRANCH": "main",
    "FILE_PATH": "l9.xlsx",
    "LOCAL_FILE": "l9.xlsx",
    "MAX_ACTIVE_USERS": 5,
    "SESSION_DURATION_MINUTES": 60,
    "IMAGES_FOLDER": "event_images",
    "ALLOWED_IMAGE_TYPES": ["jpg", "jpeg", "png", "gif", "bmp", "webp"],
    "MAX_IMAGE_SIZE_MB": 10,
    "DEFAULT_SHEET_COLUMNS": ["مده الاصلاح", "التاريخ", "المعدة", "الحدث/العطل", "الإجراء التصحيحي", "تم بواسطة", "قطع غيار مستخدمة", "نوع العطل", "قدرة الفني (حل/تفكير/مبادرة/قرار)", "الالتزام بتعليمات السلامة", "رابط الصورة"],
    "SPARE_PARTS_SHEET": "قطع_الغيار",
    "SPARE_PARTS_COLUMNS": ["اسم القطعة", "المقاس", "قوه الشد", "الرصيد الموجود", "مدة التوريد", "ضرورية", "القسم", "رابط_الصورة"],
    "MAINTENANCE_SHEET": "صيانة_وقائية",
    "SPARE_PARTS_COLUMNS": ["اسم القطعة", "المقاس", "قوه الشد", "الرصيد الموجود", "مدة التوريد", "ضرورية", "القسم", "رابط_الصورة", "حد_الإنذار"],
    "MAINTENANCE_COLUMNS": ["المعدة", "نوع_الصيانة", "اسم_البند", "الفترة_بالأيام", "آخر_تنفيذ", "التاريخ_التالي", "ملاحظات", "قطع_غيار_مستخدمة_افتراضية", "رابط_الصورة"],
    "GENERAL_SECTION": "عام"
}

# ------------------------------- إعداد الصفحة -------------------------------
st.set_page_config(page_title=APP_CONFIG["APP_TITLE"], layout="wide")

# ------------------------------- استيرادات إضافية مع معالجة الأخطاء -------------------------------
try:
    import plotly.express as px
    import plotly.graph_objects as go
    PLOTLY_AVAILABLE = True
except ImportError:
    PLOTLY_AVAILABLE = False
    try:
        import matplotlib.pyplot as plt
        import matplotlib.dates as mdates
        plt.rcParams['font.family'] = 'Arial'
        MATPLOTLIB_AVAILABLE = True
    except ImportError:
        MATPLOTLIB_AVAILABLE = False

# ------------------------------- ثوابت إضافية -------------------------------
USERS_FILE = "users.json"
STATE_FILE = "state.json"
SESSION_DURATION = timedelta(minutes=APP_CONFIG["SESSION_DURATION_MINUTES"])
MAX_ACTIVE_USERS = APP_CONFIG["MAX_ACTIVE_USERS"]
IMAGES_FOLDER = APP_CONFIG["IMAGES_FOLDER"]
EQUIPMENT_CONFIG_FILE = "equipment_config.json"
SUPPORT_CONFIG_FILE = "support_config.json"   # جديد: إعدادات الدعم الفني

GITHUB_EXCEL_URL = f"https://github.com/{APP_CONFIG['REPO_NAME'].split('/')[0]}/{APP_CONFIG['REPO_NAME'].split('/')[1]}/raw/{APP_CONFIG['BRANCH']}/{APP_CONFIG['FILE_PATH']}"
GITHUB_USERS_URL = "https://raw.githubusercontent.com/mahmedabdallh123/sky/refs/heads/main/users.json"
GITHUB_REPO_USERS = "mahmedabdallh123/sky"
GITHUB_TOKEN = st.secrets.get("github", {}).get("token", None)
GITHUB_AVAILABLE = GITHUB_TOKEN is not None
ACTIVITY_LOG_FILE = "activity_log.json"

# ------------------------------- دوال رفع الصور -------------------------------
def upload_image_to_github(image_file, entity_type, entity_id, custom_filename=None):
    if not GITHUB_AVAILABLE:
        st.error("❌ GitHub token غير متوفر، لا يمكن رفع الصور")
        return None
    try:
        img = Image.open(image_file)
        if img.mode in ('RGBA', 'LA', 'P'):
            img = img.convert('RGB')
        buffer = io.BytesIO()
        img.save(buffer, format='JPEG', quality=85, optimize=True)
        buffer.seek(0)
        if custom_filename:
            filename = custom_filename
        else:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"{entity_type}_{entity_id}_{timestamp}.jpg"
        repo_path = f"{IMAGES_FOLDER}/{entity_type}/{filename}"
        g = Github(GITHUB_TOKEN)
        repo = g.get_repo(APP_CONFIG["REPO_NAME"])
        try:
            repo.get_contents(f"{IMAGES_FOLDER}/{entity_type}/", ref=APP_CONFIG["BRANCH"])
        except GithubException:
            repo.create_file(f"{IMAGES_FOLDER}/{entity_type}/.gitkeep", f"Create folder for {entity_type} images", "", branch=APP_CONFIG["BRANCH"])
        content = buffer.getvalue()
        result = repo.create_file(path=repo_path, message=f"Add image for {entity_type} {entity_id}", content=content, branch=APP_CONFIG["BRANCH"])
        return f"https://raw.githubusercontent.com/{APP_CONFIG['REPO_NAME']}/{APP_CONFIG['BRANCH']}/{repo_path}"
    except Exception as e:
        st.error(f"❌ خطأ في معالجة الصورة: {e}")
        return None

def get_image_component(image_url, caption=""):
    if not image_url or not isinstance(image_url, str):
        return None
    try:
        return st.image(image_url, caption=caption, use_container_width=True)
    except:
        st.warning(f"⚠️ تعذر عرض الصورة: {image_url}")
        return None

# ------------------------------- دوال إعدادات الدعم الفني (جديدة) -------------------------------
def load_support_config():
    """تحميل إعدادات الدعم الفني (رابط الصورة، رابط اليوتيوب)"""
    default_config = {"image_url": "", "youtube_link": ""}
    if GITHUB_AVAILABLE:
        try:
            g = Github(GITHUB_TOKEN)
            repo = g.get_repo(APP_CONFIG["REPO_NAME"])
            contents = repo.get_contents(SUPPORT_CONFIG_FILE, ref=APP_CONFIG["BRANCH"])
            import base64
            content = base64.b64decode(contents.content).decode('utf-8')
            config = json.loads(content)
            return config
        except:
            pass
    if os.path.exists(SUPPORT_CONFIG_FILE):
        try:
            with open(SUPPORT_CONFIG_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except:
            return default_config
    return default_config

def save_support_config(config):
    """حفظ إعدادات الدعم الفني على GitHub والمحلي"""
    config_str = json.dumps(config, indent=2, ensure_ascii=False)
    if GITHUB_AVAILABLE:
        try:
            g = Github(GITHUB_TOKEN)
            repo = g.get_repo(APP_CONFIG["REPO_NAME"])
            try:
                contents = repo.get_contents(SUPPORT_CONFIG_FILE, ref=APP_CONFIG["BRANCH"])
                repo.update_file(SUPPORT_CONFIG_FILE, "تحديث إعدادات الدعم الفني", config_str, contents.sha, branch=APP_CONFIG["BRANCH"])
            except:
                repo.create_file(SUPPORT_CONFIG_FILE, "إنشاء إعدادات الدعم الفني", config_str, branch=APP_CONFIG["BRANCH"])
        except:
            pass
    with open(SUPPORT_CONFIG_FILE, "w", encoding="utf-8") as f:
        json.dump(config, f, indent=2, ensure_ascii=False)

# ------------------------------- دوال قطع الغيار -------------------------------
def load_spare_parts():
    if not os.path.exists(APP_CONFIG["LOCAL_FILE"]):
        return pd.DataFrame(columns=APP_CONFIG["SPARE_PARTS_COLUMNS"])
    try:
        df = pd.read_excel(APP_CONFIG["LOCAL_FILE"], sheet_name=APP_CONFIG["SPARE_PARTS_SHEET"])
        df.columns = df.columns.astype(str).str.strip()
        for col in APP_CONFIG["SPARE_PARTS_COLUMNS"]:
            if col not in df.columns:
                df[col] = ""
        df = df.fillna("")
        df["الرصيد الموجود"] = pd.to_numeric(df["الرصيد الموجود"], errors='coerce').fillna(0)
        if "حد_الإنذار" not in df.columns:
            df["حد_الإنذار"] = 1
        else:
            df["حد_الإنذار"] = pd.to_numeric(df["حد_الإنذار"], errors='coerce').fillna(1)
        return df
    except Exception:
        return pd.DataFrame(columns=APP_CONFIG["SPARE_PARTS_COLUMNS"])

def get_spare_parts_for_section(section_name):
    df = load_spare_parts()
    if df.empty:
        return []
    filtered = df[(df["القسم"] == section_name) | (df["القسم"] == APP_CONFIG["GENERAL_SECTION"])]
    return list(zip(filtered["اسم القطعة"], filtered["الرصيد الموجود"]))

def consume_spare_part(part_name, quantity=1):
    df = load_spare_parts()
    if df.empty:
        return False, "لا توجد قطع غيار مسجلة", None
    mask = df["اسم القطعة"] == part_name
    if not mask.any():
        return False, f"القطعة '{part_name}' غير موجودة", None
    current_qty = df.loc[mask, "الرصيد الموجود"].values[0]
    if current_qty < quantity:
        return False, f"الرصيد غير كافٍ (الموجود: {current_qty}, المطلوب: {quantity})", current_qty
    new_qty = current_qty - quantity
    df.loc[mask, "الرصيد الموجود"] = new_qty
    if "temp_spare_parts_df" not in st.session_state:
        st.session_state.temp_spare_parts_df = df
    else:
        st.session_state.temp_spare_parts_df = df
    return True, f"تم خصم {quantity} من '{part_name}'، الرصيد الجديد: {new_qty}", new_qty

def get_critical_spare_parts():
    df = load_spare_parts()
    if df.empty:
        return []
    df["الرصيد الموجود"] = pd.to_numeric(df["الرصيد الموجود"], errors='coerce').fillna(0)
    if "حد_الإنذار" not in df.columns:
        df["حد_الإنذار"] = 1
    else:
        df["حد_الإنذار"] = pd.to_numeric(df["حد_الإنذار"], errors='coerce').fillna(1)
    if "القسم" not in df.columns:
        return []
    df["القسم"] = df["القسم"].fillna("").astype(str)
    df = df[df["القسم"].str.strip() != ""]
    df["ضرورية"] = df["ضرورية"].astype(str).str.strip()
    critical = df[(df["ضرورية"] == "نعم") & (df["الرصيد الموجود"] < df["حد_الإنذار"])]
    result = critical[["اسم القطعة", "القسم", "الرصيد الموجود", "حد_الإنذار"]].to_dict('records')
    return result

# ------------------------------- دوال سجل النشاطات -------------------------------
def log_activity(action_type, details, username=None):
    if username is None:
        username = st.session_state.get("username", "غير معروف")
    log_entry = {"timestamp": datetime.now().isoformat(), "username": username, "action_type": action_type, "details": details}
    log = []
    if os.path.exists(ACTIVITY_LOG_FILE):
        try:
            with open(ACTIVITY_LOG_FILE, "r", encoding="utf-8") as f:
                log = json.load(f)
        except:
            log = []
    log.append(log_entry)
    if len(log) > 100:
        log = log[-100:]
    with open(ACTIVITY_LOG_FILE, "w", encoding="utf-8") as f:
        json.dump(log, f, indent=2, ensure_ascii=False)
    if GITHUB_AVAILABLE:
        try:
            g = Github(GITHUB_TOKEN)
            repo = g.get_repo(APP_CONFIG["REPO_NAME"])
            content = json.dumps(log, indent=2, ensure_ascii=False)
            try:
                contents = repo.get_contents(ACTIVITY_LOG_FILE, ref=APP_CONFIG["BRANCH"])
                repo.update_file(ACTIVITY_LOG_FILE, "تحديث سجل النشاطات", content, contents.sha, branch=APP_CONFIG["BRANCH"])
            except:
                repo.create_file(ACTIVITY_LOG_FILE, "إنشاء سجل النشاطات", content, branch=APP_CONFIG["BRANCH"])
        except:
            pass

def load_activity_log():
    if GITHUB_AVAILABLE:
        try:
            g = Github(GITHUB_TOKEN)
            repo = g.get_repo(APP_CONFIG["REPO_NAME"])
            contents = repo.get_contents(ACTIVITY_LOG_FILE, ref=APP_CONFIG["BRANCH"])
            import base64
            content = base64.b64decode(contents.content).decode('utf-8')
            return json.loads(content)
        except:
            pass
    if os.path.exists(ACTIVITY_LOG_FILE):
        with open(ACTIVITY_LOG_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return []

# ------------------------------- دوال الصيانة الوقائية -------------------------------
def load_maintenance_tasks():
    if not os.path.exists(APP_CONFIG["LOCAL_FILE"]):
        return pd.DataFrame(columns=APP_CONFIG["MAINTENANCE_COLUMNS"])
    try:
        df = pd.read_excel(APP_CONFIG["LOCAL_FILE"], sheet_name=APP_CONFIG["MAINTENANCE_SHEET"])
        df.columns = df.columns.astype(str).str.strip()
        for col in APP_CONFIG["MAINTENANCE_COLUMNS"]:
            if col not in df.columns:
                df[col] = ""
        df = df.fillna("")
        if "آخر_تنفيذ" in df.columns:
            df["آخر_تنفيذ"] = pd.to_datetime(df["آخر_تنفيذ"], errors='coerce')
        if "التاريخ_التالي" in df.columns:
            df["التاريخ_التالي"] = pd.to_datetime(df["التاريخ_التالي"], errors='coerce')
        if "الفترة_بالأيام" in df.columns:
            df["الفترة_بالأيام"] = pd.to_numeric(df["الفترة_بالأيام"], errors='coerce').fillna(0)
        return df
    except Exception as e:
        st.error(f"خطأ في تحميل مهام الصيانة: {e}")
        return pd.DataFrame(columns=APP_CONFIG["MAINTENANCE_COLUMNS"])

def get_tasks_for_equipment(equipment_name):
    df = load_maintenance_tasks()
    if df.empty:
        return df
    return df[df["المعدة"] == equipment_name]

def add_maintenance_task(sheets_edit, equipment, task_name, period_hours, start_date=None, notes="", default_spare="", image_url=None):
    df = sheets_edit.get(APP_CONFIG["MAINTENANCE_SHEET"])
    if df is None:
        df = pd.DataFrame(columns=APP_CONFIG["MAINTENANCE_COLUMNS"])
    if start_date is None:
        start_date = datetime.now().date()
    period_days = period_hours / 24.0
    next_date = start_date + timedelta(days=period_days)
    new_row = pd.DataFrame([{
        "المعدة": equipment, "نوع_الصيانة": f"{period_hours} ساعة", "اسم_البند": task_name,
        "الفترة_بالأيام": period_days, "آخر_تنفيذ": pd.NaT, "التاريخ_التالي": next_date,
        "ملاحظات": notes, "قطع_غيار_مستخدمة_افتراضية": default_spare, "رابط_الصورة": image_url or ""
    }])
    new_df = pd.concat([df, new_row], ignore_index=True)
    sheets_edit[APP_CONFIG["MAINTENANCE_SHEET"]] = new_df
    log_activity("add_maintenance_task", f"تم إضافة بند صيانة '{task_name}' للماكينة {equipment} (فترة {period_hours} ساعة)")
    return sheets_edit

def get_upcoming_maintenance(days_ahead=3):
    df = load_maintenance_tasks()
    if df.empty:
        return pd.DataFrame(), pd.DataFrame()
    today = datetime.now().date()
    overdue = df[df["التاريخ_التالي"] < pd.Timestamp(today)]
    upcoming = df[(df["التاريخ_التالي"] >= pd.Timestamp(today)) & (df["التاريخ_التالي"] <= pd.Timestamp(today + timedelta(days=days_ahead)))]
    return overdue, upcoming

# ------------------------------- دوال تحليل الأعطال المتقدمة -------------------------------
def flexible_date_parser(date_series):
    """تحويل سلسلة من التواريخ بتنسيقات متعددة إلى datetime، مع تجاهل الأخطاء."""
    def parse_single(val):
        if pd.isna(val) or val == "":
            return pd.NaT
        if isinstance(val, (pd.Timestamp, datetime)):
            return val
        val_str = str(val).strip()
        val_str = val_str.replace('\\', '/')
        try:
            return pd.to_datetime(val_str, format='%Y-%m-%d', errors='raise')
        except:
            pass
        try:
            return pd.to_datetime(val_str, format='%d/%m/%Y', errors='raise')
        except:
            pass
        try:
            return pd.to_datetime(val_str, format='%d-%m-%Y', errors='raise')
        except:
            pass
        try:
            return pd.to_datetime(val_str, format='%d.%m.%Y', errors='raise')
        except:
            pass
        try:
            return pd.to_datetime(val_str, format='%Y/%m/%d', errors='raise')
        except:
            pass
        return pd.to_datetime(val_str, errors='coerce')
    return date_series.apply(parse_single)

def analyze_time_between_corrections(df, filter_text=None):
    """تحليل المدة الزمنية بين الإجراءات التصحيحية المتكررة (حسب كلمة البحث)"""
    if df is None or df.empty:
        return pd.DataFrame()
    data = df.copy()
    if "التاريخ" not in data.columns or "المعدة" not in data.columns or "الإجراء التصحيحي" not in data.columns:
        return pd.DataFrame()
    
    data["التاريخ"] = flexible_date_parser(data["التاريخ"])
    data = data.dropna(subset=["التاريخ"]).sort_values(["المعدة", "التاريخ"])
    
    if filter_text:
        data["الإجراء التصحيحي"] = data["الإجراء التصحيحي"].fillna("").astype(str)
        data = data[data["الإجراء التصحيحي"].str.contains(filter_text, case=False, na=False)]
    
    results = []
    for equipment in data["المعدة"].unique():
        eq_data = data[data["المعدة"] == equipment].copy()
        if len(eq_data) < 2:
            continue
        for i in range(len(eq_data)-1):
            current = eq_data.iloc[i]
            next_row = eq_data.iloc[i+1]
            gap_days = (next_row["التاريخ"] - current["التاريخ"]).total_seconds() / (24 * 3600)
            prev_correction = eq_data.iloc[i-1]["الإجراء التصحيحي"] if i > 0 else None
            prev_date = eq_data.iloc[i-1]["التاريخ"] if i > 0 else None
            
            results.append({
                "المعدة": equipment,
                "الإجراء السابق": prev_correction if prev_correction else "---",
                "تاريخ الإجراء السابق": prev_date.strftime("%Y-%m-%d") if prev_date else "---",
                "الإجراء التالي": next_row["الإجراء التصحيحي"],
                "تاريخ الإجراء التالي": next_row["التاريخ"].strftime("%Y-%m-%d"),
                "المدة الزمنية (أيام)": round(gap_days, 1)
            })
    result_df = pd.DataFrame(results)
    if result_df.empty:
        return pd.DataFrame()
    result_df.reset_index(drop=True, inplace=True)
    return result_df

def failures_analysis_tab(all_sheets):
    st.header("📊 تحليل الإجراءات التصحيحية المتكررة")
    if not all_sheets:
        st.warning("لا توجد بيانات للتحليل")
        return
    
    username = st.session_state.get("username")
    
    # الحصول على الأقسام المسموح للمستخدم بالوصول إليها (view)
    allowed_sections = get_allowed_sections(all_sheets, username, "view")
    
    if not allowed_sections:
        st.warning("لا توجد أقسام مسموح لك بالوصول إليها للتحليل")
        return
    
    selected_section = st.selectbox("🏭 اختر القسم:", allowed_sections, key="analysis_section")
    df = all_sheets[selected_section].copy()
    
    if "المعدة" not in df.columns:
        st.error(f"⚠️ القسم '{selected_section}' لا يحتوي على عمود 'المعدة'")
        return
    
    df["المعدة"] = df["المعدة"].astype(str).str.strip()
    
    equipment_list = get_equipment_list_from_sheet(df)
    if not equipment_list:
        st.warning(f"⚠️ لا توجد ماكينات مسجلة في قسم '{selected_section}'")
        return
    
    equipment_options = ["جميع الماكينات"] + equipment_list
    selected_equipment = st.selectbox("🔧 اختر الماكينة:", equipment_options, key="analysis_equipment")
    
    col1, col2 = st.columns(2)
    with col1:
        start_date = st.date_input("📅 من تاريخ (اختياري):", value=None, key="start_date_filter")
    with col2:
        end_date = st.date_input("📅 إلى تاريخ (اختياري):", value=None, key="end_date_filter")
    
    search_text = st.text_input("🔍 كلمة البحث في الإجراء التصحيحي (اختياري):", placeholder="مثال: سير, كويلر, 1270", key="search_text_analysis")
    
    if st.button("🔄 تشغيل التحليل", key="run_analysis", type="primary"):
        filtered_df = df.copy()
        
        if selected_equipment != "جميع الماكينات":
            filtered_df = filtered_df[filtered_df["المعدة"] == selected_equipment]
        
        if "التاريخ" in filtered_df.columns:
            filtered_df["التاريخ"] = flexible_date_parser(filtered_df["التاريخ"])
            filtered_df = filtered_df.dropna(subset=["التاريخ"])
            if start_date:
                filtered_df = filtered_df[filtered_df["التاريخ"] >= pd.to_datetime(start_date)]
            if end_date:
                filtered_df = filtered_df[filtered_df["التاريخ"] <= pd.to_datetime(end_date) + timedelta(days=1)]
        
        if filtered_df.empty:
            st.warning("⚠️ لا توجد بيانات تطابق معايير التصفية")
            return
        
        details_gaps = analyze_time_between_corrections(filtered_df, search_text if search_text else None)
        
        if "الإجراء التصحيحي" in filtered_df.columns:
            top_corrections = filtered_df["الإجراء التصحيحي"].value_counts().reset_index().head(10)
            top_corrections.columns = ["الإجراء التصحيحي", "عدد المرات"]
        else:
            top_corrections = pd.DataFrame()
        
        if selected_equipment == "جميع الماكينات" and "المعدة" in filtered_df.columns:
            top_equipment = filtered_df["المعدة"].value_counts().reset_index().head(10)
            top_equipment.columns = ["المعدة", "عدد الأعطال"]
        else:
            top_equipment = pd.DataFrame()
        
        st.success(f"✅ تم العثور على {len(filtered_df)} إجراء تصحيحي")
        
        if not top_corrections.empty:
            st.subheader("🔝 أكثر الإجراءات التصحيحية تكراراً")
            st.dataframe(top_corrections, use_container_width=True)
        
        if not top_equipment.empty:
            st.subheader("🏭 أكثر الماكينات التي تحتاج إجراءات تصحيحية")
            st.dataframe(top_equipment, use_container_width=True)
        
        st.subheader("📋 الفجوات الزمنية التفصيلية بين الإجراءات التصحيحية المتكررة")
        if search_text:
            st.info(f"ℹ️ يتم حساب الفجوات فقط بين الإجراءات التي تحتوي على النص: **'{search_text}'**")
        
        if details_gaps.empty:
            st.info("ℹ️ لا توجد بيانات كافية لحساب الفجوات (يلزم على الأقل إجراءان لنفس الماكينة ويحتويان على كلمة البحث إن وجدت)")
        else:
            st.dataframe(details_gaps, use_container_width=True, height=500)
            csv = details_gaps.to_csv(index=False).encode('utf-8')
            st.download_button("📥 تحميل الفجوات التفصيلية CSV", csv, "detailed_corrections_gaps.csv", "text/csv")
        
        st.markdown("---")
        st.subheader("📥 تصدير التقرير كامل (Excel)")
        excel_buffer = io.BytesIO()
        with pd.ExcelWriter(excel_buffer, engine='openpyxl') as writer:
            filtered_df.to_excel(writer, sheet_name="البيانات الأصلية", index=False)
            if not top_corrections.empty:
                top_corrections.to_excel(writer, sheet_name="الإجراءات الأكثر تكراراً", index=False)
            if not top_equipment.empty:
                top_equipment.to_excel(writer, sheet_name="الماكينات الأكثر احتياجاً", index=False)
            if not details_gaps.empty:
                details_gaps.to_excel(writer, sheet_name="الفجوات التفصيلية", index=False)
        excel_buffer.seek(0)
        st.download_button(
            "📥 تحميل التقرير (Excel)",
            excel_buffer,
            f"corrections_analysis_{selected_section}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )

def download_users_from_github():
    try:
        response = requests.get(GITHUB_USERS_URL, timeout=10)
        response.raise_for_status()
        users_data = response.json()
        for username, user_info in users_data.items():
            if "permissions" in user_info and isinstance(user_info["permissions"], list):
                if "all" in user_info["permissions"]:
                    user_info["permissions"] = {"all_sections": True}
                else:
                    user_info["permissions"] = {"all_sections": False}
                user_info["sections_permissions"] = {}
            elif "permissions" not in user_info:
                user_info["permissions"] = {"all_sections": False}
                user_info["sections_permissions"] = {}
        with open(USERS_FILE, "w", encoding="utf-8") as f:
            json.dump(users_data, f, indent=4, ensure_ascii=False)
        return users_data
    except Exception as e:
        st.warning(f"لم نتمكن من تحميل users.json من GitHub: {e}")
        if os.path.exists(USERS_FILE):
            with open(USERS_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        return None

def upload_users_to_github(users_data):
    try:
        token = st.secrets.get("github", {}).get("token", None)
        if not token:
            st.error("❌ لم يتم العثور على GitHub token")
            return False
        g = Github(token)
        repo = g.get_repo(GITHUB_REPO_USERS)
        users_json = json.dumps(users_data, indent=4, ensure_ascii=False, sort_keys=True)
        try:
            contents = repo.get_contents("users.json", ref="main")
            repo.update_file(path="users.json", message="تحديث ملف المستخدمين", content=users_json, sha=contents.sha, branch="main")
            return True
        except:
            repo.create_file(path="users.json", message="إنشاء ملف المستخدمين", content=users_json, branch="main")
            return True
    except Exception as e:
        st.error(f"❌ فشل رفع المستخدمين: {e}")
        return False

def load_users():
    try:
        users_data = download_users_from_github()
        if not users_data or "admin" not in users_data:
            if os.path.exists(USERS_FILE):
                with open(USERS_FILE, "r", encoding="utf-8") as f:
                    local_users = json.load(f)
                    if "admin" in local_users:
                        return local_users
            default_users = {
                "admin": {"password": "1234", "role": "admin", "permissions": {"all_sections": True}, "sections_permissions": {}},
                "مدير_صيانة": {"password": "12345", "role": "admin", "permissions": {"all_sections": True}, "sections_permissions": {}}
            }
            return default_users
        return users_data
    except Exception as e:
        st.error(f"خطأ في تحميل المستخدمين: {e}")
        return {"admin": {"password": "1234", "role": "admin", "permissions": {"all_sections": True}, "sections_permissions": {}}}

def load_state():
    if not os.path.exists(STATE_FILE):
        with open(STATE_FILE, "w", encoding="utf-8") as f:
            json.dump({}, f)
        return {}
    try:
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except:
        return {}

def save_state(state):
    with open(STATE_FILE, "w", encoding="utf-8") as f:
        json.dump(state, f, indent=4, ensure_ascii=False)

def cleanup_sessions(state):
    now = datetime.now()
    changed = False
    for user, info in list(state.items()):
        if info.get("active") and "login_time" in info:
            try:
                login_time = datetime.fromisoformat(info["login_time"])
                if now - login_time > SESSION_DURATION:
                    info["active"] = False
                    info.pop("login_time", None)
                    changed = True
            except:
                info["active"] = False
                changed = True
    if changed:
        save_state(state)
    return state

def remaining_time(state, username):
    if not username or username not in state:
        return None
    info = state.get(username)
    if not info or not info.get("active"):
        return None
    try:
        lt = datetime.fromisoformat(info["login_time"])
        remaining = SESSION_DURATION - (datetime.now() - lt)
        if remaining.total_seconds() <= 0:
            return None
        return remaining
    except:
        return None

def logout_action():
    state = load_state()
    username = st.session_state.get("username")
    if username and username in state:
        state[username]["active"] = False
        state[username].pop("login_time", None)
        save_state(state)
    for k in list(st.session_state.keys()):
        st.session_state.pop(k, None)
    st.rerun()

def login_ui():
    users = load_users()
    state = cleanup_sessions(load_state())
    if "logged_in" not in st.session_state:
        st.session_state.logged_in = False
        st.session_state.username = None
        st.session_state.user_role = None
        st.session_state.user_permissions = []
    st.title(f"{APP_CONFIG['APP_ICON']} تسجيل الدخول - {APP_CONFIG['APP_TITLE']}")
    username_input = st.selectbox("اختر المستخدم", list(users.keys()))
    password = st.text_input("كلمة المرور", type="password")
    active_users = [u for u, v in state.items() if v.get("active")]
    active_count = len(active_users)
    st.caption(f"المستخدمون النشطون: {active_count} / {MAX_ACTIVE_USERS}")
    if not st.session_state.logged_in:
        if st.button("تسجيل الدخول"):
            current_users = load_users()
            if username_input in current_users and current_users[username_input]["password"] == password:
                if username_input != "admin" and username_input in active_users:
                    st.warning("هذا المستخدم مسجل دخول بالفعل.")
                    return False
                elif active_count >= MAX_ACTIVE_USERS and username_input != "admin":
                    st.error("الحد الأقصى للمستخدمين المتصلين.")
                    return False
                state[username_input] = {"active": True, "login_time": datetime.now().isoformat()}
                save_state(state)
                st.session_state.logged_in = True
                st.session_state.username = username_input
                st.session_state.user_role = current_users[username_input].get("role", "viewer")
                st.session_state.user_permissions = current_users[username_input].get("permissions", ["view"])
                st.success(f"تم تسجيل الدخول: {username_input}")
                st.rerun()
            else:
                st.error("كلمة المرور غير صحيحة.")
        return False
    else:
        st.success(f"مسجل الدخول كـ: {st.session_state.username}")
        rem = remaining_time(state, st.session_state.username)
        if rem:
            mins, secs = divmod(int(rem.total_seconds()), 60)
            st.info(f"الوقت المتبقي: {mins:02d}:{secs:02d}")
        if st.button("تسجيل الخروج"):
            logout_action()
        return True

# ------------------------------- دوال الصلاحيات -------------------------------
def get_user_permissions(username):
    users = load_users()
    if username not in users:
        return {"all_sections": False, "sections_permissions": {}}
    user_data = users[username]
    if "permissions" in user_data and isinstance(user_data["permissions"], dict):
        perms = user_data["permissions"]
    elif "permissions" in user_data and isinstance(user_data["permissions"], list):
        if "all" in user_data["permissions"]:
            perms = {"all_sections": True}
        else:
            perms = {"all_sections": False}
    else:
        perms = {"all_sections": False}
    if "sections_permissions" not in user_data:
        user_data["sections_permissions"] = {}
    return {"all_sections": perms.get("all_sections", False), "sections_permissions": user_data.get("sections_permissions", {})}

def has_section_permission(username, section_name, required_permission="view"):
    if username == "admin":
        return True
    permissions = get_user_permissions(username)
    if not permissions:
        return False
    if permissions.get("all_sections", False):
        return True
    section_perms = permissions.get("sections_permissions", {}).get(section_name, [])
    return required_permission in section_perms

def get_allowed_sections(all_sheets, username, required_permission="view"):
    """إرجاع قائمة الأقسام الحقيقية التي يسمح للمستخدم بالوصول إليها (بدون إضافة 'عام')"""
    allowed = []
    for sheet_name in all_sheets.keys():
        if sheet_name in [APP_CONFIG["SPARE_PARTS_SHEET"], APP_CONFIG["MAINTENANCE_SHEET"]]:
            continue
        if has_section_permission(username, sheet_name, required_permission):
            allowed.append(sheet_name)
    return allowed

# ------------------------------- دوال الملفات -------------------------------
def fetch_from_github_requests():
    try:
        response = requests.get(GITHUB_EXCEL_URL, stream=True, timeout=15)
        response.raise_for_status()
        with open(APP_CONFIG["LOCAL_FILE"], "wb") as f:
            shutil.copyfileobj(response.raw, f)
        st.cache_data.clear()
        return True
    except Exception as e:
        st.error(f"فشل التحديث: {e}")
        return False

@st.cache_data(show_spinner=False)
def load_all_sheets():
    if not os.path.exists(APP_CONFIG["LOCAL_FILE"]):
        return None
    try:
        sheets = pd.read_excel(APP_CONFIG["LOCAL_FILE"], sheet_name=None)
        if not sheets:
            return None
        for name, df in sheets.items():
            if df.empty:
                continue
            df.columns = df.columns.astype(str).str.strip()
            df = df.fillna('')
            sheets[name] = df
        return sheets
    except Exception as e:
        st.error(f"خطأ في تحميل الأقسام: {e}")
        return None

@st.cache_data(show_spinner=False)
def load_sheets_for_edit():
    if not os.path.exists(APP_CONFIG["LOCAL_FILE"]):
        return None
    try:
        sheets = pd.read_excel(APP_CONFIG["LOCAL_FILE"], sheet_name=None, dtype=object)
        if not sheets:
            return None
        for name, df in sheets.items():
            df.columns = df.columns.astype(str).str.strip()
            df = df.fillna('')
            sheets[name] = df
        return sheets
    except Exception as e:
        st.error(f"خطأ في تحميل الأقسام: {e}")
        return None

def save_excel_locally(sheets_dict):
    try:
        if "temp_spare_parts_df" in st.session_state:
            sheets_dict[APP_CONFIG["SPARE_PARTS_SHEET"]] = st.session_state.temp_spare_parts_df
            del st.session_state.temp_spare_parts_df
        if APP_CONFIG["MAINTENANCE_SHEET"] not in sheets_dict:
            sheets_dict[APP_CONFIG["MAINTENANCE_SHEET"]] = load_maintenance_tasks()
        with pd.ExcelWriter(APP_CONFIG["LOCAL_FILE"], engine="openpyxl") as writer:
            for name, sh in sheets_dict.items():
                try:
                    sh.to_excel(writer, sheet_name=name, index=False)
                except Exception:
                    sh.astype(object).to_excel(writer, sheet_name=name, index=False)
        return True
    except Exception as e:
        st.error(f"❌ خطأ في الحفظ المحلي: {e}")
        return False

def push_to_github():
    try:
        token = st.secrets.get("github", {}).get("token", None)
        if not token:
            st.error("❌ لم يتم العثور على GitHub token في secrets")
            return False
        if not GITHUB_AVAILABLE:
            st.error("❌ PyGithub غير متوفر")
            return False
        g = Github(token)
        repo = g.get_repo(APP_CONFIG["REPO_NAME"])
        with open(APP_CONFIG["LOCAL_FILE"], "rb") as f:
            content = f.read()
        try:
            contents = repo.get_contents(APP_CONFIG["FILE_PATH"], ref=APP_CONFIG["BRANCH"])
            repo.update_file(path=APP_CONFIG["FILE_PATH"], message=f"تحديث البيانات - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}", content=content, sha=contents.sha, branch=APP_CONFIG["BRANCH"])
            st.success("✅ تم رفع التغييرات إلى GitHub")
            return True
        except GithubException as e:
            if e.status == 404:
                repo.create_file(path=APP_CONFIG["FILE_PATH"], message=f"إنشاء ملف جديد - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}", content=content, branch=APP_CONFIG["BRANCH"])
                st.success("✅ تم إنشاء الملف على GitHub")
                return True
            else:
                st.error(f"❌ خطأ GitHub: {e}")
                return False
    except Exception as e:
        st.error(f"❌ فشل الرفع: {e}")
        return False

def save_and_push_to_github(sheets_dict, operation_name):
    st.info(f"💾 جاري حفظ {operation_name}...")
    if save_excel_locally(sheets_dict):
        st.success("✅ تم الحفظ محلياً")
        if push_to_github():
            st.success("✅ تم الرفع إلى GitHub")
            st.cache_data.clear()
            return True
        else:
            st.warning("⚠️ تم الحفظ محلياً فقط")
            return True
    else:
        st.error("❌ فشل الحفظ المحلي")
        return False

# ------------------------------- دوال التصدير والعرض -------------------------------
def export_sheet_to_excel(sheets_dict, sheet_name):
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df = sheets_dict[sheet_name]
        df.to_excel(writer, sheet_name=sheet_name, index=False)
    output.seek(0)
    return output

def export_all_sheets_to_excel(sheets_dict):
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        for sheet_name, df in sheets_dict.items():
            df.to_excel(writer, sheet_name=sheet_name, index=False)
    output.seek(0)
    return output

def export_filtered_results_to_excel(results_df, sheet_name):
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        results_df.to_excel(writer, sheet_name=sheet_name, index=False)
    output.seek(0)
    return output

def display_sheet_data(sheet_name, df, unique_id, sheets_edit):
    st.markdown(f"### 🏭 {sheet_name}")
    st.info(f"عدد الماكينات المسجلة: {len(df)} | عدد الأعمدة: {len(df.columns)}")
    equipment_list = get_equipment_list_from_sheet(df)
    if equipment_list and "المعدة" in df.columns:
        st.markdown("#### 🔍 فلتر حسب الماكينة:")
        selected_filter = st.selectbox("اختر الماكينة:", ["جميع الماكينات"] + equipment_list, key=f"filter_{unique_id}")
        if selected_filter != "جميع الماكينات":
            df = df[df["المعدة"] == selected_filter]
            st.info(f"عرض لماكينة: {selected_filter} - السجلات: {len(df)}")
    display_df = df.copy()
    for col in display_df.columns:
        if display_df[col].dtype == 'object':
            display_df[col] = display_df[col].astype(str).apply(lambda x: x[:100] + "..." if len(x) > 100 else x)
    if "رابط الصورة" in display_df.columns:
        display_df = display_df.drop(columns=["رابط الصورة"])
    st.dataframe(display_df, use_container_width=True, height=400)
    if "رابط الصورة" in df.columns and not df["رابط الصورة"].isnull().all():
        st.markdown("#### 🖼️ الصور المرفقة")
        for idx, row in df.iterrows():
            img_url = row["رابط الصورة"]
            if img_url and isinstance(img_url, str) and img_url.strip() != "":
                with st.expander(f"📸 صورة للصف رقم {idx+1}"):
                    try:
                        st.image(img_url, use_container_width=True)
                        st.caption(f"[رابط الصورة]({img_url})")
                    except Exception as e:
                        st.warning(f"⚠️ تعذر عرض الصورة: {e}")
    col_btn1, col_btn2 = st.columns(2)
    with col_btn1:
        excel_file = export_sheet_to_excel({sheet_name: df}, sheet_name)
        st.download_button("📥 تحميل بيانات هذا القسم كملف Excel", excel_file, f"{sheet_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", key=f"export_sheet_{unique_id}")
    with col_btn2:
        all_sheets_excel = export_all_sheets_to_excel({sheet_name: df})
        st.download_button("📥 تحميل جميع البيانات كملف Excel", all_sheets_excel, f"all_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", key=f"export_all_{unique_id}")

def search_across_sheets(all_sheets):
    st.subheader("بحث متقدم في السجلات")
    if not all_sheets:
        st.warning("لا توجد بيانات للبحث")
        return

    username = st.session_state.get("username")
    
    search_type = st.selectbox("نوع البيانات المراد البحث فيها:", ["الأقسام (الأعطال)", "قطع الغيار", "الصيانة الوقائية"], key="search_type")

    allowed_sections = get_allowed_sections(all_sheets, username, "view")
    
    selected_section_filter = "جميع الأقسام"
    if search_type in ["قطع الغيار", "الصيانة الوقائية"] and allowed_sections:
        section_options = ["جميع الأقسام"] + allowed_sections
        selected_section_filter = st.selectbox("🏭 القسم:", section_options, key="section_filter")

    if search_type == "الأقسام (الأعطال)":
        sheet_options = ["جميع الأقسام"] + allowed_sections
        selected_sheet = st.selectbox("اختر القسم للبحث:", sheet_options, key="search_sheet")
        
        if selected_sheet != "جميع الأقسام":
            df_temp = all_sheets[selected_sheet]
            equipment_list = get_equipment_list_from_sheet(df_temp)
        else:
            all_eq = set()
            for sh_name in allowed_sections:
                df_temp = all_sheets[sh_name]
                all_eq.update(get_equipment_list_from_sheet(df_temp))
            equipment_list = sorted(all_eq)
        filter_equipment = st.selectbox("فلتر حسب الماكينة:", ["الكل"] + equipment_list, key="search_eq")
        
        col1, col2 = st.columns(2)
        with col1:
            general_search = st.text_input("🔍 كلمة البحث العامة (في الحدث/الإجراء):", placeholder="مثال: تسريب زيت, قطع سير...")
        with col2:
            technician_search = st.text_input("👨‍🔧 بحث بالفني (تم بواسطة):", placeholder="أدخل اسم الفني...")
        
        st.markdown("#### نطاق التاريخ")
        use_date_filter = st.checkbox("تفعيل البحث بالتاريخ", key="use_date_filter_failures")
        if use_date_filter:
            col_date1, col_date2 = st.columns(2)
            with col_date1:
                start_date = st.date_input("من تاريخ:", value=None, key="start_date_failures")
            with col_date2:
                end_date = st.date_input("إلى تاريخ:", value=None, key="end_date_failures")
        else:
            start_date = None
            end_date = None

        view_mode = st.radio("طريقة العرض:", ["جدول", "بطاقات مع الصور"], horizontal=True, key="search_view_mode_failures")

        if st.button("بحث", key="search_btn_failures", type="primary"):
            results = []
            sheets_to_search = []
            if selected_sheet != "جميع الأقسام":
                sheets_to_search = [(selected_sheet, all_sheets[selected_sheet])]
            else:
                for sheet_name in allowed_sections:
                    sheets_to_search.append((sheet_name, all_sheets[sheet_name]))
            
            for sheet_name, df in sheets_to_search:
                df_filtered = df.copy()
                
                if filter_equipment != "الكل" and "المعدة" in df_filtered.columns:
                    df_filtered = df_filtered[df_filtered["المعدة"] == filter_equipment]
                
                if "التاريخ" in df_filtered.columns:
                    df_filtered["التاريخ"] = flexible_date_parser(df_filtered["التاريخ"])
                    df_filtered = df_filtered.dropna(subset=["التاريخ"])
                    if use_date_filter and start_date and end_date:
                        mask = (df_filtered["التاريخ"] >= pd.to_datetime(start_date)) & (df_filtered["التاريخ"] <= pd.to_datetime(end_date) + timedelta(days=1))
                        df_filtered = df_filtered[mask]
                
                if general_search:
                    event_col = "الحدث/العطل"
                    action_col = "الإجراء التصحيحي"
                    mask_general = pd.Series([False] * len(df_filtered))
                    if event_col in df_filtered.columns:
                        mask_general = mask_general | df_filtered[event_col].astype(str).str.contains(general_search, case=False, na=False)
                    if action_col in df_filtered.columns:
                        mask_general = mask_general | df_filtered[action_col].astype(str).str.contains(general_search, case=False, na=False)
                    df_filtered = df_filtered[mask_general]
                
                if technician_search:
                    tech_col = "تم بواسطة"
                    if tech_col in df_filtered.columns:
                        mask_tech = df_filtered[tech_col].astype(str).str.contains(technician_search, case=False, na=False)
                        df_filtered = df_filtered[mask_tech]
                
                if not df_filtered.empty:
                    df_filtered["القسم"] = sheet_name
                    results.append(df_filtered)
            
            if results:
                combined_results = pd.concat(results, ignore_index=True)
                if "رابط الصورة" in combined_results.columns:
                    combined_results["رابط_الصورة_موحد"] = combined_results["رابط الصورة"]
                    combined_results = combined_results.drop(columns=["رابط الصورة"])
                elif "رابط_الصورة" in combined_results.columns:
                    combined_results["رابط_الصورة_موحد"] = combined_results["رابط_الصورة"]
                    combined_results = combined_results.drop(columns=["رابط_الصورة"])
                else:
                    combined_results["رابط_الصورة_موحد"] = ""
                
                st.success(f"تم العثور على {len(combined_results)} نتيجة")
                
                if "التاريخ" in combined_results.columns:
                    combined_results["التاريخ"] = pd.to_datetime(combined_results["التاريخ"], errors='coerce')
                    combined_results = combined_results.dropna(subset=["التاريخ"])
                    combined_results = combined_results.sort_values(by=["المعدة", "التاريخ"], ascending=[True, False])
                
                if view_mode == "جدول":
                    display_cols = [c for c in combined_results.columns if c not in ["رابط_الصورة_موحد"]]
                    st.dataframe(combined_results[display_cols], use_container_width=True, height=500)
                else:
                    for idx, row in combined_results.iterrows():
                        with st.container(border=True):
                            col_img, col_info = st.columns([1, 3])
                            img_url = row.get("رابط_الصورة_موحد", "")
                            with col_img:
                                if img_url and isinstance(img_url, str) and img_url.strip():
                                    try:
                                        st.image(img_url, use_container_width=True)
                                    except:
                                        st.write("🖼️ (تعذر عرض الصورة)")
                                else:
                                    st.write("📄 لا توجد صورة")
                            with col_info:
                                st.markdown(f"**📁 القسم:** {row.get('القسم', '')}")
                                st.markdown(f"**📅 التاريخ:** {row.get('التاريخ', '')}")
                                st.markdown(f"**⚙️ المعدة:** {row.get('المعدة', '')}")
                                st.markdown(f"**⚠️ العطل:** {str(row.get('الحدث/العطل', ''))[:150]}")
                                st.markdown(f"**🔧 الإجراء:** {str(row.get('الإجراء التصحيحي', ''))[:150]}")
                                st.markdown(f"**👨‍🔧 تم بواسطة:** {row.get('تم بواسطة', '')}")
                                if img_url:
                                    st.caption(f"[🔗 رابط الصورة]({img_url})")
                
                export_df = combined_results.drop(columns=["رابط_الصورة_موحد"])
                excel_file = export_filtered_results_to_excel(export_df, "نتائج_البحث")
                st.download_button("📥 تحميل نتائج البحث كملف Excel", excel_file, f"search_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", key='download-excel')
            else:
                st.warning("لا توجد نتائج مطابقة للبحث")
    
    elif search_type == "قطع الغيار":
        spare_df = load_spare_parts()
        if spare_df.empty:
            st.warning("لا توجد بيانات في قطع الغيار")
            return
        df_filtered = spare_df.copy()
        if selected_section_filter != "جميع الأقسام":
            df_filtered = df_filtered[df_filtered["القسم"] == selected_section_filter]
        
        search_term = st.text_input("🔍 كلمة البحث (اسم القطعة، المقاس، القسم...):", key="search_term_spare")
        if search_term:
            search_columns = ["اسم القطعة", "المقاس", "قوه الشد", "مدة التوريد", "القسم", "رابط_الصورة"]
            mask = pd.Series([False] * len(df_filtered))
            for col in search_columns:
                if col in df_filtered.columns:
                    mask = mask | df_filtered[col].astype(str).str.contains(search_term, case=False, na=False)
            df_filtered = df_filtered[mask]
        
        if not df_filtered.empty:
            st.success(f"تم العثور على {len(df_filtered)} قطعة")
            st.dataframe(df_filtered, use_container_width=True)
            excel_file = export_filtered_results_to_excel(df_filtered, "قطع_الغيار")
            st.download_button("📥 تحميل النتائج", excel_file, f"spare_parts_search_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
        else:
            st.warning("لا توجد نتائج")

    else:  # الصيانة الوقائية
        maint_df = load_maintenance_tasks()
        if maint_df.empty:
            st.warning("لا توجد بيانات في الصيانة الوقائية")
            return
        
        equipment_to_section = {}
        for sheet_name in allowed_sections:
            df_sheet = all_sheets[sheet_name]
            if "المعدة" in df_sheet.columns:
                for eq in df_sheet["المعدة"].dropna().unique():
                    equipment_to_section[str(eq).strip()] = sheet_name
        
        df_filtered = maint_df.copy()
        if selected_section_filter != "جميع الأقسام":
            allowed_equipment = [eq for eq, sec in equipment_to_section.items() if sec == selected_section_filter]
            df_filtered = df_filtered[df_filtered["المعدة"].isin(allowed_equipment)]
        
        search_term = st.text_input("🔍 كلمة البحث (المعدة، البند، الملاحظات...):", key="search_term_maintenance")
        if search_term:
            mask = pd.Series([False] * len(df_filtered))
            search_columns = ["المعدة", "اسم_البند", "ملاحظات"]
            for col in search_columns:
                if col in df_filtered.columns:
                    mask = mask | df_filtered[col].astype(str).str.contains(search_term, case=False, na=False)
            df_filtered = df_filtered[mask]
        
        if not df_filtered.empty:
            df_filtered["القسم"] = df_filtered["المعدة"].map(equipment_to_section).fillna("غير محدد")
            if "التاريخ" in df_filtered.columns:
                df_filtered = df_filtered.sort_values(by="التاريخ", ascending=True)
            st.success(f"تم العثور على {len(df_filtered)} مهمة صيانة")
            st.dataframe(df_filtered, use_container_width=True)
            excel_file = export_filtered_results_to_excel(df_filtered, "صيانة_وقائية")
            st.download_button("📥 تحميل النتائج", excel_file, f"maintenance_search_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
        else:
            st.warning("لا توجد نتائج")

# ------------------------------- دوال إدارة المعدات والأقسام -------------------------------
def load_equipment_config():
    if not os.path.exists(EQUIPMENT_CONFIG_FILE):
        default_config = {}
        with open(EQUIPMENT_CONFIG_FILE, "w", encoding="utf-8") as f:
            json.dump(default_config, f, indent=4, ensure_ascii=False)
        return default_config
    try:
        with open(EQUIPMENT_CONFIG_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except:
        return {}

def save_equipment_config(config):
    try:
        with open(EQUIPMENT_CONFIG_FILE, "w", encoding="utf-8") as f:
            json.dump(config, f, indent=4, ensure_ascii=False)
        return True
    except Exception as e:
        st.error(f"خطأ في حفظ تكوين المعدات: {e}")
        return False

def get_equipment_list_from_sheet(df):
    if df is None or df.empty or "المعدة" not in df.columns:
        return []
    equipment = df["المعدة"].dropna().unique()
    equipment = [str(e).strip() for e in equipment if str(e).strip() != ""]
    return sorted(equipment)

def get_available_sections(sheets_edit):
    sections = []
    for sheet_name, df in sheets_edit.items():
        if sheet_name in [APP_CONFIG["SPARE_PARTS_SHEET"], APP_CONFIG["MAINTENANCE_SHEET"]]:
            continue
        if "المعدة" in df.columns and not df["المعدة"].dropna().empty:
            sections.append(sheet_name)
    return sections

def add_equipment_to_sheet_data(sheets_edit, sheet_name, new_equipment):
    if sheet_name not in sheets_edit:
        return False, "القسم غير موجود"
    df = sheets_edit[sheet_name]
    if "المعدة" not in df.columns:
        return False, "عمود 'المعدة' غير موجود في هذا القسم"
    existing = get_equipment_list_from_sheet(df)
    if new_equipment in existing:
        return False, f"الماكينة '{new_equipment}' موجودة بالفعل في هذا القسم"
    new_row = {col: "" for col in df.columns}
    new_row["المعدة"] = new_equipment
    new_row_df = pd.DataFrame([new_row])
    sheets_edit[sheet_name] = pd.concat([df, new_row_df], ignore_index=True)
    return True, f"تم إضافة الماكينة '{new_equipment}' بنجاح إلى قسم {sheet_name}"

def remove_equipment_from_sheet_data(sheets_edit, sheet_name, equipment_name):
    if sheet_name not in sheets_edit:
        return False, "القسم غير موجود"
    df = sheets_edit[sheet_name]
    if "المعدة" not in df.columns:
        return False, "عمود 'المعدة' غير موجود"
    if equipment_name not in get_equipment_list_from_sheet(df):
        return False, "الماكينة غير موجودة"
    new_df = df[df["المعدة"] != equipment_name]
    sheets_edit[sheet_name] = new_df
    return True, f"تم حذف جميع سجلات الماكينة '{equipment_name}'"

def add_new_department(sheets_edit):
    if st.session_state.get("username") == "admin":
        st.subheader("➕ إضافة قسم جديد")
        st.info("سيتم إنشاء قسم جديد (شيت جديد) في ملف Excel لإدارة ماكينات هذا القسم")
        col1, col2 = st.columns(2)
        with col1:
            new_department_name = st.text_input("📝 اسم القسم الجديد:", key="new_department_name", placeholder="مثال: قسم الميكانيكا, قسم الكهرباء, محطة المياه")
            if new_department_name and new_department_name in sheets_edit:
                st.error(f"❌ القسم '{new_department_name}' موجود بالفعل!")
            elif new_department_name:
                st.success(f"✅ اسم القسم '{new_department_name}' متاح")
        with col2:
            st.markdown("#### 📋 إعدادات الأعمدة")
            use_default = st.checkbox("استخدام الأعمدة الافتراضية", value=True, key="use_default_columns")
            if use_default:
                columns_list = APP_CONFIG["DEFAULT_SHEET_COLUMNS"]
                st.info(f"📊 الأعمدة: {', '.join(columns_list)}")
            else:
                columns_text = st.text_area("✏️ الأعمدة (كل عمود في سطر):", value="\n".join(APP_CONFIG["DEFAULT_SHEET_COLUMNS"]), key="custom_columns", height=150)
                columns_list = [col.strip() for col in columns_text.split("\n") if col.strip()]
                if not columns_list:
                    columns_list = APP_CONFIG["DEFAULT_SHEET_COLUMNS"]
        st.markdown("---")
        st.markdown("### 📋 معاينة القسم الجديد")
        preview_df = pd.DataFrame(columns=columns_list)
        st.dataframe(preview_df, use_container_width=True)
        st.caption(f"📊 عدد الأعمدة: {len(columns_list)} | سيتم إنشاء قسم فارغ بهذه الأعمدة")
        if st.button("✅ إنشاء وإضافة القسم الجديد", key="create_department_btn", type="primary", use_container_width=True):
            if not new_department_name:
                st.error("❌ الرجاء إدخال اسم القسم")
                return sheets_edit
            clean_name = re.sub(r'[\\/*?:"<>|]', '_', new_department_name.strip())
            if clean_name != new_department_name:
                st.warning(f"⚠ تم تعديل اسم القسم إلى: {clean_name}")
                new_department_name = clean_name
            if new_department_name in sheets_edit:
                st.error(f"❌ القسم '{new_department_name}' موجود بالفعل!")
                return sheets_edit
            new_df = pd.DataFrame(columns=columns_list)
            sheets_edit[new_department_name] = new_df
            if save_and_push_to_github(sheets_edit, f"إنشاء قسم جديد: {new_department_name}"):
                st.success(f"✅ تم إنشاء القسم '{new_department_name}' بنجاح!")
                st.cache_data.clear()
                st.balloons()
                st.rerun()
            else:
                st.error("❌ فشل حفظ القسم")
                return sheets_edit

        st.markdown("---")
        st.subheader("🗑️ حذف قسم موجود")
        st.warning("⚠️ انتبه: حذف القسم سيؤدي إلى حذف جميع بياناته (ماكينات، أعطال، قطع غيار) نهائياً ولا يمكن استرجاعها.")
        deletable_sections = [name for name in sheets_edit.keys() if name not in [APP_CONFIG["SPARE_PARTS_SHEET"], APP_CONFIG["MAINTENANCE_SHEET"]]]
        if not deletable_sections:
            st.info("لا توجد أقسام قابلة للحذف.")
        else:
            selected_dept = st.selectbox("اختر القسم المراد حذفه:", deletable_sections, key="delete_department_select")
            if selected_dept:
                st.error(f"🔴 أنت على وشك حذف قسم **'{selected_dept}'** نهائياً.")
                confirm = st.text_input("لتأكيد الحذف، اكتب اسم القسم هنا:", key="delete_confirm")
                if confirm == selected_dept:
                    if st.button("🗑️ حذف القسم نهائياً", key="delete_department_btn", type="primary"):
                        spare_df = load_spare_parts()
                        if not spare_df.empty:
                            spare_df = spare_df[spare_df["القسم"] != selected_dept]
                            sheets_edit[APP_CONFIG["SPARE_PARTS_SHEET"]] = spare_df
                            st.info(f"🗑️ تم حذف قطع الغيار التابعة للقسم '{selected_dept}'.")
                        del sheets_edit[selected_dept]
                        if save_and_push_to_github(sheets_edit, f"حذف قسم: {selected_dept}"):
                            log_activity("delete_section", f"تم حذف القسم '{selected_dept}' وقطع الغيار التابعة له")
                            st.success(f"✅ تم حذف القسم '{selected_dept}' بنجاح!")
                            st.cache_data.clear()
                            st.rerun()
                        else:
                            st.error("❌ فشل حفظ التغييرات بعد حذف القسم.")
                elif confirm:
                    st.warning("الاسم غير متطابق. لن يتم حذف القسم.")
    else:
        st.info("🔒 فقط المدير (admin) يمكنه إضافة أو حذف الأقسام.")
    st.markdown("---")
    st.markdown("### 📋 الأقسام الموجودة حالياً:")
    if sheets_edit:
        for dept_name in sheets_edit.keys():
            if dept_name not in [APP_CONFIG["SPARE_PARTS_SHEET"], APP_CONFIG["MAINTENANCE_SHEET"]]:
                st.write(f"- 🏭 {dept_name}")
    else:
        st.info("لا توجد أقسام بعد")
    return sheets_edit

def add_new_machine(sheets_edit, sheet_name):
    st.markdown(f"### 🔧 إضافة ماكينة جديدة في قسم: {sheet_name}")
    df = sheets_edit[sheet_name]
    equipment_list = get_equipment_list_from_sheet(df)
    st.markdown(f"**الماكينات الموجودة حالياً في هذا القسم:**")
    if equipment_list:
        for eq in equipment_list:
            st.markdown(f"- 🔹 {eq}")
    else:
        st.info("لا توجد ماكينات مسجلة بعد في هذا القسم")
    st.markdown("---")
    new_machine = st.text_input("📝 اسم الماكينة الجديدة:", key=f"new_machine_{sheet_name}", placeholder="مثال: محرك رئيسي 1, مضخة مياه, ضاغط هواء")
    if st.button("➕ إضافة ماكينة", key=f"add_machine_{sheet_name}", type="primary"):
        if new_machine:
            success, msg = add_equipment_to_sheet_data(sheets_edit, sheet_name, new_machine)
            if success:
                if save_and_push_to_github(sheets_edit, f"إضافة ماكينة جديدة: {new_machine} في قسم {sheet_name}"):
                    st.success(msg)
                    st.cache_data.clear()
                    st.rerun()
                else:
                    st.error("فشل الحفظ")
            else:
                st.error(msg)
        else:
            st.warning("يرجى إدخال اسم الماكينة")
    return sheets_edit

def manage_machines(sheets_edit, sheet_name):
    st.markdown(f"### 🔧 إدارة الماكينات في قسم: {sheet_name}")
    df = sheets_edit[sheet_name]
    equipment_list = get_equipment_list_from_sheet(df)
    if equipment_list:
        st.markdown("#### 📋 قائمة الماكينات في هذا القسم:")
        for eq in equipment_list:
            st.markdown(f"- 🔹 {eq}")
    else:
        st.info("لا توجد ماكينات مسجلة في هذا القسم بعد")
    st.markdown("---")
    col1, col2 = st.columns(2)
    with col1:
        new_machine = st.text_input("➕ اسم الماكينة الجديدة:", key=f"new_machine_{sheet_name}")
        if st.button("➕ إضافة ماكينة", key=f"add_machine_{sheet_name}"):
            if new_machine:
                success, msg = add_equipment_to_sheet_data(sheets_edit, sheet_name, new_machine)
                if success:
                    if save_and_push_to_github(sheets_edit, f"إضافة ماكينة: {new_machine} في قسم {sheet_name}"):
                        st.success(msg)
                        st.cache_data.clear()
                        st.rerun()
                    else:
                        st.error("فشل الحفظ")
                else:
                    st.error(msg)
            else:
                st.warning("يرجى إدخال اسم الماكينة")
    with col2:
        if equipment_list:
            st.markdown("#### 🗑️ حذف ماكينة")
            if st.session_state.get("username") == "admin":
                machine_to_delete = st.selectbox("اختر الماكينة للحذف:", equipment_list, key=f"delete_machine_{sheet_name}")
                st.warning("⚠️ تحذير: حذف الماكينة سيؤدي إلى حذف جميع سجلات الأعطال المرتبطة بها نهائياً!")
                if st.button("🗑️ حذف الماكينة نهائياً", key=f"delete_machine_btn_{sheet_name}"):
                    success, msg = remove_equipment_from_sheet_data(sheets_edit, sheet_name, machine_to_delete)
                    if success:
                        if save_and_push_to_github(sheets_edit, f"حذف ماكينة: {machine_to_delete} من قسم {sheet_name}"):
                            st.success(msg)
                            st.cache_data.clear()
                            st.rerun()
                        else:
                            st.error("فشل الحفظ")
                    else:
                        st.error(msg)
            else:
                st.info("🔒 حذف الماكينات مقيد بصلاحيات المدير (admin). تواصل مع مدير النظام.")
        else:
            st.info("لا توجد ماكينات لحذفها")

def add_new_event(sheets_edit, sheet_name):
    st.markdown(f"### 📝 إضافة حدث عطل جديد في قسم: {sheet_name}")
    df = sheets_edit[sheet_name]
    equipment_list = get_equipment_list_from_sheet(df)
    if not equipment_list:
        st.warning("⚠ لا توجد ماكينات مسجلة في هذا القسم. يرجى إضافة ماكينة أولاً من تبويب 'إدارة الماكينات'")
        return sheets_edit
    if "selected_equipment_temp" not in st.session_state:
        st.session_state.selected_equipment_temp = equipment_list[0] if equipment_list else ""
    selected_equipment = st.selectbox("🔧 اختر الماكينة:", equipment_list, index=equipment_list.index(st.session_state.selected_equipment_temp) if st.session_state.selected_equipment_temp in equipment_list else 0, key="equipment_select")
    if selected_equipment != st.session_state.selected_equipment_temp:
        st.session_state.selected_equipment_temp = selected_equipment
        st.rerun()
    spare_parts_list = get_spare_parts_for_section(sheet_name)
    with st.form(key="add_event_form"):
        col1, col2 = st.columns(2)
        with col1:
            event_date = st.date_input("📅 التاريخ:", value=datetime.now())
            repair_duration = st.number_input("⏱️ مدة الإصلاح (ساعات):", min_value=0.0, step=0.5, format="%.1f")
            event_desc = st.text_area("📝 الحدث/العطل:", height=100)
            fault_type = st.selectbox("🏷️ نوع العطل:", ["", "ميكانيكي", "كهربائي", "إلكتروني", "هيدروليكي", "هوائي", "هيكلي", "آخر"])
            uploaded_image = st.file_uploader("🖼️ رفع صورة (اختياري):", type=APP_CONFIG["ALLOWED_IMAGE_TYPES"])
        with col2:
            correction_desc = st.text_area("🔧 الإجراء التصحيحي:", height=100)
            servised_by = st.text_input("👨‍🔧 تم بواسطة:")
            technician_rating = st.select_slider("⭐ قدرة الفني (حل/تفكير/مبادرة/قرار):", options=[1, 2, 3, 4, 5], value=3)
            safety_compliance = st.selectbox("🛡️ الالتزام بتعليمات السلامة:", ["", "ملتزم بالكامل", "ملتزم جزئياً", "غير ملتزم", "غير مطبق"])
            st.markdown("---")
            st.markdown("**🔩 قطع الغيار المستخدمة**")
            if spare_parts_list:
                part_names = [f"{name} (الرصيد: {qty})" for name, qty in spare_parts_list]
                selected_part_display = st.selectbox("اختر قطعة:", [""] + part_names, key="spare_part_select")
                if selected_part_display:
                    part_name = selected_part_display.split(" (")[0]
                    current_qty = next((qty for name, qty in spare_parts_list if name == part_name), 0)
                    st.caption(f"الرصيد الحالي: {current_qty}")
                    consume_qty = st.number_input("الكمية المستخدمة:", min_value=1, max_value=max(1, current_qty), value=1, step=1, key="consume_qty")
                    if consume_qty > current_qty:
                        st.error(f"⚠️ الرصيد غير كافٍ (الموجود {current_qty})")
                    else:
                        st.success(f"سيتم خصم {consume_qty} من الرصيد")
                else:
                    part_name = ""
                    consume_qty = 0
            else:
                st.info("لا توجد قطع غيار مسجلة لهذا القسم. يمكنك إضافتها من تبويب 'قطع الغيار'.")
                part_name = ""
                consume_qty = 0
        submitted = st.form_submit_button("✅ إضافة الحدث", type="primary")
        if submitted:
            spare_part_used = ""
            warning_msg = ""
            if part_name and consume_qty > 0:
                success, msg, new_qty = consume_spare_part(part_name, consume_qty)
                if success:
                    spare_part_used = f"{part_name} (كمية {consume_qty})"
                    critical_parts = get_critical_spare_parts()
                    for cp in critical_parts:
                        if cp["اسم القطعة"] == part_name:
                            warning_msg = f"⚠️ **تحذير:** القطعة '{part_name}' ضرورية وأصبح رصيدها {new_qty} (أقل من 1). يرجى إعادة التوريد."
                            break
                else:
                    st.error(msg)
                    return sheets_edit
            image_url = None
            if uploaded_image is not None:
                event_id = str(uuid.uuid4())[:8]
                image_url = upload_image_to_github(uploaded_image, "event", event_id)
                if image_url:
                    st.success("✅ تم رفع الصورة بنجاح!")
                else:
                    st.warning("⚠️ فشل رفع الصورة، سيتم حفظ الحدث بدون صورة")
            new_row = {
                "مده الاصلاح": repair_duration if repair_duration > 0 else "",
                "التاريخ": event_date.strftime("%Y-%m-%d"),
                "المعدة": selected_equipment,
                "الحدث/العطل": event_desc,
                "الإجراء التصحيحي": correction_desc,
                "تم بواسطة": servised_by,
                "قطع غيار مستخدمة": spare_part_used,
                "نوع العطل": fault_type if fault_type else "",
                "قدرة الفني (حل/تفكير/مبادرة/قرار)": technician_rating,
                "الالتزام بتعليمات السلامة": safety_compliance if safety_compliance else "",
                "رابط الصورة": image_url or ""
            }
            for col in df.columns:
                if col not in new_row:
                    new_row[col] = ""
            new_row_df = pd.DataFrame([new_row])
            df_new = pd.concat([df, new_row_df], ignore_index=True)
            sheets_edit[sheet_name] = df_new
            if "temp_spare_parts_df" in st.session_state:
                sheets_edit[APP_CONFIG["SPARE_PARTS_SHEET"]] = st.session_state.temp_spare_parts_df
                del st.session_state.temp_spare_parts_df
            if save_and_push_to_github(sheets_edit, f"إضافة حدث عطل مع استخدام قطعة {part_name}"):
                st.cache_data.clear()
                log_activity("add_event", f"تم إضافة عطل: {event_desc[:50]} للماكينة {selected_equipment}")
                st.success("✅ تم إضافة الحدث بنجاح ورفعه إلى GitHub!")
                if warning_msg:
                    st.warning(warning_msg)
                st.rerun()
            else:
                st.error("❌ فشل الحفظ")
    return sheets_edit

# ------------------------------- دوال مساعدة للصيانة الوقائية -------------------------------
def execute_maintenance_with_date(sheets_edit, equipment_name, task_name, execution_date, performed_by, used_spare_part="", used_quantity=1, image_url=None):
    df = sheets_edit.get(APP_CONFIG["MAINTENANCE_SHEET"])
    if df is None:
        return False, "لا توجد مهام صيانة"
    mask = (df["المعدة"] == equipment_name) & (df["اسم_البند"] == task_name)
    if not mask.any():
        return False, f"المهمة '{task_name}' غير موجودة للمعدة '{equipment_name}'"
    idx = df[mask].index[0]
    period_days = df.loc[idx, "الفترة_بالأيام"]
    df.loc[idx, "آخر_تنفيذ"] = pd.to_datetime(execution_date)
    next_date = execution_date + timedelta(days=period_days)
    df.loc[idx, "التاريخ_التالي"] = next_date
    old_notes = df.loc[idx, "ملاحظات"] if pd.notna(df.loc[idx, "ملاحظات"]) else ""
    new_entry = f"{execution_date.strftime('%Y-%m-%d')} | تم بواسطة: {performed_by}"
    warning_msg = ""
    if used_spare_part and used_quantity > 0:
        success, msg, new_qty = consume_spare_part(used_spare_part, used_quantity)
        if not success:
            return False, f"فشل خصم قطعة الغيار: {msg}"
        new_entry += f" | استخدمت {used_spare_part} كمية {used_quantity} - {msg}"
        critical_parts = get_critical_spare_parts()
        for cp in critical_parts:
            if cp["اسم القطعة"] == used_spare_part:
                warning_msg = f"⚠️ **تحذير:** القطعة '{used_spare_part}' ضرورية وأصبح رصيدها {new_qty} (أقل من 1). يرجى إعادة التوريد."
                break
    if image_url:
        new_entry += f" | صورة: {image_url}"
    df.loc[idx, "ملاحظات"] = (old_notes + "\n" + new_entry).strip()
    sheets_edit[APP_CONFIG["MAINTENANCE_SHEET"]] = df
    log_activity("execute_maintenance", f"تم تنفيذ صيانة '{task_name}' للماكينة {equipment_name} بواسطة {performed_by}")
    result_msg = f"تم تنفيذ الصيانة '{task_name}' بتاريخ {execution_date.strftime('%Y-%m-%d')} بواسطة {performed_by}. التاريخ التالي: {next_date.strftime('%Y-%m-%d')}" + (f" {warning_msg}" if warning_msg else "")
    return True, result_msg

def add_maintenance_as_event(sheets_edit, equipment_name, task_name, execution_date, performed_by, used_spare_part="", used_quantity=1, image_url=None):
    target_sheet = None
    target_df = None
    for sheet_name, df in sheets_edit.items():
        if sheet_name not in [APP_CONFIG["SPARE_PARTS_SHEET"], APP_CONFIG["MAINTENANCE_SHEET"]]:
            if equipment_name in get_equipment_list_from_sheet(df):
                target_sheet = sheet_name
                target_df = df
                break
    if target_sheet is None:
        return False, f"لم يتم العثور على قسم يحتوي على المعدة '{equipment_name}'"
    spare_part_used = f"{used_spare_part} (كمية {used_quantity})" if used_spare_part else ""
    new_row = {
        "مده الاصلاح": 0, "التاريخ": execution_date.strftime("%Y-%m-%d"), "المعدة": equipment_name,
        "الحدث/العطل": f"صيانة وقائية: {task_name}", "الإجراء التصحيحي": f"تم تنفيذ الصيانة الدورية '{task_name}' بواسطة {performed_by}",
        "تم بواسطة": performed_by, "قطع غيار مستخدمة": spare_part_used, "نوع العطل": "صيانة وقائية",
        "قدرة الفني (حل/تفكير/مبادرة/قرار)": 5, "الالتزام بتعليمات السلامة": "ملتزم بالكامل", "رابط الصورة": image_url or ""
    }
    for col in target_df.columns:
        if col not in new_row:
            new_row[col] = ""
    new_row_df = pd.DataFrame([new_row])
    sheets_edit[target_sheet] = pd.concat([target_df, new_row_df], ignore_index=True)
    return True, f"تم تسجيل الصيانة كحدث في قسم '{target_sheet}' بواسطة {performed_by}"

# ------------------------------- تبويب قطع الغيار والصيانة الوقائية -------------------------------
def manage_spare_parts_tab(sheets_edit):
    st.header("📦 إدارة قطع الغيار")
    st.info("هنا يمكنك إضافة وتعديل قطع الغيار المرتبطة بكل قسم. القطع المضافة للقسم 'عام' تكون متاحة لجميع الأقسام.")
    username = st.session_state.get("username")
    all_sheets = load_all_sheets()
    real_sections = get_allowed_sections(all_sheets, username, "view")
    allowed_sections = real_sections.copy()
    if real_sections or username == "admin":
        if APP_CONFIG["GENERAL_SECTION"] not in allowed_sections:
            allowed_sections = [APP_CONFIG["GENERAL_SECTION"]] + allowed_sections
    else:
        st.warning("⚠️ لا توجد أقسام مسموح لك بالوصول إليها.")
        return sheets_edit
    selected_section = st.selectbox("🏭 اختر القسم:", allowed_sections, key="spare_section")
    spare_df = load_spare_parts()
    view_mode = st.radio("طريقة العرض:", ["جدول", "بطاقات مع الصور"], horizontal=True, key="spare_view_mode")
    st.subheader("📋 قائمة قطع الغيار")
    filtered_df = spare_df[spare_df["القسم"] == selected_section].copy()
    filtered_df.reset_index(drop=False, inplace=True)
    filtered_df.rename(columns={'index': 'original_index'}, inplace=True)
    filtered_df["id"] = filtered_df.index
    if filtered_df.empty:
        st.info(f"لا توجد قطع غيار مسجلة للقسم '{selected_section}'.")
    else:
        part_name_filter = st.text_input("فلتر حسب اسم القطعة:", placeholder="اكتب جزءاً من الاسم...", key="spare_name_filter")
        if part_name_filter:
            filtered_df = filtered_df[filtered_df["اسم القطعة"].str.contains(part_name_filter, case=False, na=False)]
        if view_mode == "جدول":
            display_cols = [c for c in filtered_df.columns if c not in ["original_index", "id", "رابط_الصورة"]]
            st.dataframe(filtered_df[display_cols], use_container_width=True)
            st.markdown("#### 🛠️ تعديل أو حذف قطعة")
            part_options = filtered_df["اسم القطعة"].tolist()
            selected_part_name = st.selectbox("اختر القطعة:", part_options, key="edit_part_name_select")
            if selected_part_name:
                part_row = filtered_df[filtered_df["اسم القطعة"] == selected_part_name].iloc[0]
                with st.expander(f"✏️ تعديل قطعة: {selected_part_name}"):
                    new_name = st.text_input("اسم القطعة", value=part_row["اسم القطعة"], key="edit_name")
                    new_size = st.text_input("المقاس", value=part_row["المقاس"], key="edit_size")
                    new_qty = st.number_input("الرصيد", value=int(part_row["الرصيد الموجود"]), step=1, key="edit_qty")
                    new_lead = st.text_input("مدة التوريد", value=part_row["مدة التوريد"], key="edit_lead")
                    new_critical = st.checkbox("قطعة ضرورية", value=(part_row["ضرورية"] == "نعم"), key="edit_critical")
                    new_threshold = st.number_input("حد الإنذار", value=int(part_row.get("حد_الإنذار", 1)), step=1, key="edit_threshold")
                    if st.button("💾 حفظ التغييرات", key="save_edit_part"):
                        original_idx = part_row["original_index"]
                        spare_df.loc[original_idx, "اسم القطعة"] = new_name
                        spare_df.loc[original_idx, "المقاس"] = new_size
                        spare_df.loc[original_idx, "الرصيد الموجود"] = new_qty
                        spare_df.loc[original_idx, "مدة التوريد"] = new_lead
                        spare_df.loc[original_idx, "ضرورية"] = "نعم" if new_critical else "لا"
                        spare_df.loc[original_idx, "حد_الإنذار"] = new_threshold
                        sheets_edit[APP_CONFIG["SPARE_PARTS_SHEET"]] = spare_df
                        if save_and_push_to_github(sheets_edit, f"تعديل قطعة: {selected_part_name}"):
                            log_activity("add_spare_part", f"تم تعديل قطعة غيار '{selected_part_name}' للقسم {selected_section}")
                            st.success("تم التعديل")
                            st.rerun()
                if st.button("🗑️ حذف هذه القطعة", key="delete_part_btn"):
                    original_idx = part_row["original_index"]
                    spare_df = spare_df.drop(index=original_idx)
                    sheets_edit[APP_CONFIG["SPARE_PARTS_SHEET"]] = spare_df
                    if save_and_push_to_github(sheets_edit, f"حذف قطعة: {selected_part_name}"):
                        st.success("تم الحذف")
                        st.rerun()
        else:
            cols_per_row = 2
            for i in range(0, len(filtered_df), cols_per_row):
                row_cols = st.columns(cols_per_row)
                for j, col in enumerate(row_cols):
                    idx = i + j
                    if idx < len(filtered_df):
                        row = filtered_df.iloc[idx]
                        with col:
                            with st.container(border=True):
                                img_url = row.get("رابط_الصورة", "")
                                if img_url and isinstance(img_url, str) and img_url.strip():
                                    try:
                                        st.image(img_url, use_container_width=True)
                                    except:
                                        st.write("🖼️ (تعذر عرض الصورة)")
                                else:
                                    st.write("📦 لا توجد صورة")
                                st.markdown(f"**🔩 {row['اسم القطعة']}**")
                                st.markdown(f"**المقاس:** {row['المقاس']}")
                                st.markdown(f"**الرصيد:** {row['الرصيد الموجود']}")
                                st.markdown(f"**ضرورية:** {row['ضرورية']}")
                                if row.get('مدة التوريد'):
                                    st.markdown(f"**مدة التوريد:** {row['مدة التوريد']}")
                                col_btn1, col_btn2 = st.columns(2)
                                with col_btn1:
                                    if st.button("✏️ تعديل", key=f"edit_card_{row['id']}"):
                                        st.session_state[f"edit_mode_{row['id']}"] = True
                                with col_btn2:
                                    if st.button("🗑️ حذف", key=f"delete_card_{row['id']}"):
                                        original_idx = row["original_index"]
                                        spare_df = spare_df.drop(index=original_idx)
                                        sheets_edit[APP_CONFIG["SPARE_PARTS_SHEET"]] = spare_df
                                        if save_and_push_to_github(sheets_edit, f"حذف قطعة: {row['اسم القطعة']}"):
                                            st.success("تم الحذف")
                                            st.rerun()
                                if st.session_state.get(f"edit_mode_{row['id']}", False):
                                    with st.form(key=f"edit_form_{row['id']}"):
                                        new_name = st.text_input("اسم القطعة", value=row['اسم القطعة'])
                                        new_size = st.text_input("المقاس", value=row['المقاس'])
                                        new_qty = st.number_input("الرصيد", value=int(row['الرصيد الموجود']))
                                        new_lead = st.text_input("مدة التوريد", value=row['مدة التوريد'])
                                        new_critical = st.checkbox("ضرورية", value=(row['ضرورية'] == "نعم"))
                                        if st.form_submit_button("💾 حفظ"):
                                            original_idx = row["original_index"]
                                            spare_df.loc[original_idx, "اسم القطعة"] = new_name
                                            spare_df.loc[original_idx, "المقاس"] = new_size
                                            spare_df.loc[original_idx, "الرصيد الموجود"] = new_qty
                                            spare_df.loc[original_idx, "مدة التوريد"] = new_lead
                                            spare_df.loc[original_idx, "ضرورية"] = "نعم" if new_critical else "لا"
                                            sheets_edit[APP_CONFIG["SPARE_PARTS_SHEET"]] = spare_df
                                            if save_and_push_to_github(sheets_edit, f"تعديل قطعة: {row['اسم القطعة']}"):
                                                st.success("تم التعديل")
                                                del st.session_state[f"edit_mode_{row['id']}"]
                                                st.rerun()
                                            else:
                                                st.error("فشل الحفظ")
        st.subheader("➕ إضافة قطعة غيار جديدة")
    with st.form(key="add_spare_part_form"):
        col1, col2 = st.columns(2)
        with col1:
            part_name = st.text_input("🔩 اسم القطعة:")
            part_size = st.text_input("📏 المقاس:")
            part_image = st.file_uploader("🖼️ صورة القطعة (اختياري):", type=APP_CONFIG["ALLOWED_IMAGE_TYPES"], key="spare_part_image")
        with col2:
            initial_qty = st.number_input("📦 الرصيد الموجود:", min_value=0, step=1, value=0)
            lead_time = st.text_input("⏱️ مدة التوريد (أيام أو نص):")
            is_critical = st.checkbox("⚠️ قطعة ضرورية (ستظهر في الإشعارات حال نقص الرصيد)")
            critical_threshold = st.number_input("⚠️ حد الإنذار (عند نقص الرصيد عن هذا الرقم):", min_value=1, step=1, value=1, help="مثال: 2 يعني إذا أصبح الرصيد 1 أو أقل تصبح حرجة")
        submitted = st.form_submit_button("✅ إضافة قطعة")
        if submitted:
            if not part_name:
                st.error("❌ الرجاء إدخال اسم القطعة")
            else:
                existing = spare_df[(spare_df["اسم القطعة"] == part_name) & (spare_df["القسم"] == selected_section)]
                if not existing.empty:
                    st.error(f"❌ القطعة '{part_name}' موجودة بالفعل للقسم '{selected_section}'")
                else:
                    image_url = None
                    if part_image is not None:
                        part_id = str(uuid.uuid4())[:8]
                        image_url = upload_image_to_github(part_image, "spare_part", part_id)
                        if image_url:
                            st.success("✅ تم رفع الصورة")
                        else:
                            st.warning("⚠️ فشل رفع الصورة")
                    new_row = pd.DataFrame([{
                        "اسم القطعة": part_name,
                        "المقاس": part_size,
                        "الرصيد الموجود": initial_qty,
                        "مدة التوريد": lead_time,
                        "ضرورية": "نعم" if is_critical else "لا",
                        "القسم": selected_section,
                        "رابط_الصورة": image_url or "",
                        "حد_الإنذار": critical_threshold
                    }])
                    new_spare_df = pd.concat([spare_df, new_row], ignore_index=True)
                    sheets_edit[APP_CONFIG["SPARE_PARTS_SHEET"]] = new_spare_df
                    if save_and_push_to_github(sheets_edit, f"إضافة قطعة غيار: {part_name} للقسم {selected_section}"):
                        log_activity("add_spare_part", f"تم إضافة قطعة غيار '{part_name}' للقسم {selected_section} (الرصيد: {initial_qty})")
                        st.success("✅ تمت إضافة قطعة الغيار")
                        st.rerun()
                    else:
                        st.error("❌ فشل الحفظ")
    return sheets_edit

def preventive_maintenance_tab(sheets_edit):
    st.header("🛠 الصيانة الوقائية")
    st.info("إدارة بنود الصيانة الدورية. يتم حفظ البيانات تلقائياً في ملف Excel.")
    username = st.session_state.get("username")
    all_sheets = load_all_sheets()
    allowed_sections = get_allowed_sections(all_sheets, username, "view")
    if not allowed_sections:
        st.warning("⚠️ لا توجد أقسام مسموح لك بالوصول إليها.")
        return sheets_edit
    selected_section = st.selectbox("🏭 اختر القسم:", allowed_sections, key="pm_section")
    df_section = sheets_edit[selected_section]
    equipment_list = get_equipment_list_from_sheet(df_section)
    if not equipment_list:
        st.warning(f"⚠️ لا توجد ماكينات في قسم '{selected_section}'.")
        return sheets_edit
    selected_equipment = st.selectbox("🔧 اختر المعدة:", equipment_list, key="pm_equipment")
    if APP_CONFIG["MAINTENANCE_SHEET"] in sheets_edit:
        tasks_df = sheets_edit[APP_CONFIG["MAINTENANCE_SHEET"]].copy()
    else:
        tasks_df = load_maintenance_tasks()
        sheets_edit[APP_CONFIG["MAINTENANCE_SHEET"]] = tasks_df
    tasks_df = tasks_df[tasks_df["المعدة"] == selected_equipment].copy()
    st.subheader(f"📋 بنود الصيانة لـ {selected_equipment}")
    if tasks_df.empty:
        st.info("لا توجد بنود صيانة مسجلة لهذه المعدة. يمكنك إضافة بند جديد أدناه.")
    else:
        view_mode = st.radio("طريقة العرض:", ["جدول", "بطاقات مع الصور"], horizontal=True, key="maintenance_view_mode")
        today = datetime.now().date()
        tasks_display = tasks_df.copy()
        tasks_display.reset_index(drop=False, inplace=True)
        tasks_display.rename(columns={"index": "original_index"}, inplace=True)
        def days_remaining(row):
            if pd.isna(row["التاريخ_التالي"]):
                return "غير محدد"
            return (row["التاريخ_التالي"].date() - today).days
        tasks_display["الأيام_المتبقية"] = tasks_display.apply(days_remaining, axis=1)
        tasks_display["الحالة"] = tasks_display["الأيام_المتبقية"].apply(lambda x: "🔴 متأخرة" if (isinstance(x, int) and x < 0) else ("🟡 قادمة" if (isinstance(x, int) and x <= 3) else "🟢 جيدة"))
        tasks_display["عدد_الصيانات"] = tasks_display["آخر_تنفيذ"].apply(lambda x: 1 if pd.notna(x) else 0)
        if view_mode == "جدول":
            cols_to_show = ["نوع_الصيانة", "اسم_البند", "الفترة_بالأيام", "آخر_تنفيذ", "التاريخ_التالي", "الأيام_المتبقية", "الحالة", "عدد_الصيانات", "ملاحظات"]
            st.dataframe(tasks_display[cols_to_show], use_container_width=True)
            st.markdown("#### 🛠️ تعديل أو حذف بند صيانة")
            task_options = tasks_display["اسم_البند"].tolist()
            selected_task_name = st.selectbox("اختر البند:", task_options, key="edit_task_select")
            if selected_task_name:
                task_row = tasks_display[tasks_display["اسم_البند"] == selected_task_name].iloc[0]
                original_idx = task_row["original_index"]
                with st.expander(f"✏️ تعديل بند: {selected_task_name}"):
                    new_name = st.text_input("اسم البند", value=task_row["اسم_البند"], key="edit_task_name")
                    new_period_hours = st.number_input("عدد الساعات بين الصيانة", min_value=1, value=int(task_row["الفترة_بالأيام"]*24), key="edit_period_hours")
                    new_notes = st.text_area("ملاحظات", value=task_row["ملاحظات"] if pd.notna(task_row["ملاحظات"]) else "", key="edit_task_notes")
                    if st.button("💾 حفظ التغييرات", key="save_task_edit"):
                        new_period_days = new_period_hours / 24.0
                        main_df = sheets_edit[APP_CONFIG["MAINTENANCE_SHEET"]]
                        main_df.loc[original_idx, "اسم_البند"] = new_name
                        main_df.loc[original_idx, "الفترة_بالأيام"] = new_period_days
                        main_df.loc[original_idx, "نوع_الصيانة"] = f"{new_period_hours} ساعة"
                        main_df.loc[original_idx, "ملاحظات"] = new_notes
                        last_exec = main_df.loc[original_idx, "آخر_تنفيذ"]
                        if pd.notna(last_exec) and hasattr(last_exec, 'date'):
                            main_df.loc[original_idx, "التاريخ_التالي"] = last_exec + timedelta(days=new_period_days)
                        else:
                            main_df.loc[original_idx, "التاريخ_التالي"] = datetime.now().date() + timedelta(days=new_period_days)
                        sheets_edit[APP_CONFIG["MAINTENANCE_SHEET"]] = main_df
                        if save_and_push_to_github(sheets_edit, f"تعديل بند صيانة: {selected_task_name}"):
                            st.success("تم التعديل")
                            st.rerun()
                if st.button("🗑️ حذف هذا البند", key="delete_task_btn"):
                    main_df = sheets_edit[APP_CONFIG["MAINTENANCE_SHEET"]]
                    main_df = main_df.drop(index=original_idx)
                    sheets_edit[APP_CONFIG["MAINTENANCE_SHEET"]] = main_df
                    if save_and_push_to_github(sheets_edit, f"حذف بند صيانة: {selected_task_name}"):
                        st.success("تم الحذف")
                        st.rerun()
        else:
            cols_per_row = 2
            for i in range(0, len(tasks_display), cols_per_row):
                row_cols = st.columns(cols_per_row)
                for j, col in enumerate(row_cols):
                    idx = i + j
                    if idx < len(tasks_display):
                        row = tasks_display.iloc[idx]
                        original_idx = row["original_index"]
                        with col:
                            with st.container(border=True):
                                img_url = row.get("رابط_الصورة", "")
                                if img_url and isinstance(img_url, str) and img_url.strip():
                                    try:
                                        st.image(img_url, use_container_width=True)
                                    except:
                                        st.write("🖼️ (تعذر عرض الصورة)")
                                else:
                                    st.write("🔧 لا توجد صورة")
                                st.markdown(f"**{row['اسم_البند']}**")
                                st.markdown(f"**نوع الصيانة:** {row['نوع_الصيانة']}")
                                st.markdown(f"**الفترة:** {row['الفترة_بالأيام']:.2f} يوم")
                                last_exec_val = row['آخر_تنفيذ']
                                if pd.notna(last_exec_val) and hasattr(last_exec_val, 'strftime'):
                                    last_exec_str = last_exec_val.strftime('%Y-%m-%d')
                                else:
                                    last_exec_str = 'لم تنفذ بعد'
                                st.markdown(f"**آخر تنفيذ:** {last_exec_str}")
                                next_date_val = row['التاريخ_التالي']
                                if pd.notna(next_date_val) and hasattr(next_date_val, 'strftime'):
                                    next_date_str = next_date_val.strftime('%Y-%m-%d')
                                else:
                                    next_date_str = 'غير محدد'
                                st.markdown(f"**التاريخ التالي:** {next_date_str}")
                                st.markdown(f"**الحالة:** {row['الحالة']}")
                                col_btn1, col_btn2 = st.columns(2)
                                with col_btn1:
                                    if st.button("✏️ تعديل", key=f"edit_task_card_{original_idx}"):
                                        st.session_state[f"edit_task_mode_{original_idx}"] = True
                                with col_btn2:
                                    if st.button("🗑️ حذف", key=f"delete_task_card_{original_idx}"):
                                        main_df = sheets_edit[APP_CONFIG["MAINTENANCE_SHEET"]]
                                        main_df = main_df.drop(index=original_idx)
                                        sheets_edit[APP_CONFIG["MAINTENANCE_SHEET"]] = main_df
                                        if save_and_push_to_github(sheets_edit, f"حذف بند صيانة: {row['اسم_البند']}"):
                                            st.success("تم الحذف")
                                            st.rerun()
                                if st.session_state.get(f"edit_task_mode_{original_idx}", False):
                                    with st.form(key=f"edit_task_form_{original_idx}"):
                                        new_name = st.text_input("اسم البند", value=row['اسم_البند'])
                                        new_period_hours = st.number_input("عدد الساعات", min_value=1, value=int(row['الفترة_بالأيام']*24))
                                        new_notes = st.text_area("ملاحظات", value=row['ملاحظات'] if pd.notna(row['ملاحظات']) else "")
                                        if st.form_submit_button("💾 حفظ"):
                                            new_period_days = new_period_hours / 24.0
                                            main_df = sheets_edit[APP_CONFIG["MAINTENANCE_SHEET"]]
                                            main_df.loc[original_idx, "اسم_البند"] = new_name
                                            main_df.loc[original_idx, "الفترة_بالأيام"] = new_period_days
                                            main_df.loc[original_idx, "نوع_الصيانة"] = f"{new_period_hours} ساعة"
                                            main_df.loc[original_idx, "ملاحظات"] = new_notes
                                            last_exec = main_df.loc[original_idx, "آخر_تنفيذ"]
                                            if pd.notna(last_exec) and hasattr(last_exec, 'date'):
                                                main_df.loc[original_idx, "التاريخ_التالي"] = last_exec + timedelta(days=new_period_days)
                                            else:
                                                main_df.loc[original_idx, "التاريخ_التالي"] = datetime.now().date() + timedelta(days=new_period_days)
                                            sheets_edit[APP_CONFIG["MAINTENANCE_SHEET"]] = main_df
                                            if save_and_push_to_github(sheets_edit, f"تعديل بند صيانة: {row['اسم_البند']}"):
                                                st.success("تم التعديل")
                                                del st.session_state[f"edit_task_mode_{original_idx}"]
                                                st.rerun()
                                            else:
                                                st.error("فشل الحفظ")
        st.markdown("---")
        st.subheader("✅ تنفيذ صيانة")
        task_options = tasks_df["اسم_البند"].tolist()
        if task_options:
            selected_task = st.selectbox("اختر البند المنفذ:", task_options, key="execute_task_select")
            if selected_task:
                execution_date = st.date_input("📅 تاريخ التنفيذ:", value=datetime.now().date(), key="execution_date_input")
                performed_by = st.text_input("👨‍🔧 تم بواسطة:", key="maintenance_performed_by", placeholder="اسم الشخص الذي نفذ الصيانة")
                spare_parts_list = get_spare_parts_for_section(selected_section)
                st.markdown("**🔩 استهلاك قطع غيار (اختياري)**")
                part_name = ""
                consume_qty = 0
                use_part = True
                if spare_parts_list:
                    part_names = [""] + [f"{name} (الرصيد: {qty})" for name, qty in spare_parts_list]
                    selected_part_display = st.selectbox("اختر قطعة:", part_names, key="pm_spare_part")
                    if selected_part_display:
                        part_name = selected_part_display.split(" (")[0]
                        current_qty = next((qty for name, qty in spare_parts_list if name == part_name), 0)
                        st.caption(f"الرصيد الحالي: {current_qty}")
                        consume_qty = st.number_input("الكمية المستخدمة:", min_value=1, max_value=max(1, current_qty), value=1, step=1, key="pm_consume_qty")
                        if consume_qty > current_qty:
                            st.error(f"⚠️ الرصيد غير كافٍ")
                            use_part = False
                else:
                    st.info("لا توجد قطع غيار مسجلة لهذا القسم")
                execution_image = st.file_uploader("🖼️ رفع صورة للصيانة المنفذة (اختياري):", type=APP_CONFIG["ALLOWED_IMAGE_TYPES"], key="maintenance_execution_image")
                link_to_event = st.checkbox("🔗 تسجيل هذه الصيانة كحدث عطل", value=False)
                if st.button("✅ تم تنفيذ الصيانة", type="primary"):
                    if not performed_by:
                        st.error("❌ الرجاء إدخال اسم المنفذ")
                    elif not use_part:
                        st.error("لا يمكن التنفيذ بسبب نقص الرصيد")
                    else:
                        image_url = None
                        if execution_image:
                            maint_id = str(uuid.uuid4())[:8]
                            image_url = upload_image_to_github(execution_image, "maintenance_execution", maint_id)
                        success, msg = execute_maintenance_with_date(sheets_edit, selected_equipment, selected_task, execution_date, performed_by, part_name, consume_qty, image_url)
                        if success:
                            if link_to_event:
                                event_success, event_msg = add_maintenance_as_event(sheets_edit, selected_equipment, selected_task, execution_date, performed_by, part_name, consume_qty, image_url)
                                if event_success:
                                    st.success(f"✅ {msg} وتم تسجيله كحدث عطل")
                                else:
                                    st.warning(f"✅ {msg} لكن فشل تسجيل الحدث: {event_msg}")
                            else:
                                st.success(msg)
                            if "temp_spare_parts_df" in st.session_state:
                                sheets_edit[APP_CONFIG["SPARE_PARTS_SHEET"]] = st.session_state.temp_spare_parts_df
                                del st.session_state.temp_spare_parts_df
                            if save_and_push_to_github(sheets_edit, f"تنفيذ صيانة '{selected_task}' لـ {selected_equipment} بواسطة {performed_by}"):
                                st.rerun()
                        else:
                            st.error(msg)
        else:
            st.info("لا توجد بنود صيانة لتنفيذها")
    st.markdown("---")
    st.subheader("➕ إضافة بند صيانة جديد")
    use_custom_start = st.checkbox("📅 تحديد تاريخ بدء الصيانة", key="use_custom_start_checkbox")
    start_date = None
    if use_custom_start:
        start_date = st.date_input("تاريخ البدء:", value=datetime.now().date(), key="maintenance_start_date")
    with st.form(key="add_maintenance_form"):
        col1, col2 = st.columns(2)
        with col1:
            task_name = st.text_input("اسم البند:")
            period_hours = st.number_input("⏱️ عدد الساعات بين الصيانة:", min_value=1, step=1, value=24)
            st.caption(f"✅ الفترة: {period_hours} ساعة = {period_hours/24:.2f} يوم")
            task_image = st.file_uploader("🖼️ صورة توضيحية:", type=APP_CONFIG["ALLOWED_IMAGE_TYPES"], key="maintenance_task_image")
        with col2:
            notes = st.text_area("ملاحظات:")
            default_spare = st.text_input("قطعة غيار افتراضية:", placeholder="اختياري")
        submitted = st.form_submit_button("➕ إضافة بند صيانة")
        if submitted:
            if not task_name:
                st.error("❌ الرجاء إدخال اسم البند")
            else:
                image_url = None
                if task_image:
                    task_id = str(uuid.uuid4())[:8]
                    image_url = upload_image_to_github(task_image, "maintenance_task", task_id)
                sheets_edit = add_maintenance_task(sheets_edit, selected_equipment, task_name, period_hours, start_date, notes, default_spare, image_url)
                if save_and_push_to_github(sheets_edit, f"إضافة بند صيانة '{task_name}'"):
                    st.success("✅ تم إضافة البند بنجاح")
                    st.rerun()
                else:
                    st.error("❌ فشل الحفظ")
    return sheets_edit

# ------------------------------- دالة إدارة البيانات الرئيسية -------------------------------
def manage_data_edit(sheets_edit):
    if sheets_edit is None:
        st.warning("الملف غير موجود. استخدم زر 'تحديث من GitHub' في الشريط الجانبي أولاً")
        return sheets_edit
    if APP_CONFIG["SPARE_PARTS_SHEET"] not in sheets_edit:
        sheets_edit[APP_CONFIG["SPARE_PARTS_SHEET"]] = load_spare_parts()
    if APP_CONFIG["MAINTENANCE_SHEET"] not in sheets_edit:
        sheets_edit[APP_CONFIG["MAINTENANCE_SHEET"]] = load_maintenance_tasks()
    # إزالة تبويب إضافة حدث عطل لأنه أصبح مستقلاً
    tab_names = ["📋 عرض الأقسام", "🔧 إدارة الماكينات", "➕ إضافة قسم جديد", "📦 قطع الغيار", "🛠 الصيانة الوقائية"]
    tabs_edit = st.tabs(tab_names)
    with tabs_edit[0]:
        st.subheader("جميع الأقسام")
        if sheets_edit:
            dept_names = [name for name in sheets_edit.keys() if name not in [APP_CONFIG["SPARE_PARTS_SHEET"], APP_CONFIG["MAINTENANCE_SHEET"]]]
            if dept_names:
                dept_tabs = st.tabs(dept_names)
                for i, dept_name in enumerate(dept_names):
                    with dept_tabs[i]:
                        df = sheets_edit[dept_name]
                        display_sheet_data(dept_name, df, f"view_{dept_name}", sheets_edit)
                        with st.expander("✏️ تعديل مباشر للبيانات", expanded=False):
                            edited_df = st.data_editor(df.astype(str), num_rows="dynamic", use_container_width=True, key=f"editor_{dept_name}")
                            if st.button(f"💾 حفظ", key=f"save_{dept_name}"):
                                sheets_edit[dept_name] = edited_df.astype(object)
                                if save_and_push_to_github(sheets_edit, f"تعديل بيانات في قسم {dept_name}"):
                                    st.cache_data.clear()
                                    st.success("تم الحفظ والرفع إلى GitHub!")
                                    st.rerun()
            else:
                st.info("لا توجد أقسام بعد")
    with tabs_edit[1]:
        if sheets_edit:
            sheet_name = st.selectbox("اختر القسم:", [name for name in sheets_edit.keys() if name not in [APP_CONFIG["SPARE_PARTS_SHEET"], APP_CONFIG["MAINTENANCE_SHEET"]]], key="manage_machines_sheet_edit")
            manage_machines(sheets_edit, sheet_name)
    with tabs_edit[2]:
        sheets_edit = add_new_department(sheets_edit)
    with tabs_edit[3]:
        sheets_edit = manage_spare_parts_tab(sheets_edit)
    with tabs_edit[4]:
        sheets_edit = preventive_maintenance_tab(sheets_edit)
    return sheets_edit

# ------------------------------- الواجهة الرئيسية -------------------------------
with st.sidebar:
    st.header("الجلسة")
    if not st.session_state.get("logged_in"):
        if not login_ui():
            st.stop()
    else:
        state = cleanup_sessions(load_state())
        username = st.session_state.username
        rem = remaining_time(state, username)
        if rem:
            mins, secs = divmod(int(rem.total_seconds()), 60)
            st.success(f"👋 {username} | ⏳ {mins:02d}:{secs:02d}")
        st.markdown("---")
        if st.button("🔄 تحديث  "):
            if fetch_from_github_requests():
                st.rerun()
        if st.button("مسح مهملات"):
            st.cache_data.clear()
            st.rerun()
        if st.button("🚪 تسجيل الخروج"):
            logout_action()

# ------------------------------- تحميل البيانات الرئيسية -------------------------------
all_sheets = load_all_sheets()
sheets_edit = load_sheets_for_edit()
st.title(f"{APP_CONFIG['APP_ICON']} {APP_CONFIG['APP_TITLE']}")
user_role = st.session_state.get("user_role", "viewer")
user_permissions = st.session_state.get("user_permissions", ["view"])
can_edit = (user_role == "admin" or user_role == "editor" or "edit" in user_permissions)

# ------------------------------- بناء قائمة التبويبات حسب الصلاحيات -------------------------------
all_sheets = load_all_sheets()
sheets_edit = load_sheets_for_edit()
st.title(f"{APP_CONFIG['APP_ICON']} {APP_CONFIG['APP_TITLE']}")
user_role = st.session_state.get("user_role", "viewer")
username = st.session_state.get("username", "")

def user_can(permission_type):
    if username == "admin":
        return True
    perms = get_user_permissions(username)
    if perms.get("all_sections", False):
        return True
    sections_perms = perms.get("sections_permissions", {})
    for perms_list in sections_perms.values():
        if permission_type in perms_list:
            return True
    return False

can_add_event = user_can("add_event")
can_manage_machines = user_can("manage_machines")
can_edit_data = user_can("edit")

tabs_list = ["🔍 بحث متقدم", "📊 تحليل الأعطال", "🔔 الإشعارات"]
if can_add_event:
    tabs_list.append("➕ إضافة حدث عطل")
if can_manage_machines:
    tabs_list.append("🔧 إدارة الماكينات")
if can_edit_data:
    tabs_list.append("🛠 تعديل وإدارة البيانات")
tabs_list.append("📞 الدعم الفني")
tabs = st.tabs(tabs_list)

# ------------------------------- تبويب البحث -------------------------------
with tabs[0]:
    search_across_sheets(all_sheets)

# ------------------------------- تبويب تحليل الأعطال -------------------------------
with tabs[1]:
    failures_analysis_tab(all_sheets)

# ------------------------------- تبويب الإشعارات -------------------------------
with tabs[2]:
    st.header("🔔 الإشعارات")
    if st.session_state.get("username") == "admin":
        st.subheader("📋 آخر النشاطات")
        activity_log = load_activity_log()
        if activity_log:
            for entry in reversed(activity_log[-20:]):
                timestamp = datetime.fromisoformat(entry["timestamp"]).strftime("%Y-%m-%d %H:%M:%S")
                action_type = entry["action_type"]
                username_act = entry["username"]
                details = entry["details"]
                if action_type == "add_event":
                    icon = "🆕"
                elif action_type == "execute_maintenance":
                    icon = "✅"
                elif action_type == "add_spare_part":
                    icon = "🔩"
                elif action_type == "add_maintenance_task":
                    icon = "🛠️"
                else:
                    icon = "📌"
                st.info(f"{icon} **{timestamp}** - **{username_act}**: {details}")
        else:
            st.info("لا توجد نشاطات مسجلة بعد.")
        st.markdown("---")
    col1, col2 = st.columns(2)
    with col1:
        st.subheader("⚠️ قطع غيار حرجة")
        critical = get_critical_spare_parts()
        if critical:
            for part in critical:
                threshold = part.get('حد_الإنذار', 1)
                st.error(f"🔴 **{part['اسم القطعة']}** (قسم: {part.get('القسم', 'غير محدد')}) - الرصيد: {part['الرصيد الموجود']} < حد الإنذار: {threshold}")
        else:
            st.success("✅ لا توجد قطع غيار حرجة")
    with col2:
        st.subheader("🔧 صيانة مستحقة")
        overdue, upcoming = get_upcoming_maintenance(3)
        if not overdue.empty:
            st.warning("🟡 صيانة متأخرة:")
            for _, row in overdue.iterrows():
                st.write(f"- {row['المعدة']}: {row['اسم_البند']} (تاريخ مستحق: {row['التاريخ_التالي'].strftime('%Y-%m-%d')})")
        else:
            st.info("✅ لا توجد صيانات متأخرة")
        if not upcoming.empty:
            st.info("🟢 صيانة قادمة خلال 3 أيام:")
            for _, row in upcoming.iterrows():
                days = (row['التاريخ_التالي'].date() - datetime.now().date()).days
                st.write(f"- {row['المعدة']}: {row['اسم_البند']} (بعد {days} يوم)")
        else:
            st.info("✅ لا توجد صيانات قادمة")

# ------------------------------- تبويب إضافة حدث عطل -------------------------------
if can_add_event:
    # حساب الفهرس المناسب: بعد أول 3 تبويبات ثابتة
    idx_add = 3
    with tabs[idx_add]:
        if sheets_edit:
            allowed_for_add = []
            for sheet_name in sheets_edit.keys():
                if sheet_name in [APP_CONFIG["SPARE_PARTS_SHEET"], APP_CONFIG["MAINTENANCE_SHEET"]]:
                    continue
                if has_section_permission(username, sheet_name, "add_event"):
                    allowed_for_add.append(sheet_name)
            if allowed_for_add:
                sheet_name = st.selectbox("اختر القسم:", allowed_for_add, key="add_event_sheet_main")  # مفتاح فريد
                sheets_edit = add_new_event(sheets_edit, sheet_name)
            else:
                st.warning("لا توجد أقسام مسموح لك بإضافة أحداث فيها.")
        else:
            st.warning("لا توجد بيانات")

# ------------------------------- تبويب إدارة الماكينات -------------------------------
if can_manage_machines:
    idx_manage = 3 + (1 if can_add_event else 0)
    with tabs[idx_manage]:
        if sheets_edit:
            allowed_for_machines = []
            for sheet_name in sheets_edit.keys():
                if sheet_name in [APP_CONFIG["SPARE_PARTS_SHEET"], APP_CONFIG["MAINTENANCE_SHEET"]]:
                    continue
                if has_section_permission(username, sheet_name, "manage_machines"):
                    allowed_for_machines.append(sheet_name)
            if allowed_for_machines:
                sheet_name = st.selectbox("اختر القسم:", allowed_for_machines, key="manage_machines_sheet_main")  # مفتاح فريد
                manage_machines(sheets_edit, sheet_name)
            else:
                st.warning("لا توجد أقسام مسموح لك بإدارة الماكينات فيها.")
        else:
            st.warning("لا توجد بيانات")

# ------------------------------- تبويب تعديل وإدارة البيانات الكاملة -------------------------------
if can_edit_data:
    idx_edit = 3 + (1 if can_add_event else 0) + (1 if can_manage_machines else 0)
    with tabs[idx_edit]:
        sheets_edit = manage_data_edit(sheets_edit)

# ------------------------------- تبويب الدعم الفني -------------------------------
idx_support = len(tabs_list) - 1
with tabs[idx_support]:
    st.header("📞 الدعم الفني")
    st.markdown("### تم تصميم وتنفيذ هذا السيستم بواسطه **م.محمد عبدالله**")
    st.markdown("#### رئيس قسم المحطات والتحضيرات بمصنع بيل يارن1")
    st.markdown("---")
    st.markdown("📧 **للتواصل والدعم الفني:** `01274424062`")
    st.markdown("---")
    YOUTUBE_LINK = "https://youtube.com/@cardtrutchler?si=bayhxhRXgCzWSpCl"
    st.markdown(f"[📺 قناة اليوتيوب الرسمية]({YOUTUBE_LINK})")
    st.caption(f"رابط القناة: {YOUTUBE_LINK}")
    st.markdown("---")
    support_config = load_support_config()
    current_image_url = support_config.get("image_url", "")
    st.subheader("🖼️ ")
    if current_image_url and current_image_url.strip():
        try:
            st.image(current_image_url, use_container_width=True)
            st.caption("✅ ")
        except:
            st.warning("⚠️ تعذر عرض الصورة المحفوظة")
    else:
        if st.session_state.get("username") == "admin":
            st.info("📷 لم يتم رفع صورة المطور بعد. يمكنك رفعها الآن (مرة واحدة فقط، ولن يمكن تغييرها لاحقاً).")
            uploaded_img = st.file_uploader("رفع صورة للمطور (jpg, png, ...)", type=APP_CONFIG["ALLOWED_IMAGE_TYPES"], key="support_img_upload_once")
            if uploaded_img is not None:
                with st.spinner("جاري رفع الصورة..."):
                    image_url = upload_image_to_github(uploaded_img, "support", "developer_image_final")
                    if image_url:
                        support_config["image_url"] = image_url
                        save_support_config(support_config)
                        st.success("✅ تم رفع الصورة بنجاح! لن يظهر خيار الرفع مرة أخرى.")
                        st.rerun()
                    else:
                        st.error("❌ فشل رفع الصورة، حاول مرة أخرى.")
        else:
            st.info("📷 لم يتم رفع صورة المطور بعد. سيتم رفعها بواسطة مدير النظام.")

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
import firebase_admin
from firebase_admin import credentials, firestore, storage

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
SUPPORT_CONFIG_FILE = "support_config.json"

GITHUB_EXCEL_URL = f"https://github.com/{APP_CONFIG['REPO_NAME'].split('/')[0]}/{APP_CONFIG['REPO_NAME'].split('/')[1]}/raw/{APP_CONFIG['BRANCH']}/{APP_CONFIG['FILE_PATH']}"
GITHUB_USERS_URL = "https://raw.githubusercontent.com/mahmedabdallh123/sky/refs/heads/main/users.json"
GITHUB_REPO_USERS = "mahmedabdallh123/sky"
GITHUB_TOKEN = st.secrets.get("github", {}).get("token", None)
GITHUB_AVAILABLE = GITHUB_TOKEN is not None
ACTIVITY_LOG_FILE = "activity_log.json"

# ------------------------------- تهيئة Firebase -------------------------------
try:
    firebase_creds = dict(st.secrets["firebase"])
    cred = credentials.Certificate(firebase_creds)
    if not firebase_admin._apps:
        firebase_admin.initialize_app(cred, {
            'storageBucket': 'cmms--system.firebasestorage.app'  # تحقق من اسم البكت الخاص بك
        })
    db = firestore.client()
    bucket = storage.bucket()
    FIREBASE_AVAILABLE = True
except Exception as e:
    st.error(f"❌ فشل تهيئة Firebase: {e}")
    FIREBASE_AVAILABLE = False

# ------------------------------- دوال رفع الصور إلى Firebase -------------------------------
def upload_image_to_firebase(image_file, entity_type, entity_id, custom_filename=None):
    if not FIREBASE_AVAILABLE:
        st.error("❌ Firebase غير متوفر، لا يمكن رفع الصور")
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
        blob = bucket.blob(f"images/{entity_type}/{filename}")
        blob.upload_from_file(buffer, content_type='image/jpeg')
        blob.make_public()
        return blob.public_url
    except Exception as e:
        st.error(f"❌ خطأ في رفع الصورة إلى Firebase: {e}")
        return None

# للتوافق مع الكود القديم (يمكن الاحتفاظ بدالة GitHub للنسخ الاحتياطي)
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
        repo.create_file(path=repo_path, message=f"Add image for {entity_type} {entity_id}", content=content, branch=APP_CONFIG["BRANCH"])
        return f"https://raw.githubusercontent.com/{APP_CONFIG['REPO_NAME']}/{APP_CONFIG['BRANCH']}/{repo_path}"
    except Exception as e:
        st.error(f"❌ خطأ في معالجة الصورة لـ GitHub: {e}")
        return None

# استخدام Firebase كأساس، مع إمكانية النسخ الاحتياطي على GitHub
def upload_image(image_file, entity_type, entity_id, custom_filename=None):
    url = upload_image_to_firebase(image_file, entity_type, entity_id, custom_filename)
    if GITHUB_AVAILABLE and url:
        # نسخ احتياطي على GitHub (اختياري)
        try:
            upload_image_to_github(image_file, entity_type, entity_id, custom_filename)
        except:
            pass
    return url

def get_image_component(image_url, caption=""):
    if not image_url or not isinstance(image_url, str):
        return None
    try:
        return st.image(image_url, caption=caption, use_container_width=True)
    except:
        st.warning(f"⚠️ تعذر عرض الصورة: {image_url}")
        return None


# ------------------------------- دوال إعدادات الدعم الفني -------------------------------
def load_support_config():
    default_config = {"image_url": "", "youtube_link": ""}
    if FIREBASE_AVAILABLE:
        try:
            doc_ref = db.collection('settings').document('support')
            doc = doc_ref.get()
            if doc.exists:
                return doc.to_dict()
        except:
            pass
    # fallback to local file or GitHub
    if os.path.exists(SUPPORT_CONFIG_FILE):
        try:
            with open(SUPPORT_CONFIG_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except:
            return default_config
    return default_config

def save_support_config(config):
    if FIREBASE_AVAILABLE:
        try:
            db.collection('settings').document('support').set(config, merge=True)
        except:
            pass
    with open(SUPPORT_CONFIG_FILE, "w", encoding="utf-8") as f:
        json.dump(config, f, indent=2, ensure_ascii=False)
    if GITHUB_AVAILABLE:
        try:
            g = Github(GITHUB_TOKEN)
            repo = g.get_repo(APP_CONFIG["REPO_NAME"])
            config_str = json.dumps(config, indent=2, ensure_ascii=False)
            try:
                contents = repo.get_contents(SUPPORT_CONFIG_FILE, ref=APP_CONFIG["BRANCH"])
                repo.update_file(SUPPORT_CONFIG_FILE, "تحديث إعدادات الدعم الفني", config_str, contents.sha, branch=APP_CONFIG["BRANCH"])
            except:
                repo.create_file(SUPPORT_CONFIG_FILE, "إنشاء إعدادات الدعم الفني", config_str, branch=APP_CONFIG["BRANCH"])
        except:
            pass


# ------------------------------- دوال قطع الغيار -------------------------------
def load_spare_parts():
    if not FIREBASE_AVAILABLE:
        # fallback to Excel
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
    try:
        docs = db.collection('spare_parts').stream()
        records = [doc.to_dict() for doc in docs]
        if records:
            df = pd.DataFrame(records)
        else:
            df = pd.DataFrame(columns=APP_CONFIG["SPARE_PARTS_COLUMNS"])
        # تنظيف البيانات
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
    except Exception as e:
        st.error(f"خطأ في تحميل قطع الغيار من Firebase: {e}")
        return pd.DataFrame(columns=APP_CONFIG["SPARE_PARTS_COLUMNS"])

def save_spare_parts(df):
    if not FIREBASE_AVAILABLE:
        # fallback to local Excel
        try:
            with pd.ExcelWriter(APP_CONFIG["LOCAL_FILE"], engine="openpyxl", mode='a', if_sheet_exists='replace') as writer:
                df.to_excel(writer, sheet_name=APP_CONFIG["SPARE_PARTS_SHEET"], index=False)
        except:
            pass
        return
    # حفظ في Firebase
    batch = db.batch()
    # حذف المستندات القديمة
    for doc in db.collection('spare_parts').stream():
        batch.delete(doc.reference)
    batch.commit()
    # إضافة المستندات الجديدة
    for _, row in df.iterrows():
        doc_ref = db.collection('spare_parts').document()
        doc_ref.set(row.to_dict())
    # نسخ احتياطي على GitHub (اختياري)
    if GITHUB_AVAILABLE:
        try:
            with pd.ExcelWriter(APP_CONFIG["LOCAL_FILE"], engine="openpyxl") as writer:
                df.to_excel(writer, sheet_name=APP_CONFIG["SPARE_PARTS_SHEET"], index=False)
            push_to_github()  # سنضيف هذه الدالة لاحقاً
        except:
            pass

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
    # حفظ التغيير
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


# ------------------------------- دوال الصيانة الوقائية -------------------------------
def load_maintenance_tasks():
    if not FIREBASE_AVAILABLE:
        # fallback to Excel
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
        except Exception:
            return pd.DataFrame(columns=APP_CONFIG["MAINTENANCE_COLUMNS"])
    try:
        docs = db.collection('maintenance_tasks').stream()
        records = [doc.to_dict() for doc in docs]
        if records:
            df = pd.DataFrame(records)
        else:
            df = pd.DataFrame(columns=APP_CONFIG["MAINTENANCE_COLUMNS"])
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

def save_maintenance_tasks(df):
    if not FIREBASE_AVAILABLE:
        # fallback to local Excel
        try:
            with pd.ExcelWriter(APP_CONFIG["LOCAL_FILE"], engine="openpyxl", mode='a', if_sheet_exists='replace') as writer:
                df.to_excel(writer, sheet_name=APP_CONFIG["MAINTENANCE_SHEET"], index=False)
        except:
            pass
        return
    batch = db.batch()
    for doc in db.collection('maintenance_tasks').stream():
        batch.delete(doc.reference)
    batch.commit()
    for _, row in df.iterrows():
        doc_ref = db.collection('maintenance_tasks').document()
        doc_ref.set(row.to_dict())
    if GITHUB_AVAILABLE:
        try:
            with pd.ExcelWriter(APP_CONFIG["LOCAL_FILE"], engine="openpyxl") as writer:
                df.to_excel(writer, sheet_name=APP_CONFIG["MAINTENANCE_SHEET"], index=False)
            push_to_github()
        except:
            pass

def get_tasks_for_equipment(equipment_name):
    df = load_maintenance_tasks()
    if df.empty:
        return df
    return df[df["المعدة"] == equipment_name]

def add_maintenance_task(sheets_edit, equipment, task_name, period_hours, start_date=None, notes="", default_spare="", image_url=None):
    # نحن هنا نستخدم sheets_edit فقط كوسيط، لكن سنحفظ مباشرة في Firebase
    df = load_maintenance_tasks()
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
    save_maintenance_tasks(new_df)
    log_activity("add_maintenance_task", f"تم إضافة بند صيانة '{task_name}' للماكينة {equipment} (فترة {period_hours} ساعة)")
    # إرجاع sheets_edit للتوافق (يمكن تجاهله)
    if "sheets_edit" in st.session_state:
        st.session_state.sheets_edit[APP_CONFIG["MAINTENANCE_SHEET"]] = new_df
    return st.session_state.get("sheets_edit", {})

def get_upcoming_maintenance(days_ahead=3):
    df = load_maintenance_tasks()
    if df.empty:
        return pd.DataFrame(), pd.DataFrame()
    today = datetime.now().date()
    overdue = df[df["التاريخ_التالي"] < pd.Timestamp(today)]
    upcoming = df[(df["التاريخ_التالي"] >= pd.Timestamp(today)) & (df["التاريخ_التالي"] <= pd.Timestamp(today + timedelta(days=days_ahead)))]
    return overdue, upcoming

# ------------------------------- دوال الأقسام (الأعطال) -------------------------------
def load_all_sections():
    """تحميل جميع الأقسام كقاموس {اسم_القسم: DataFrame} من Firestore"""
    if not FIREBASE_AVAILABLE:
        # fallback to Excel
        if not os.path.exists(APP_CONFIG["LOCAL_FILE"]):
            return {}
        try:
            sheets = pd.read_excel(APP_CONFIG["LOCAL_FILE"], sheet_name=None)
            if not sheets:
                return {}
            for name, df in sheets.items():
                if df.empty:
                    continue
                df.columns = df.columns.astype(str).str.strip()
                df = df.fillna('')
                sheets[name] = df
            return sheets
        except:
            return {}
    sections_ref = db.collection('sections')
    docs = sections_ref.stream()
    data = {}
    for doc in docs:
        doc_data = doc.to_dict()
        records = doc_data.get('records', [])
        if records:
            df = pd.DataFrame(records)
        else:
            df = pd.DataFrame(columns=APP_CONFIG["DEFAULT_SHEET_COLUMNS"])
        data[doc.id] = df
    return data

def save_section(section_name, df):
    """حفظ DataFrame القسم في Firestore"""
    if not FIREBASE_AVAILABLE:
        # fallback to local Excel
        try:
            with pd.ExcelWriter(APP_CONFIG["LOCAL_FILE"], engine="openpyxl") as writer:
                # نحتاج لحفظ كل الأقسام، لكن هذا مبسط
                all_sheets = load_all_sections()
                all_sheets[section_name] = df
                for name, sh in all_sheets.items():
                    sh.to_excel(writer, sheet_name=name, index=False)
            if GITHUB_AVAILABLE:
                push_to_github()
        except Exception as e:
            st.error(f"فشل الحفظ المحلي: {e}")
        return
    records = df.to_dict('records')
    db.collection('sections').document(section_name).set({'records': records}, merge=True)
    # نسخ احتياطي على GitHub (اختياري)
    if GITHUB_AVAILABLE:
        try:
            all_sheets = load_all_sections()
            all_sheets[section_name] = df
            with pd.ExcelWriter(APP_CONFIG["LOCAL_FILE"], engine="openpyxl") as writer:
                for name, sh in all_sheets.items():
                    sh.to_excel(writer, sheet_name=name, index=False)
            push_to_github()
        except:
            pass


# ------------------------------- دوال المستخدمين -------------------------------
def load_users():
    if not FIREBASE_AVAILABLE:
        # fallback to GitHub or local file
        try:
            response = requests.get(GITHUB_USERS_URL, timeout=10)
            response.raise_for_status()
            users_data = response.json()
            return users_data
        except:
            if os.path.exists(USERS_FILE):
                with open(USERS_FILE, "r", encoding="utf-8") as f:
                    return json.load(f)
        default = {"admin": {"password": "1234", "role": "admin", "permissions": {"all_sections": True}, "sections_permissions": {}}}
        return default
    try:
        docs = db.collection('users').stream()
        users = {}
        for doc in docs:
            users[doc.id] = doc.to_dict()
        if not users:
            # إنشاء مستخدم admin افتراضي
            admin_data = {"password": "1234", "role": "admin", "permissions": {"all_sections": True}, "sections_permissions": {}}
            db.collection('users').document('admin').set(admin_data)
            users['admin'] = admin_data
        return users
    except:
        return {"admin": {"password": "1234", "role": "admin", "permissions": {"all_sections": True}, "sections_permissions": {}}}

def save_users(users_data):
    if not FIREBASE_AVAILABLE:
        # حفظ محلياً أو GitHub
        with open(USERS_FILE, "w", encoding="utf-8") as f:
            json.dump(users_data, f, indent=4, ensure_ascii=False)
        if GITHUB_AVAILABLE:
            try:
                g = Github(GITHUB_TOKEN)
                repo = g.get_repo(GITHUB_REPO_USERS)
                users_json = json.dumps(users_data, indent=4, ensure_ascii=False)
                try:
                    contents = repo.get_contents("users.json", ref="main")
                    repo.update_file("users.json", "تحديث المستخدمين", users_json, contents.sha, branch="main")
                except:
                    repo.create_file("users.json", "إنشاء المستخدمين", users_json, branch="main")
            except:
                pass
        return
    batch = db.batch()
    for doc in db.collection('users').stream():
        batch.delete(doc.reference)
    batch.commit()
    for username, data in users_data.items():
        db.collection('users').document(username).set(data)

def download_users_from_github():
    return load_users()  # مبسط

def upload_users_to_github(users_data):
    save_users(users_data)

# ------------------------------- دوال سجل النشاطات -------------------------------
def log_activity(action_type, details, username=None):
    if username is None:
        username = st.session_state.get("username", "غير معروف")
    log_entry = {
        "timestamp": datetime.now().isoformat(),
        "username": username,
        "action_type": action_type,
        "details": details
    }
    if FIREBASE_AVAILABLE:
        try:
            db.collection('activity_log').add(log_entry)
        except:
            pass
    # أيضاً حفظ محلياً كنسخة احتياطية
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
    if FIREBASE_AVAILABLE:
        try:
            docs = db.collection('activity_log').order_by('timestamp', direction=firestore.Query.DESCENDING).limit(100).stream()
            return [doc.to_dict() for doc in docs]
        except:
            pass
    if os.path.exists(ACTIVITY_LOG_FILE):
        with open(ACTIVITY_LOG_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return []

# ------------------------------- دوال الجلسات -------------------------------
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
    allowed = []
    for sheet_name in all_sheets.keys():
        if sheet_name in [APP_CONFIG["SPARE_PARTS_SHEET"], APP_CONFIG["MAINTENANCE_SHEET"]]:
            continue
        if has_section_permission(username, sheet_name, required_permission):
            allowed.append(sheet_name)
    return allowed


# ------------------------------- دوال الملفات (للتزامن مع GitHub) -------------------------------
def fetch_from_github_requests():
    """جلب أحدث نسخة من GitHub إلى الملف المحلي (للاستخدام كنسخة احتياطية)"""
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
    """الحفاظ على الواجهة القديمة: تحميل من Firebase وتحويلها إلى صيغة الأقسام"""
    return load_all_sections()

@st.cache_data(show_spinner=False)
def load_sheets_for_edit():
    return load_all_sections()

def save_excel_locally(sheets_dict):
    """حفظ الأقسام في ملف Excel محلي (للنسخ الاحتياطي)"""
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
        if not token or not GITHUB_AVAILABLE:
            return False
        g = Github(token)
        repo = g.get_repo(APP_CONFIG["REPO_NAME"])
        with open(APP_CONFIG["LOCAL_FILE"], "rb") as f:
            content = f.read()
        try:
            contents = repo.get_contents(APP_CONFIG["FILE_PATH"], ref=APP_CONFIG["BRANCH"])
            repo.update_file(path=APP_CONFIG["FILE_PATH"], message=f"تحديث البيانات - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}", content=content, sha=contents.sha, branch=APP_CONFIG["BRANCH"])
            return True
        except GithubException as e:
            if e.status == 404:
                repo.create_file(path=APP_CONFIG["FILE_PATH"], message=f"إنشاء ملف جديد - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}", content=content, branch=APP_CONFIG["BRANCH"])
                return True
            else:
                st.error(f"❌ خطأ GitHub: {e}")
                return False
    except Exception as e:
        st.error(f"❌ فشل الرفع: {e}")
        return False

def save_and_push_to_github(sheets_dict, operation_name):
    """حفظ في GitHub فقط كنسخة احتياطية (لا يتم استخدامها كأساس)"""
    st.info(f"💾 جاري حفظ نسخة احتياطية على GitHub: {operation_name}...")
    if save_excel_locally(sheets_dict):
        if push_to_github():
            st.success("✅ تم حفظ النسخة الاحتياطية على GitHub")
        else:
            st.warning("⚠️ فشل رفع النسخة الاحتياطية إلى GitHub")
        return True
    else:
        st.error("❌ فشل حفظ النسخة الاحتياطية محلياً")
        return False


# ------------------------------- دوال تحليل الأعطال المتقدمة -------------------------------
def flexible_date_parser(date_series):
    def parse_single(val):
        if pd.isna(val) or val == "":
            return pd.NaT
        if isinstance(val, (pd.Timestamp, datetime)):
            return val
        val_str = str(val).strip().replace('\\', '/')
        for fmt in ('%Y-%m-%d', '%d/%m/%Y', '%d-%m-%Y', '%d.%m.%Y', '%Y/%m/%d'):
            try:
                return pd.to_datetime(val_str, format=fmt, errors='raise')
            except:
                continue
        return pd.to_datetime(val_str, errors='coerce')
    return date_series.apply(parse_single)

def analyze_time_between_corrections(df, filter_text=None):
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
            gap_days = (next_row["التاريخ"] - current["التاريخ"]).total_seconds() / (24*3600)
            prev_correction = eq_data.iloc[i-1]["الإجراء التصحيحي"] if i>0 else None
            prev_date = eq_data.iloc[i-1]["التاريخ"] if i>0 else None
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
            st.info("ℹ️ لا توجد بيانات كافية لحساب الفجوات")
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
        st.download_button("📥 تحميل التقرير (Excel)", excel_buffer, f"corrections_analysis_{selected_section}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

# ------------------------------- دوال إدارة المعدات والأقسام -------------------------------
def get_equipment_list_from_sheet(df):
    if df is None or df.empty or "المعدة" not in df.columns:
        return []
    equipment = df["المعدة"].dropna().unique()
    equipment = [str(e).strip() for e in equipment if str(e).strip() != ""]
    return sorted(equipment)

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
    # حفظ في Firebase
    save_section(sheet_name, sheets_edit[sheet_name])
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
    save_section(sheet_name, new_df)
    return True, f"تم حذف جميع سجلات الماكينة '{equipment_name}'"

def add_new_department(sheets_edit):
    if st.session_state.get("username") == "admin":
        st.subheader("➕ إضافة قسم جديد")
        st.info("سيتم إنشاء قسم جديد (شيت جديد) في Firebase لإدارة ماكينات هذا القسم")
        col1, col2 = st.columns(2)
        with col1:
            new_department_name = st.text_input("📝 اسم القسم الجديد:", key="new_department_name", placeholder="مثال: قسم الميكانيكا")
            if new_department_name and new_department_name in sheets_edit:
                st.error(f"❌ القسم '{new_department_name}' موجود بالفعل!")
            elif new_department_name:
                st.success(f"✅ اسم القسم '{new_department_name}' متاح")
        with col2:
            st.markdown("#### 📋 إعدادات الأعمدة")
            use_default = st.checkbox("استخدام الأعمدة الافتراضية", value=True, key="use_default_columns")
            if use_default:
                columns_list = APP_CONFIG["DEFAULT_SHEET_COLUMNS"]
            else:
                columns_text = st.text_area("✏️ الأعمدة (كل عمود في سطر):", value="\n".join(APP_CONFIG["DEFAULT_SHEET_COLUMNS"]), key="custom_columns", height=150)
                columns_list = [col.strip() for col in columns_text.split("\n") if col.strip()]
                if not columns_list:
                    columns_list = APP_CONFIG["DEFAULT_SHEET_COLUMNS"]
        st.markdown("---")
        st.markdown("### 📋 معاينة القسم الجديد")
        preview_df = pd.DataFrame(columns=columns_list)
        st.dataframe(preview_df, use_container_width=True)
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
            # حفظ في Firebase
            save_section(new_department_name, new_df)
            # نسخة احتياطية على GitHub
            save_and_push_to_github(sheets_edit, f"إنشاء قسم جديد: {new_department_name}")
            st.success(f"✅ تم إنشاء القسم '{new_department_name}' بنجاح!")
            st.cache_data.clear()
            st.balloons()
            st.rerun()
        st.markdown("---")
        st.subheader("🗑️ حذف قسم موجود")
        st.warning("⚠️ انتبه: حذف القسم سيؤدي إلى حذف جميع بياناته نهائياً")
        deletable_sections = [name for name in sheets_edit.keys() if name not in [APP_CONFIG["SPARE_PARTS_SHEET"], APP_CONFIG["MAINTENANCE_SHEET"]]]
        if deletable_sections:
            selected_dept = st.selectbox("اختر القسم المراد حذفه:", deletable_sections, key="delete_department_select")
            if selected_dept:
                confirm = st.text_input("لتأكيد الحذف، اكتب اسم القسم هنا:", key="delete_confirm")
                if confirm == selected_dept:
                    if st.button("🗑️ حذف القسم نهائياً", key="delete_department_btn", type="primary"):
                        # حذف من Firestore
                        db.collection('sections').document(selected_dept).delete()
                        # حذف قطع الغيار المرتبطة
                        spare_df = load_spare_parts()
                        spare_df = spare_df[spare_df["القسم"] != selected_dept]
                        save_spare_parts(spare_df)
                        # تحديث sheets_edit
                        del sheets_edit[selected_dept]
                        st.success(f"✅ تم حذف القسم '{selected_dept}'")
                        st.rerun()
    else:
        st.info("🔒 فقط المدير (admin) يمكنه إضافة أو حذف الأقسام.")
    return sheets_edit

def manage_machines(sheets_edit, sheet_name, unique_suffix=""):
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
    with st.form(key=f"add_machine_form_{sheet_name}_{unique_suffix}"):
        new_machine = st.text_input("➕ اسم الماكينة الجديدة:", key=f"new_machine_input_{sheet_name}_{unique_suffix}")
        submitted_add = st.form_submit_button("➕ إضافة ماكينة")
        if submitted_add and new_machine:
            success, msg = add_equipment_to_sheet_data(sheets_edit, sheet_name, new_machine)
            if success:
                st.success(msg)
                st.rerun()
            else:
                st.error(msg)
    if equipment_list:
        st.markdown("#### 🗑️ حذف ماكينة")
        if st.session_state.get("username") == "admin":
            with st.form(key=f"delete_machine_form_{sheet_name}_{unique_suffix}"):
                machine_to_delete = st.selectbox("اختر الماكينة للحذف:", equipment_list, key=f"delete_machine_select_{sheet_name}_{unique_suffix}")
                st.warning("⚠️ تحذير: حذف الماكينة سيؤدي إلى حذف جميع سجلات الأعطال المرتبطة بها نهائياً!")
                submitted_del = st.form_submit_button("🗑️ حذف الماكينة نهائياً")
                if submitted_del:
                    success, msg = remove_equipment_from_sheet_data(sheets_edit, sheet_name, machine_to_delete)
                    if success:
                        st.success(msg)
                        st.rerun()
                    else:
                        st.error(msg)
        else:
            st.info("🔒 حذف الماكينات مقيد بصلاحيات المدير (admin).")

def add_new_event(sheets_edit, sheet_name):
    st.markdown(f"### 📝 إضافة حدث عطل جديد في قسم: {sheet_name}")
    df = sheets_edit[sheet_name]
    equipment_list = get_equipment_list_from_sheet(df)
    if not equipment_list:
        st.warning("⚠ لا توجد ماكينات مسجلة في هذا القسم. يرجى إضافة ماكينة أولاً")
        return sheets_edit
    selected_equipment = st.selectbox("🔧 اختر الماكينة:", equipment_list, key="equipment_select")
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
            technician_rating = st.select_slider("⭐ قدرة الفني:", options=[1,2,3,4,5], value=3)
            safety_compliance = st.selectbox("🛡️ الالتزام بتعليمات السلامة:", ["", "ملتزم بالكامل", "ملتزم جزئياً", "غير ملتزم", "غير مطبق"])
            st.markdown("---")
            st.markdown("**🔩 قطع الغيار المستخدمة**")
            part_name = ""
            consume_qty = 0
            if spare_parts_list:
                part_names = [f"{name} (الرصيد: {qty})" for name, qty in spare_parts_list]
                selected_part_display = st.selectbox("اختر قطعة:", [""] + part_names, key="spare_part_select")
                if selected_part_display:
                    part_name = selected_part_display.split(" (")[0]
                    current_qty = next((qty for name, qty in spare_parts_list if name == part_name), 0)
                    st.caption(f"الرصيد الحالي: {current_qty}")
                    consume_qty = st.number_input("الكمية المستخدمة:", min_value=1, max_value=max(1, current_qty), value=1, step=1, key="consume_qty")
                    if consume_qty > current_qty:
                        st.error(f"⚠️ الرصيد غير كافٍ")
                        part_name = ""
        submitted = st.form_submit_button("✅ إضافة الحدث", type="primary")
        if submitted:
            spare_part_used = ""
            if part_name and consume_qty>0:
                success, msg, _ = consume_spare_part(part_name, consume_qty)
                if success:
                    spare_part_used = f"{part_name} (كمية {consume_qty})"
                else:
                    st.error(msg)
                    return sheets_edit
            image_url = None
            if uploaded_image:
                image_url = upload_image(uploaded_image, "event", str(uuid.uuid4())[:8])
            new_row = {
                "مده الاصلاح": repair_duration,
                "التاريخ": event_date.strftime("%Y-%m-%d"),
                "المعدة": selected_equipment,
                "الحدث/العطل": event_desc,
                "الإجراء التصحيحي": correction_desc,
                "تم بواسطة": servised_by,
                "قطع غيار مستخدمة": spare_part_used,
                "نوع العطل": fault_type,
                "قدرة الفني (حل/تفكير/مبادرة/قرار)": technician_rating,
                "الالتزام بتعليمات السلامة": safety_compliance,
                "رابط الصورة": image_url or ""
            }
            for col in df.columns:
                if col not in new_row:
                    new_row[col] = ""
            new_row_df = pd.DataFrame([new_row])
            df_new = pd.concat([df, new_row_df], ignore_index=True)
            sheets_edit[sheet_name] = df_new
            # حفظ في Firebase
            save_section(sheet_name, df_new)
            # حفظ قطع الغيار إذا تغيرت
            if "temp_spare_parts_df" in st.session_state:
                save_spare_parts(st.session_state.temp_spare_parts_df)
                del st.session_state.temp_spare_parts_df
            # نسخة احتياطية على GitHub
            save_and_push_to_github(sheets_edit, f"إضافة حدث عطل مع استخدام قطعة {part_name}")
            st.success("✅ تم إضافة الحدث بنجاح!")
            st.rerun()
    return sheets_edit

def execute_maintenance_with_date(sheets_edit, equipment_name, task_name, execution_date, performed_by, used_spare_part="", used_quantity=1, image_url=None):
    df = load_maintenance_tasks()
    if df.empty:
        return False, "لا توجد مهام صيانة"
    mask = (df["المعدة"] == equipment_name) & (df["اسم_البند"] == task_name)
    if not mask.any():
        return False, f"المهمة '{task_name}' غير موجودة"
    idx = df[mask].index[0]
    period_days = df.loc[idx, "الفترة_بالأيام"]
    df.loc[idx, "آخر_تنفيذ"] = pd.to_datetime(execution_date)
    next_date = execution_date + timedelta(days=period_days)
    df.loc[idx, "التاريخ_التالي"] = next_date
    old_notes = df.loc[idx, "ملاحظات"] if pd.notna(df.loc[idx, "ملاحظات"]) else ""
    new_entry = f"{execution_date.strftime('%Y-%m-%d')} | تم بواسطة: {performed_by}"
    warning_msg = ""
    if used_spare_part and used_quantity>0:
        success, msg, new_qty = consume_spare_part(used_spare_part, used_quantity)
        if not success:
            return False, msg
        new_entry += f" | استخدمت {used_spare_part} كمية {used_quantity}"
    if image_url:
        new_entry += f" | صورة: {image_url}"
    df.loc[idx, "ملاحظات"] = (old_notes + "\n" + new_entry).strip()
    save_maintenance_tasks(df)
    log_activity("execute_maintenance", f"تم تنفيذ صيانة '{task_name}' لـ {equipment_name}")
    return True, f"تم تنفيذ الصيانة. التاريخ التالي: {next_date.strftime('%Y-%m-%d')}"

def add_maintenance_as_event(sheets_edit, equipment_name, task_name, execution_date, performed_by, used_spare_part="", used_quantity=1, image_url=None):
    target_sheet = None
    for sheet_name, df in sheets_edit.items():
        if sheet_name not in [APP_CONFIG["SPARE_PARTS_SHEET"], APP_CONFIG["MAINTENANCE_SHEET"]]:
            if equipment_name in get_equipment_list_from_sheet(df):
                target_sheet = sheet_name
                break
    if target_sheet is None:
        return False, f"لم يتم العثور على قسم يحتوي على المعدة '{equipment_name}'"
    spare_part_used = f"{used_spare_part} (كمية {used_quantity})" if used_spare_part else ""
    new_row = {
        "مده الاصلاح": 0,
        "التاريخ": execution_date.strftime("%Y-%m-%d"),
        "المعدة": equipment_name,
        "الحدث/العطل": f"صيانة وقائية: {task_name}",
        "الإجراء التصحيحي": f"تم تنفيذ الصيانة الدورية '{task_name}' بواسطة {performed_by}",
        "تم بواسطة": performed_by,
        "قطع غيار مستخدمة": spare_part_used,
        "نوع العطل": "صيانة وقائية",
        "قدرة الفني (حل/تفكير/مبادرة/قرار)": 5,
        "الالتزام بتعليمات السلامة": "ملتزم بالكامل",
        "رابط الصورة": image_url or ""
    }
    df = sheets_edit[target_sheet]
    for col in df.columns:
        if col not in new_row:
            new_row[col] = ""
    new_row_df = pd.DataFrame([new_row])
    sheets_edit[target_sheet] = pd.concat([df, new_row_df], ignore_index=True)
    save_section(target_sheet, sheets_edit[target_sheet])
    return True, f"تم تسجيل الصيانة كحدث في قسم '{target_sheet}'"

def manage_spare_parts_tab(sheets_edit):
    st.header("📦 إدارة قطع الغيار")
    username = st.session_state.get("username")
    all_sheets = load_all_sections()
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
    if filtered_df.empty:
        st.info(f"لا توجد قطع غيار مسجلة للقسم '{selected_section}'.")
    else:
        if view_mode == "جدول":
            st.dataframe(filtered_df, use_container_width=True)
            # إضافة تعديل/حذف (مشابه للكود القديم لكن باستخدام save_spare_parts)
        else:
            # عرض بطاقات مع الصور
            pass
    with st.form(key="add_spare_part_form"):
        # نفس الكود القديم لإنشاء قطعة جديدة
        pass
    return sheets_edit

def preventive_maintenance_tab(sheets_edit):
    st.header("🛠 الصيانة الوقائية")
    username = st.session_state.get("username")
    all_sheets = load_all_sections()
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
    tasks_df = load_maintenance_tasks()
    tasks_df = tasks_df[tasks_df["المعدة"] == selected_equipment].copy()
    st.subheader(f"📋 بنود الصيانة لـ {selected_equipment}")
    if not tasks_df.empty:
        st.dataframe(tasks_df, use_container_width=True)
        # تنفيذ صيانة
        selected_task = st.selectbox("اختر البند المنفذ:", tasks_df["اسم_البند"].tolist(), key="execute_task_select")
        if selected_task:
            execution_date = st.date_input("📅 تاريخ التنفيذ:", value=datetime.now().date())
            performed_by = st.text_input("👨‍🔧 تم بواسطة:")
            if st.button("✅ تم تنفيذ الصيانة"):
                success, msg = execute_maintenance_with_date(sheets_edit, selected_equipment, selected_task, execution_date, performed_by)
                if success:
                    st.success(msg)
                    st.rerun()
                else:
                    st.error(msg)
    # إضافة بند صيانة جديد
    with st.form(key="add_maintenance_form"):
        task_name = st.text_input("اسم البند:")
        period_hours = st.number_input("⏱️ عدد الساعات بين الصيانة:", min_value=1, value=24)
        if st.form_submit_button("➕ إضافة بند صيانة"):
            if task_name:
                sheets_edit = add_maintenance_task(sheets_edit, selected_equipment, task_name, period_hours)
                st.success("تم الإضافة")
                st.rerun()
    return sheets_edit

def manage_data_edit(sheets_edit):
    if sheets_edit is None:
        st.warning("الملف غير موجود. استخدم زر 'تحديث' أولاً")
        return sheets_edit
    tab_names = ["📋 عرض الأقسام", "🔧 إدارة الماكينات", "➕ إضافة قسم جديد", "📦 قطع الغيار", "🛠 الصيانة الوقائية"]
    tabs_edit = st.tabs(tab_names)
    with tabs_edit[0]:
        st.subheader("جميع الأقسام")
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
                            save_section(dept_name, sheets_edit[dept_name])
                            save_and_push_to_github(sheets_edit, f"تعديل بيانات قسم {dept_name}")
                            st.success("تم الحفظ")
                            st.rerun()
    with tabs_edit[1]:
        sheet_name = st.selectbox("اختر القسم:", [name for name in sheets_edit.keys() if name not in [APP_CONFIG["SPARE_PARTS_SHEET"], APP_CONFIG["MAINTENANCE_SHEET"]]], key="manage_machines_sheet_edit")
        manage_machines(sheets_edit, sheet_name, unique_suffix="edit")
    with tabs_edit[2]:
        sheets_edit = add_new_department(sheets_edit)
    with tabs_edit[3]:
        sheets_edit = manage_spare_parts_tab(sheets_edit)
    with tabs_edit[4]:
        sheets_edit = preventive_maintenance_tab(sheets_edit)
    return sheets_edit

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
        if st.button("🔄 تحديث"):
            # إعادة تحميل البيانات من Firebase
            st.cache_data.clear()
            st.rerun()
        if st.button("🚪 تسجيل الخروج"):
            logout_action()

all_sheets = load_all_sections()
sheets_edit = all_sheets.copy()  # نسخة للتحرير
st.title(f"{APP_CONFIG['APP_ICON']} {APP_CONFIG['APP_TITLE']}")
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

with tabs[0]:
    search_across_sheets(all_sheets)
with tabs[1]:
    failures_analysis_tab(all_sheets)
with tabs[2]:
    st.header("🔔 الإشعارات")
    # نفس الكود القديم للإشعارات
    col1, col2 = st.columns(2)
    with col1:
        st.subheader("⚠️ قطع غيار حرجة")
        critical = get_critical_spare_parts()
        if critical:
            for part in critical:
                st.error(f"🔴 **{part['اسم القطعة']}** - الرصيد: {part['الرصيد الموجود']}")
        else:
            st.success("✅ لا توجد قطع غيار حرجة")
    with col2:
        st.subheader("🔧 صيانة مستحقة")
        overdue, upcoming = get_upcoming_maintenance(3)
        if not overdue.empty:
            st.warning("🟡 صيانة متأخرة")
            for _, row in overdue.iterrows():
                st.write(f"- {row['المعدة']}: {row['اسم_البند']}")
        else:
            st.info("✅ لا توجد صيانات متأخرة")

if can_add_event:
    idx_add = 3
    with tabs[idx_add]:
        if sheets_edit:
            allowed_for_add = [s for s in sheets_edit.keys() if s not in [APP_CONFIG["SPARE_PARTS_SHEET"], APP_CONFIG["MAINTENANCE_SHEET"]] and has_section_permission(username, s, "add_event")]
            if allowed_for_add:
                sheet_name = st.selectbox("اختر القسم:", allowed_for_add, key="add_event_sheet_main")
                sheets_edit = add_new_event(sheets_edit, sheet_name)
            else:
                st.warning("لا توجد أقسام مسموح لك بإضافة أحداث فيها.")
if can_manage_machines:
    idx_manage = 3 + (1 if can_add_event else 0)
    with tabs[idx_manage]:
        if sheets_edit:
            allowed_for_machines = [s for s in sheets_edit.keys() if s not in [APP_CONFIG["SPARE_PARTS_SHEET"], APP_CONFIG["MAINTENANCE_SHEET"]] and has_section_permission(username, s, "manage_machines")]
            if allowed_for_machines:
                sheet_name = st.selectbox("اختر القسم:", allowed_for_machines, key="manage_machines_sheet_main")
                manage_machines(sheets_edit, sheet_name, unique_suffix="main")
            else:
                st.warning("لا توجد أقسام مسموح لك بإدارة الماكينات فيها.")
if can_edit_data:
    idx_edit = 3 + (1 if can_add_event else 0) + (1 if can_manage_machines else 0)
    with tabs[idx_edit]:
        sheets_edit = manage_data_edit(sheets_edit)

with tabs[-1]:
    # تبويب الدعم الفني (نفس الكود السابق)
    st.header("📞 الدعم الفني")
    st.markdown("### تم تصميم وتنفيذ هذا السيستم بواسطه **م.محمد عبدالله**")
    st.markdown("#### رئيس قسم المحطات والتحضيرات بمصنع بيل يارن1")
    st.markdown("---")
    st.markdown("📧 **للتواصل والدعم الفني:** `01274424062`")
    st.markdown("---")
    YOUTUBE_LINK = "https://youtube.com/@cardtrutchler?si=bayhxhRXgCzWSpCl"
    st.markdown(f"[📺 قناة اليوتيوب الرسمية]({YOUTUBE_LINK})")
    st.markdown("---")
    support_config = load_support_config()
    current_image_url = support_config.get("image_url", "")
    if current_image_url:
        try:
            st.image(current_image_url, use_container_width=True)
        except:
            st.warning("⚠️ تعذر عرض الصورة")
    else:
        if st.session_state.get("username") == "admin":
            uploaded_img = st.file_uploader("رفع صورة للمطور", type=APP_CONFIG["ALLOWED_IMAGE_TYPES"])
            if uploaded_img:
                url = upload_image(uploaded_img, "support", "developer")
                if url:
                    support_config["image_url"] = url
                    save_support_config(support_config)
                    st.success("تم رفع الصورة")
                    st.rerun()
        else:
            st.info("لم يتم رفع صورة المطور بعد.")



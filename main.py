import re
import os
import time
import hashlib
import urllib.request as _urlreq
from dotenv import load_dotenv

from fastapi import FastAPI, Request, Form
from fastapi.templating import Jinja2Templates
from fastapi.responses import RedirectResponse, JSONResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from Bio import Entrez
from openai import OpenAI
from datetime import datetime, timedelta
from fastapi import FastAPI, Request, Query
from db import (
    init_db,
    init_memos_tables,
    save_paper,
    get_saved_papers,
    get_public_papers,
    get_saved_paper_by_id,
    get_saved_papers_by_folder,
    toggle_favorite,
    add_like,
    toggle_public,
    create_user,
    verify_user,
    get_user_by_id,
    get_folder_name_suggestions,
    count_user_saved_papers,
    update_user_plan,
    get_user_by_ref_code,
    apply_referral_bonus,
    set_trial_extend_days,
    update_saved_paper_folder,
    update_saved_paper_custom_title,
    update_saved_paper_user_note,
    rename_folder,
    update_user_profile,
    get_user_daily_usage,
    count_user_all_memos,
    get_user_memos,
    get_memo_by_id,
    create_memo,
    update_memo,
    delete_memo,
    get_user_paper_memos,
    get_paper_memo_by_id,
    create_paper_memo,
    update_paper_memo,
    delete_paper_memo,
    create_post,
    get_posts,
    get_replies,
    toggle_post_like,
    delete_post,
    update_saved_paper_highlights,
    toggle_paper_like,
    get_paper_liked,
    update_memo_tags,
    update_paper_memo_tags,
    get_paper_jp_title_global,
    get_user_tags,
    upsert_user_tag,
    delete_user_tag,
    record_interest,
    get_recommended_papers,
    get_friend_promo_code,
    use_friend_promo_code,
    apply_promo_to_user,
)
from starlette.middleware.sessions import SessionMiddleware

load_dotenv()

app = FastAPI()
app.add_middleware(SessionMiddleware, secret_key=os.getenv("SESSION_SECRET"))
templates = Jinja2Templates(directory="templates")
if os.path.isdir("static"):
    app.mount("/static", StaticFiles(directory="static"), name="static")

Entrez.email = os.getenv("ENTREZ_EMAIL")
client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
JAPANESE_MEDICAL_KEYWORDS = {
    "脳卒中": "stroke",
    "脳梗塞": "cerebral infarction",
    "脳出血": "intracerebral hemorrhage",
    "くも膜下出血": "subarachnoid hemorrhage",
    "リハビリ": "rehabilitation",
    "理学療法": "physical therapy",
    "作業療法": "occupational therapy",
    "言語療法": "speech therapy",
    "歩行": "gait",
    "歩行訓練": "gait training",
    "バランス": "balance",
    "立位": "standing",
    "座位": "sitting",
    "起立": "sit to stand",
    "移乗": "transfer",
    "上肢": "upper limb",
    "下肢": "lower limb",
    "手": "hand",
    "腕": "arm",
    "肩": "shoulder",
    "肘": "elbow",
    "膝": "knee",
    "足関節": "ankle",
    "麻痺": "paralysis",
    "片麻痺": "hemiplegia",
    "両麻痺": "diplegia",
    "筋力": "muscle strength",
    "筋力低下": "muscle weakness",
    "持久力": "endurance",
    "可動域": "range of motion",
    "痙縮": "spasticity",
    "拘縮": "contracture",
    "疼痛": "pain",
    "慢性疼痛": "chronic pain",
    "しびれ": "numbness",
    "感覚障害": "sensory impairment",
    "高次脳機能": "higher brain function",
    "注意障害": "attention disorder",
    "失語": "aphasia",
    "嚥下": "swallowing",
    "嚥下障害": "dysphagia",
    "認知": "cognition",
    "認知障害": "cognitive impairment",
    "うつ": "depression",
    "予後": "prognosis",
    "目標設定": "goal setting",
    "退院": "discharge",
    "在宅": "home discharge",
    "日常生活動作": "activities of daily living",
    "ADL": "activities of daily living",
    "IADL": "instrumental activities of daily living",
    "転倒": "fall",
    "転倒予防": "fall prevention",
    "脊髄損傷": "spinal cord injury",
    "脳性麻痺": "cerebral palsy",
    "パーキンソン病": "Parkinson disease",
    "整形": "orthopedics",
    "変形性膝関節症": "knee osteoarthritis",
    "股関節": "hip joint",
    "骨折": "fracture",
    "大腿骨近位部骨折": "hip fracture",
    "人工膝関節": "total knee arthroplasty",
    "人工股関節": "total hip arthroplasty",
    "呼吸": "respiration",
    "心肺": "cardiopulmonary",
    "心不全": "heart failure",
    "呼吸リハビリ": "pulmonary rehabilitation",
    "運動療法": "exercise therapy",
    "介入": "intervention",
    "訓練": "training",
    "評価": "assessment",
    "尺度": "scale",
    "ランダム化比較試験": "randomized controlled trial",
    "RCT": "randomized controlled trial",
    "症例報告": "case report",
    "コホート": "cohort",
    "メタアナリシス": "meta-analysis",
    "システマティックレビュー": "systematic review",
}


@app.on_event("startup")
def startup_event():
    init_db()
    init_memos_tables()

def get_current_user(request: Request):
    user_id = request.session.get("user_id")

    if not user_id:
        return None

    return get_user_by_id(user_id)

def get_user_plan(user):
    if not user:
        return "guest"

    today = datetime.now().date()

    # ① promo が有効なら最優先（users.plan は free のまま）
    promo_ends_at = (user.get("promo_ends_at") or "").strip()
    promo_plan = (user.get("promo_plan") or "").strip().lower()
    if promo_ends_at and promo_plan in ("pro", "expert"):
        try:
            end = datetime.strptime(promo_ends_at, "%Y-%m-%d").date()
            if today <= end:
                return promo_plan
        except ValueError:
            pass

    # ② 通常 plan（trial 中も含む）
    plan = (user.get("plan") or "free").strip().lower()
    return plan if plan in ("free", "pro", "expert") else "free"

def get_plan_limits(plan):

    limits = {
        "guest": {
            "daily_summary_limit": 5,
            "save_limit": 0,
            "ranking_limit": 20,
            "can_view_score_ranking": True,
            "memo_limit": 0,
        },

        "free": {
            "daily_summary_limit": 5,
            "save_limit": 10,
            "ranking_limit": 20,
            "can_view_score_ranking": True,
            "memo_limit": 15,
        },

        "pro": {
            "daily_summary_limit": 50,
            "save_limit": 300,
            "ranking_limit": 100,
            "can_view_score_ranking": True,
            "memo_limit": None,
        },

        "expert": {
            "daily_summary_limit": None,
            "save_limit": None,
            "ranking_limit": 300,
            "can_view_score_ranking": True,
            "memo_limit": None,
        },
    }

    if not plan:
        return limits["guest"]

    return limits.get(plan, limits["free"])


@app.post("/set-plan")
def set_plan(request: Request, plan: str = Form(...)):
    current_user = get_current_user(request)

    if not current_user:
        return RedirectResponse("/login", status_code=303)

    user = get_user_by_id(current_user["id"])
    plan = (plan or "").strip().lower()

    if plan not in ["free", "pro", "expert"]:
        return RedirectResponse("/plans", status_code=303)

    today = datetime.now()

    base_trial_days = 7
    bonus_days = int(user.get("trial_extend_days") or 0)
    trial_used = int(user.get("trial_used") or 0)

    if plan == "free":
        update_user_plan(
            user_id=current_user["id"],
            plan="free",
            trial_ends_at="",
            plan_started_at=today.strftime("%Y-%m-%d"),
            plan_renews_at="",
            is_yearly=0,
            trial_used=trial_used,
        )
        return RedirectResponse("/plans", status_code=303)

    total_trial_days = 0

    if trial_used:
        total_trial_days = bonus_days
    else:
        total_trial_days = base_trial_days + bonus_days

    trial_end = ""
    renew_at = ""

    if total_trial_days > 0:
        trial_end_date = today + timedelta(days=total_trial_days)
        trial_end = trial_end_date.strftime("%Y-%m-%d")
        renew_at = trial_end
    else:
        renew_date = today + timedelta(days=30)
        renew_at = renew_date.strftime("%Y-%m-%d")

    update_user_plan(
        user_id=current_user["id"],
        plan=plan,
        trial_ends_at=trial_end,
        plan_started_at=today.strftime("%Y-%m-%d"),
        plan_renews_at=renew_at,
        is_yearly=0,
        trial_used=1,
    )

    if bonus_days > 0:
        set_trial_extend_days(current_user["id"], 0)

    return RedirectResponse("/plans", status_code=303)

    conn_user = get_user_by_id(current_user["id"])
    if conn_user and int(conn_user.get("trial_extend_days") or 0) > 0:
        conn = sqlite3.connect("papers.db")
        cur = conn.cursor()
        cur.execute("""
            UPDATE users
            SET trial_extend_days = 0
            WHERE id = ?
        """, (current_user["id"],))
        conn.commit()
        conn.close()

    return RedirectResponse("/plans", status_code=303)

@app.post("/apply-referral")
def apply_referral(request: Request, ref_code: str = Form(...)):
    current_user = get_current_user(request)

    if not current_user:
        return RedirectResponse("/login", status_code=303)

    ref_code = (ref_code or "").strip().upper()

    if not ref_code:
        return RedirectResponse("/plans?ref_error=empty", status_code=303)

    referrer = get_user_by_ref_code(ref_code)

    if not referrer:
        return RedirectResponse("/plans?ref_error=not_found", status_code=303)

    ok, reason = apply_referral_bonus(
        referrer_id=referrer["id"],
        referred_user_id=current_user["id"]
    )

    if not ok:
        return RedirectResponse(f"/plans?ref_error={reason}", status_code=303)

    today = datetime.now()
    trial_end = today + timedelta(days=7)

    update_user_plan(
        user_id=current_user["id"],
        plan="pro",
        trial_ends_at=trial_end.strftime("%Y-%m-%d"),
        plan_started_at=today.strftime("%Y-%m-%d"),
        plan_renews_at=trial_end.strftime("%Y-%m-%d"),
        is_yearly=0,
        trial_used=1,
    )

    return RedirectResponse("/plans?ref_success=1", status_code=303)


# promo_error の値一覧:
#   login_required  - 未ログイン
#   empty           - コードが空
#   already_used    - このユーザーは既にプロモを利用済み
#   already_pro     - 実効プランが pro/expert（trial中・promo中を含む）
#   not_found       - コードが存在しない
#   inactive        - コードが無効化されている
#   expired         - コード自体の有効期限切れ
#   limit_reached   - max_uses を超過
#   email_mismatch  - target_email と一致しない

@app.post("/apply-promo")
def apply_promo(request: Request, promo_code: str = Form(...)):
    current_user = get_current_user(request)
    if not current_user:
        return RedirectResponse("/login?from=plans", status_code=303)

    promo_code = (promo_code or "").strip().upper()
    if not promo_code:
        return RedirectResponse("/plans?promo_error=empty", status_code=303)

    user = get_user_by_id(current_user["id"])

    # 既にプロモを利用済み
    if (user.get("promo_code_used") or "").strip():
        return RedirectResponse("/plans?promo_error=already_used", status_code=303)

    # 実効プランが pro/expert（trial中・promo中を含む）は対象外
    effective_plan = get_user_plan(user)
    if effective_plan in ("pro", "expert"):
        return RedirectResponse("/plans?promo_error=already_pro", status_code=303)

    code_row = get_friend_promo_code(promo_code)
    if not code_row:
        return RedirectResponse("/plans?promo_error=not_found", status_code=303)

    if not code_row["is_active"]:
        return RedirectResponse("/plans?promo_error=inactive", status_code=303)

    # コード自体の有効期限チェック
    expires_at = (code_row.get("expires_at") or "").strip()
    if expires_at:
        today = datetime.now().date()
        try:
            exp = datetime.strptime(expires_at, "%Y-%m-%d").date()
            if today > exp:
                return RedirectResponse("/plans?promo_error=expired", status_code=303)
        except ValueError:
            pass

    if code_row["used_count"] >= code_row["max_uses"]:
        return RedirectResponse("/plans?promo_error=limit_reached", status_code=303)

    # target_email が設定されている場合は一致確認
    target_email = (code_row.get("target_email") or "").strip().lower()
    if target_email and target_email != (user.get("email") or "").strip().lower():
        return RedirectResponse("/plans?promo_error=email_mismatch", status_code=303)

    # 適用
    today = datetime.now()
    ends_at = (today + timedelta(days=int(code_row["free_days"]))).strftime("%Y-%m-%d")
    plan_to_grant = code_row.get("plan_to_grant") or "pro"

    apply_promo_to_user(
        user_id=current_user["id"],
        plan=plan_to_grant,
        ends_at=ends_at,
        code=promo_code,
    )
    use_friend_promo_code(code_row["id"])

    return RedirectResponse("/plans?promo_success=1", status_code=303)


def can_user_save(user):
    plan = get_user_plan(user)
    limits = get_plan_limits(plan)

    return limits["save_limit"] != 0

def convert_japanese_keyword_to_english(keyword: str) -> str:
    converted = keyword

    for ja_word, en_word in JAPANESE_MEDICAL_KEYWORDS.items():
        converted = converted.replace(ja_word, en_word)

    converted = " ".join(converted.split())
    return converted


def contains_japanese(text: str) -> bool:
    return re.search(r"[ぁ-んァ-ン一-龥]", text) is not None

keyword_cache = {}

# ─── PubMed トレンドキャッシュ ────────────────────────────────────
_trending_cache: dict = {"papers": None, "ts": 0.0, "fetching": False}
TRENDING_CACHE_TTL = 10800  # 3 hours


def _fetch_trending_pmids(limit: int = 20) -> list:
    """PubMed Trending ページから PMID を抽出する"""
    try:
        req = _urlreq.Request(
            "https://pubmed.ncbi.nlm.nih.gov/trending/",
            headers={"User-Agent": "Mozilla/5.0 (compatible; research-tool/1.0)"},
        )
        with _urlreq.urlopen(req, timeout=10) as resp:
            html = resp.read().decode("utf-8", errors="ignore")
        ids = re.findall(r'data-article-id="(\d+)"', html)
        if not ids:
            ids = re.findall(r'href="/(\d{6,9})/"', html)
        if not ids:
            ids = re.findall(r'"docsum-pmid">(\d+)', html)
        return list(dict.fromkeys(ids))[:limit]
    except Exception:
        return []


def _fetch_trending_papers_bg(limit: int = 20):
    """バックグラウンドでトレンド論文を取得してキャッシュに保存"""
    try:
        now = time.time()
        pmids = _fetch_trending_pmids(limit)
        if not pmids:
            _trending_cache.update({"papers": [], "ts": now, "fetching": False})
            return

        saved_map = {p["pubmed_id"]: p for p in get_saved_papers()}
        missing = [pid for pid in pmids if pid not in saved_map]

        entrez_map: dict = {}
        if missing:
            try:
                handle = Entrez.efetch(db="pubmed", id=",".join(missing), retmode="xml")
                records = Entrez.read(handle)
                handle.close()

                for article in records.get("PubmedArticle", []):
                    try:
                        citation = article.get("MedlineCitation", {})
                        pmid = str(citation.get("PMID", ""))
                        article_data = citation.get("Article", {})
                        raw_title = str(article_data.get("ArticleTitle", ""))

                        raw_journal = ""
                        raw_pubdate = ""
                        if "Journal" in article_data:
                            raw_journal = str(article_data["Journal"].get("Title", ""))
                            issue = article_data["Journal"].get("JournalIssue", {})
                            pub = issue.get("PubDate", {})
                            year = pub.get("Year", "")
                            month = pub.get("Month", "")
                            raw_pubdate = f"{year} {month}".strip()

                        authors = []
                        if "AuthorList" in article_data:
                            for a in article_data["AuthorList"]:
                                full = f"{a.get('LastName','')} {a.get('ForeName','')}".strip()
                                if full:
                                    authors.append(full)

                        jp_title = ""
                        try:
                            jp_title = translate_title_to_japanese(raw_title) if raw_title else ""
                        except Exception:
                            jp_title = raw_title

                        entrez_map[pmid] = {
                            "pubmed_id": pmid,
                            "title": raw_title,
                            "jp_title": jp_title,
                            "authors": ", ".join(authors),
                            "journal": raw_journal,
                            "pubdate": raw_pubdate,
                            "likes": 0,
                            "clinical_score": "",
                            "folder_name": "",
                            "summary_jp": "",
                        }
                    except Exception:
                        continue
            except Exception:
                pass

        papers = []
        for pid in pmids:
            if pid in saved_map:
                papers.append(dict(saved_map[pid]))
            elif pid in entrez_map:
                papers.append(entrez_map[pid])

        _trending_cache.update({"papers": papers, "ts": now, "fetching": False})
    except Exception:
        _trending_cache["fetching"] = False


def get_pubmed_trending_papers(limit: int = 20) -> list:
    """PubMed トレンド論文を取得（キャッシュ付き・非ブロッキング）"""
    now = time.time()
    # キャッシュが有効ならそのまま返す
    if _trending_cache["papers"] is not None and (now - _trending_cache["ts"]) < TRENDING_CACHE_TTL:
        return _trending_cache["papers"]

    # キャッシュ切れまたは未取得 — バックグラウンドで取得開始し即返す
    if not _trending_cache["fetching"]:
        _trending_cache["fetching"] = True
        import threading
        threading.Thread(target=_fetch_trending_papers_bg, args=(limit,), daemon=True).start()

    # キャッシュが古くてもあれば返す、なければ空リスト
    return _trending_cache["papers"] if _trending_cache["papers"] is not None else []

def stable_score_offset(pubmed_id: str) -> float:
    seed = hashlib.md5(pubmed_id.encode("utf-8")).hexdigest()
    value = int(seed[:8], 16) / 0xFFFFFFFF
    return (value * 0.16) - 0.08

def convert_keyword_with_gpt_if_needed(keyword: str) -> str:
    converted = convert_japanese_keyword_to_english(keyword)

    if not contains_japanese(converted):
        return converted

    try:
        response = client.responses.create(
            model="gpt-4.1-mini",
            input=f"""
次の検索キーワードを、PubMed検索に適した英語の医学検索語へ変換してください。

条件:
- 出力は英語の検索語のみ
- 余計な説明は不要
- 単語をスペース区切りで出す
- 医学・リハビリ分野として自然な語を使う
- もとの意味を変えない

検索キーワード:
{keyword}
"""
        )

        gpt_keyword = response.output_text.strip()
        return " ".join(gpt_keyword.split())

    except Exception:
        return converted
    
def generate_tags(title: str, abstract: str):
    text = f"{title} {abstract}".lower()
    scored: list[tuple[str, int]] = []

    # ===== Tier 1 — 疾患・領域 (score 3) =====
    if any(k in text for k in ["stroke", "cerebral infarction", "cerebral hemorrhage",
                                "hemiplegia", "spinal cord injury", "brain injury",
                                "traumatic brain", " tbi ", "myelopathy"]):
        scored.append(("中枢", 3))

    if any(k in text for k in ["parkinson", "alzheimer", "multiple sclerosis",
                                "amyotrophic", "guillain", "peripheral neuropath",
                                "cranial nerve", "neurodegenerative"]):
        scored.append(("脳神経", 3))

    if any(k in text for k in ["orthopedic", "musculoskeletal", "lumbar", "cervical spine",
                                "arthroplasty", "scoliosis", "spine surgery", "knee osteoarthritis",
                                "knee oa", "total knee", "total hip"]):
        scored.append(("整形", 3))

    if any(k in text for k in ["fracture", "osteoporosis", "bone mineral density",
                                "hip fracture", "vertebral fracture", "bone density"]):
        scored.append(("骨折", 3))

    if any(k in text for k in ["respiratory", "copd", "pulmonary", " lung ", "ventilator",
                                "mechanical ventilation", "tracheostomy", "oxygen therapy",
                                "respiratory failure"]):
        scored.append(("呼吸器", 3))

    if any(k in text for k in ["cardiac", "heart failure", "coronary", "myocardial",
                                "atrial fibrillation", "cardiovascular", "hypertension",
                                "cardiac rehabilitation"]):
        scored.append(("循環器", 3))

    if any(k in text for k in ["diabetes", "diabetic", "glycemic", "hba1c", "insulin"]):
        scored.append(("糖尿病", 3))

    if any(k in text for k in ["renal", "kidney", "hemodialysis", "dialysis",
                                "chronic kidney disease", "ckd"]):
        scored.append(("腎臓リハ", 3))

    if any(k in text for k in ["elderly", "older adult", "older people", "frailty",
                                "frail", "sarcopenia", "aging", "geriatric", "older patient"]):
        scored.append(("高齢者・フレイル", 3))

    if any(k in text for k in ["pediatric", "children", "child", "infant",
                                "cerebral palsy", "developmental delay"]):
        scored.append(("小児リハ", 3))

    if any(k in text for k in ["athlete", " sport", "athletic", "acl",
                                "anterior cruciate", "sports injury"]):
        scored.append(("スポーツ", 3))

    # ===== Tier 2 — 症状・機能 (score 2) =====
    if any(k in text for k in ["gait", "walking", "balance", "postural stability",
                                "timed up and go", " tug ", "6-minute walk", "fall risk"]):
        scored.append(("歩行・バランス", 2))

    if any(k in text for k in ["upper extremity", "upper limb", " arm ", "hand function",
                                "wrist", "elbow", "shoulder function", "grip strength"]):
        scored.append(("上肢", 2))

    if any(k in text for k in ["adl", "activities of daily living", "functional independence",
                                " fim ", "barthel", "instrumental adl", "iadl"]):
        scored.append(("ADL・生活機能", 2))

    if any(k in text for k in ["swallowing", "dysphagia", "aspiration", "oral feeding",
                                "deglutition", "videofluoroscopy", "fiberoptic endoscopic"]):
        scored.append(("嚥下障害", 2))

    if any(k in text for k in ["pain", "chronic pain", "neuropathic pain", "low back pain",
                                " vas ", "numeric rating scale", "pain management"]):
        scored.append(("疼痛", 2))

    if any(k in text for k in ["fall ", "falls ", "falling", "fall prevention",
                                "fall incidence", "fall rate"]):
        scored.append(("転倒", 2))

    if any(k in text for k in ["deconditioning", "disuse", "immobilization", "bed rest",
                                "physical inactivity", "hospital-acquired deconditioning"]):
        scored.append(("廃用症候群", 2))

    if any(k in text for k in ["malnutrition", "undernutrition", "nutritional status",
                                " albumin", "weight loss", "muscle wasting", "cachexia"]):
        scored.append(("低栄養", 2))

    if any(k in text for k in ["nutritional intervention", "dietary supplement", "enteral",
                                "parenteral", "tube feeding", "nutritional support"]):
        scored.append(("栄養管理", 2))

    if any(k in text for k in ["contracture", "joint contracture", "stretching",
                                "positioning", "splinting", "passive range of motion"]):
        scored.append(("拘縮予防", 2))

    if any(k in text for k in ["early mobilization", "early ambulation", "bed mobility",
                                "sitting out of bed", "patient mobilization"]):
        scored.append(("離床", 2))

    if any(k in text for k in ["orthosis", "brace", "prosthesis", "ankle foot orthosis",
                                " afo ", "wheelchair", "assistive device"]):
        scored.append(("装具", 2))

    # ===== Tier 3 — 文脈・手法 (score 1) =====
    if any(k in text for k in ["randomized controlled", " rct ", "systematic review",
                                "meta-analysis", "cohort study", "observational study"]):
        scored.append(("評価・研究デザイン", 1))

    if any(k in text for k in ["risk management", "adverse event", "patient safety",
                                "complication", "monitoring"]):
        scored.append(("リスク管理", 1))

    if any(k in text for k in ["multidisciplinary", "interdisciplinary", "team approach",
                                "collaboration", "care team", "interprofessional"]):
        scored.append(("多職種連携", 1))

    if any(k in text for k in ["home visit", "home care", "outpatient", "discharge planning",
                                "community-dwelling", "community rehabilitation"]):
        scored.append(("訪問・地域", 1))

    if any(k in text for k in ["polypharmacy", "multiple medications",
                                "drug interaction", "deprescribing"]):
        scored.append(("ポリファーマシー", 1))

    if any(k in text for k in ["constipation", "bowel management", "defecation", "laxative"]):
        scored.append(("便秘", 1))

    if any(k in text for k in ["readmission", "rehospitalization",
                                "hospital readmission", "30-day readmission"]):
        scored.append(("再入院予防", 1))

    if any(k in text for k in ["blood test", "laboratory value", " serum ", " crp ",
                                "hemoglobin", "blood count", "biomarker"]):
        scored.append(("血液検査", 1))

    if any(k in text for k in [" mri ", " ct ", "imaging", "radiograph",
                                "ultrasound", "echocardiography"]):
        scored.append(("画像所見", 1))

    if any(k in text for k in ["cognitive", "cognition", "speech therapy", "aphasia",
                                "communication disorder", "memory", "dementia"]):
        scored.append(("嚥下・言語・認知", 1))

    # スコア降順でユニーク上位5件を返す
    seen: set[str] = set()
    result: list[str] = []
    for tag, _score in sorted(scored, key=lambda x: -x[1]):
        if tag not in seen:
            seen.add(tag)
            result.append(tag)
        if len(result) >= 5:
            break

    return result
    
def translate_title_to_japanese(title: str) -> str:
    if not title:
        return ""

    try:
        response = client.responses.create(
            model="gpt-4.1-mini",
            input=f"""
次の英語論文タイトルを、日本の医療職が一目で内容を理解できる自然な日本語タイトルに変換してください。

条件:
- 出力は日本語タイトルのみ
- 40〜55字程度を目安にする
- 何の患者・介入・評価かが分かる形を優先する
- 不自然な直訳を避ける
- 誇張しない
- 元の意味を変えない
- 余計な説明は書かない

英語タイトル:
{title}
"""
        )

        return response.output_text.strip()

    except Exception:
        return title


def summarize_abstract_in_japanese(text: str):
    if not text:
        return {
            "score": "0.0",
            "reason": "abstractがありません。",
            "summary": "abstractがありません。"
        }

    try:
        response = client.responses.create(
            model="gpt-4.1-mini",
            input=f"""
次の医学論文abstractを、日本の理学療法士・リハビリ職が臨床で使える形に日本語で整理してください。

必ず次の形式で出力してください。
項目名も完全に一致させてください。

【臨床参考度】
0.0〜5.0の範囲で、0.1刻みの数値だけを書く

【参考度の理由】
1〜2文で簡潔に書く

【結論】
研究から分かる最も重要な結果を簡潔に書く。
臨床でどう役立つかが分かる文章にする。

【臨床ポイント】
臨床応用できるポイントを箇条書きで書く。
最大3〜5個まで。
推測は書かない。abstractにある内容のみ。

【臨床目標設定の参考】
予後判断・目標設定に使える情報を書く。
対象患者、期間、効果量、重症度、年齢、訓練期間などを抽出する。
abstractに書かれている情報のみ使う。

【研究概要】
研究の目的と対象を2〜3文で簡潔にまとめる。

【方法】
研究デザイン、対象人数、期間、評価方法を簡潔に書く。

【限界】
サンプル数、対象の偏り、期間の短さなど、abstractに書かれている制限を書く。
明確な記載がなければ「記載なし」と書く。

臨床参考度は0.0〜5.0で厳密に評価する。平均的な論文は2.5〜3.0。以下の基準を使うこと：
・0.0〜1.0：症例報告（n<5）、abstractのみで内容不明、エビデンス皆無
・1.0〜2.0：小規模観察研究（n<20）、方法論が不明確、臨床適用困難
・2.0〜3.0：中規模研究（n=20〜50）、一定の方法論あり、限界が大きい
・3.0〜4.0：RCTまたは良質コホート（n≥30）、介入・評価が明確、一般化に制限あり
・4.0〜5.0：高品質RCT（n≥100）またはシステマティックレビュー・メタ解析、強いエビデンス

以下を総合的に判断すること：
・研究デザイン（RCT>コホート>横断>症例）
・対象人数（少ないほど低評価）
・介入と評価方法の明確さ
・臨床応用のしやすさ
・一般化のしやすさ
・限界の大きさ（大きいほど低評価）

ルール:
・日本語で書く
・簡潔に書く
・abstractに無い内容は書かない
・推測しない
・医学的に正確に書く
・各項目の順番を必ず守る
・見出しを必ず付ける

abstract:
{text}
"""
        )

        output_text = response.output_text.strip()

        score = "0.0"
        reason = ""
        cleaned_summary = output_text

        score_match = re.search(
            r"【臨床参考度】\s*(.*?)\s*【参考度の理由】",
            output_text,
            re.DOTALL
        )

        reason_match = re.search(
            r"【参考度の理由】\s*(.*?)\s*(【結論】.*)",
            output_text,
            re.DOTALL
        )

        if score_match:
            score = score_match.group(1).strip().splitlines()[0].strip()

        if reason_match:
            reason = reason_match.group(1).strip()
            cleaned_summary = reason_match.group(2).strip()

        return {
            "score": score,
            "reason": reason,
            "summary": cleaned_summary
        }

    except Exception as e:
        return {
            "score": "0.0",
            "reason": f"要約エラー: {str(e)}",
            "summary": f"要約エラー: {str(e)}"
        }


@app.get("/")
def root(request: Request):

    current_user = get_current_user(request)

    if current_user:

        user = get_user_by_id(current_user["id"])

        user = check_trial_expired(user)

        current_user = user

    plan = get_user_plan(current_user)
    limits = get_plan_limits(plan)

    papers = get_saved_papers()

    # ホームページでは翻訳APIを呼ばない（速度優先）
    # jp_titleがない場合は英語titleをそのまま使う
    updated_papers = []
    for paper in papers:
        jp_title = paper.get("jp_title") or ""
        if not jp_title:
            paper["jp_title"] = paper.get("title") or ""
        updated_papers.append(paper)

    # 人気ランキング

    sorted_popular = sorted(
        updated_papers,
        key=lambda x: int(x.get("likes") or 0),
        reverse=True
    )

    limit = limits["ranking_limit"]

    popular_papers = sorted_popular[:limit]

    # 臨床参考度ランキング

    sorted_score = sorted(
        updated_papers,
        key=lambda x: float(x.get("clinical_score") or 0),
        reverse=True
    )

    top_rated_papers = sorted_score[:limit]

    try:
        trending_papers = get_pubmed_trending_papers(20)
    except Exception:
        trending_papers = []

    return templates.TemplateResponse(
        "index.html",
        {
            "request": request,
            "popular_papers": popular_papers,
            "top_rated_papers": top_rated_papers,
            "trending_papers": trending_papers,
            "current_user": current_user,
            "current_plan": plan,
            "can_view_score_ranking": limits["can_view_score_ranking"],
            "ranking_limit": limits["ranking_limit"],
        }
    )

@app.get("/plans")
def plans(request: Request):

    current_user = get_current_user(request)

    if not current_user:
        return templates.TemplateResponse(
            "plans.html",
            {
                "request": request,
                "current_user": None,
                "current_plan": "guest",
                "promo_days_left": None,
                "promo_message": "",
                "promo_is_success": False,
                "referral_message": "",
                "referral_success": False,
            }
        )

    user = get_user_by_id(current_user["id"])
    user = check_trial_expired(user)

    current_plan = get_user_plan(user)

    trial_ends_at = user.get("trial_ends_at")
    plan_renews_at = user.get("plan_renews_at")
    ref_code = user.get("ref_code")

    trial_days_left = None

    if trial_ends_at:

        today = datetime.now().date()
        end = datetime.strptime(trial_ends_at, "%Y-%m-%d").date()

        diff = (end - today).days

        if diff > 0:
            trial_days_left = diff

    # promo の残り日数
    promo_days_left = None
    promo_ends_at = (user.get("promo_ends_at") or "").strip()
    if promo_ends_at:
        today = datetime.now().date()
        try:
            end = datetime.strptime(promo_ends_at, "%Y-%m-%d").date()
            diff = (end - today).days
            if diff >= 0:
                promo_days_left = diff
        except ValueError:
            pass

    # referral フィードバック
    ref_error = request.query_params.get("ref_error", "")
    ref_success = request.query_params.get("ref_success", "")
    ref_error_messages = {
        "empty":        "紹介コードを入力してください。",
        "not_found":    "紹介コードが見つかりません。",
        "already_used": "既に紹介コードを使用済みです。",
        "self_referral":"自分の紹介コードは使えません。",
    }
    referral_message = ref_error_messages.get(ref_error, "") if ref_error else ("紹介コードを適用しました！" if ref_success else "")
    referral_success = bool(ref_success)

    # promo フィードバック
    promo_error = request.query_params.get("promo_error", "")
    promo_success_param = request.query_params.get("promo_success", "")
    promo_error_messages = {
        "empty":         "招待コードを入力してください。",
        "already_used":  "招待コードは既に使用済みです。",
        "already_pro":   "現在 Pro / Expert プランのため適用できません。",
        "not_found":     "招待コードが見つかりません。",
        "inactive":      "このコードは現在無効です。",
        "expired":       "このコードの有効期限が切れています。",
        "limit_reached": "このコードの利用上限に達しています。",
        "email_mismatch":"このコードはあなたのアカウントでは使用できません。",
    }
    promo_message = promo_error_messages.get(promo_error, "") if promo_error else ("招待コードを適用しました！ Pro プランをお楽しみください。" if promo_success_param else "")
    promo_is_success = bool(promo_success_param)

    return templates.TemplateResponse(
        "plans.html",
        {
            "request": request,
            "current_user": current_user,
            "current_plan": current_plan,
            "trial_days_left": trial_days_left,
            "plan_renews_at": plan_renews_at,
            "ref_code": ref_code,
            "promo_days_left": promo_days_left,
            "referral_message": referral_message,
            "referral_success": referral_success,
            "promo_message": promo_message,
            "promo_is_success": promo_is_success,
        }
    )

@app.get("/mypage")
def mypage(request: Request):
    current_user = get_current_user(request)
    if not current_user:
        return RedirectResponse("/login", status_code=303)

    user = get_user_by_id(current_user["id"])
    user = check_trial_expired(user)

    current_plan = get_user_plan(user)
    limits = get_plan_limits(current_plan)
    trial_ends_at = user.get("trial_ends_at")
    plan_renews_at = user.get("plan_renews_at")
    ref_code = user.get("ref_code") or ""
    trial_extend_days = int(user.get("trial_extend_days") or 0)

    trial_days_left = None
    if trial_ends_at:
        today = datetime.now().date()
        end = datetime.strptime(trial_ends_at, "%Y-%m-%d").date()
        diff = (end - today).days
        if diff > 0:
            trial_days_left = diff

    daily_usage = get_user_daily_usage(current_user["id"])
    saved_count = count_user_saved_papers(current_user["id"])
    memo_count = count_user_all_memos(current_user["id"])

    recent_papers = get_saved_papers(user_id=current_user["id"])
    recent_papers = sorted(recent_papers, key=lambda x: x.get("created_at", ""), reverse=True)[:3]

    return templates.TemplateResponse(
        "mypage.html",
        {
            "request": request,
            "current_user": user,
            "current_plan": current_plan,
            "limits": limits,
            "trial_days_left": trial_days_left,
            "plan_renews_at": plan_renews_at,
            "ref_code": ref_code,
            "trial_extend_days": trial_extend_days,
            "daily_usage": daily_usage,
            "saved_count": saved_count,
            "memo_count": memo_count,
            "recent_papers": recent_papers,
        }
    )


@app.post("/mypage/profile")
def mypage_profile_update(
    request: Request,
    display_name: str = Form(""),
    bio: str = Form(""),
    avatar: str = Form(""),
):
    current_user = get_current_user(request)
    if not current_user:
        return RedirectResponse("/login", status_code=303)
    update_user_profile(
        current_user["id"],
        display_name.strip()[:40],
        bio.strip()[:160],
        avatar.strip()[:200000],
    )
    return RedirectResponse("/mypage?saved=1", status_code=303)


@app.get("/board")
def board_page(request: Request, tag: str = Query("")):
    # /board は /learn?tab=board にリダイレクト（既存リンクの後方互換）
    dest = "/learn?tab=board"
    if tag:
        dest += f"&tag={tag}"
    return RedirectResponse(dest, status_code=301)


@app.get("/learn")
def learn_page(request: Request, tab: str = Query("recommend"), tag: str = Query("")):
    current_user = get_current_user(request)
    posts = get_posts(limit=50, viewer_user_id=current_user["id"] if current_user else None, tag_filter=tag)
    recommended = []
    if current_user:
        try:
            recommended = get_recommended_papers(current_user["id"], limit=10)
        except Exception:
            recommended = []
    return templates.TemplateResponse("learn.html", {
        "request": request,
        "current_user": current_user,
        "posts": posts,
        "active_tag": tag,
        "tab": tab,
        "recommended": recommended,
    })


@app.post("/board/post")
def board_create_post(
    request: Request,
    content: str = Form(""),
    pubmed_id: str = Form(""),
    paper_title: str = Form(""),
    paper_jp_title: str = Form(""),
    tags: str = Form(""),
    parent_id: str = Form(""),
    redirect_to: str = Form(""),
):
    current_user = get_current_user(request)
    if not current_user:
        return RedirectResponse("/login", status_code=303)
    content = content.strip()
    if not content:
        return RedirectResponse(redirect_to or "/learn?tab=board", status_code=303)
    pid = int(parent_id) if parent_id.strip().isdigit() else None
    create_post(
        user_id=current_user["id"],
        content=content[:500],
        pubmed_id=pubmed_id.strip(),
        paper_title=paper_title.strip(),
        paper_jp_title=paper_jp_title.strip(),
        tags=tags.strip(),
        parent_id=pid,
    )
    # 投稿タグを興味スコアに反映（重み2）
    if tags.strip() and current_user:
        tag_list = [t.strip() for t in tags.split(",") if t.strip()]
        try:
            record_interest(current_user["id"], tag_list, weight=2.0)
        except Exception:
            pass
    dest = redirect_to or "/learn?tab=board"
    if pid:
        dest += f"#{pid}"
    return RedirectResponse(dest, status_code=303)


@app.post("/board/post/{post_id}/like")
def board_like_post(post_id: int, request: Request):
    current_user = get_current_user(request)
    if not current_user:
        return JSONResponse({"error": "login required"}, status_code=401)
    liked = toggle_post_like(post_id, current_user["id"])
    return JSONResponse({"liked": liked})


@app.post("/board/post/{post_id}/delete")
def board_delete_post(post_id: int, request: Request):
    current_user = get_current_user(request)
    if not current_user:
        return RedirectResponse("/login", status_code=303)
    delete_post(post_id, current_user["id"])
    return RedirectResponse("/board", status_code=303)


@app.get("/board/post/{post_id}/replies")
def board_get_replies(post_id: int, request: Request):
    current_user = get_current_user(request)
    replies = get_replies(post_id, viewer_user_id=current_user["id"] if current_user else None)
    return JSONResponse({"replies": replies})


@app.get("/user/tags")
def user_tags_get(request: Request):
    current_user = get_current_user(request)
    if not current_user:
        return JSONResponse({"tags": []})
    tags = get_user_tags(current_user["id"])
    return JSONResponse({"tags": tags})


@app.post("/user/tags")
async def user_tags_add(request: Request):
    current_user = get_current_user(request)
    if not current_user:
        return JSONResponse({"ok": False})
    form = await request.form()
    tag = (form.get("tag") or "").strip()
    if not tag:
        return JSONResponse({"ok": False})
    upsert_user_tag(current_user["id"], tag)
    return JSONResponse({"ok": True})


@app.post("/user/tags/{tag}/delete")
def user_tags_delete(tag: str, request: Request):
    current_user = get_current_user(request)
    if not current_user:
        return JSONResponse({"ok": False})
    delete_user_tag(current_user["id"], tag)
    return JSONResponse({"ok": True})


@app.post("/paper/highlight")
def paper_highlight_save(
    request: Request,
    pubmed_id: str = Form(""),
    highlights: str = Form(""),
):
    current_user = get_current_user(request)
    if not current_user or not pubmed_id:
        return JSONResponse({"ok": False})
    update_saved_paper_highlights(pubmed_id, highlights, current_user["id"])
    return JSONResponse({"ok": True})


@app.post("/memo/{memo_id}/tags")
def save_memo_tags(memo_id: int, request: Request, tags: str = Form("")):
    current_user = get_current_user(request)
    if not current_user:
        return JSONResponse({"ok": False})
    update_memo_tags(memo_id, current_user["id"], tags.strip())
    return JSONResponse({"ok": True})


@app.post("/memo/paper/{memo_id}/tags")
def save_paper_memo_tags(memo_id: int, request: Request, tags: str = Form("")):
    current_user = get_current_user(request)
    if not current_user:
        return JSONResponse({"ok": False})
    update_paper_memo_tags(memo_id, current_user["id"], tags.strip())
    return JSONResponse({"ok": True})


@app.get("/memo/export")
def memo_export(request: Request, fmt: str = Query("csv")):
    import csv, io
    current_user = get_current_user(request)
    if not current_user:
        return RedirectResponse("/login", status_code=303)
    memos = get_user_memos(current_user["id"])
    paper_memos = get_user_paper_memos(current_user["id"])

    if fmt == "csv":
        from fastapi.responses import StreamingResponse
        output = io.StringIO()
        writer = csv.writer(output)
        writer.writerow(["種別", "タイトル", "本文", "タグ", "更新日時"])
        for m in memos:
            writer.writerow(["メモ", m.get("title",""), m.get("body",""), m.get("tags",""), m.get("updated_at","")])
        for m in paper_memos:
            writer.writerow(["論文メモ", m.get("paper_title",""), m.get("body",""), m.get("tags",""), m.get("updated_at","")])
        output.seek(0)
        return StreamingResponse(
            iter([output.getvalue()]),
            media_type="text/csv",
            headers={"Content-Disposition": "attachment; filename=memos.csv"}
        )
    # print (HTML for browser print-to-PDF)
    all_memos = [{"type": "メモ", **m} for m in memos] + [{"type": "論文メモ", **m} for m in paper_memos]
    return templates.TemplateResponse("memo_export_print.html", {
        "request": request,
        "memos": all_memos,
        "current_user": current_user,
    })


@app.get("/saved/export")
def saved_export(request: Request):
    current_user = get_current_user(request)
    if not current_user:
        return RedirectResponse("/login", status_code=303)
    papers = get_saved_papers(current_user["id"])
    return templates.TemplateResponse("saved_export_print.html", {
        "request": request,
        "papers": papers,
        "current_user": current_user,
    })


@app.get("/search")
def search(request: Request, keyword: str = Query(...), page: int = Query(1)):
    current_user = get_current_user(request)
    PER_PAGE = 25

    if not keyword.strip():
        return templates.TemplateResponse(
            "search.html",
            {
                "request": request,
                "papers": [],
                "keyword": keyword,
                "converted_keyword": "",
                "current_user": current_user,
            }
        )

    if keyword in keyword_cache:
        converted_keyword = keyword_cache[keyword]
    else:
        try:
            converted_keyword = convert_keyword_with_gpt_if_needed(keyword)
        except Exception:
            converted_keyword = keyword

        keyword_cache[keyword] = converted_keyword

    try:
        handle = Entrez.esearch(
            db="pubmed",
            term=converted_keyword,
            retmax=250
        )
        record = Entrez.read(handle)
        handle.close()
    except Exception:
        return templates.TemplateResponse(
            "search.html",
            {
                "request": request,
                "papers": [],
                "keyword": keyword,
                "converted_keyword": converted_keyword,
                "current_user": current_user,
            }
        )

    id_list = record.get("IdList", [])

    if not id_list:
        return templates.TemplateResponse(
            "search.html",
            {
                "request": request,
                "papers": [],
                "keyword": keyword,
                "converted_keyword": converted_keyword,
                "page": 1,
                "total_pages": 1,
                "current_user": current_user,
            }
        )

    total_count = len(id_list)
    total_pages = max(1, (total_count + PER_PAGE - 1) // PER_PAGE)

    if page < 1:
        page = 1
    if page > total_pages:
        page = total_pages

    start_idx = (page - 1) * PER_PAGE
    end_idx = start_idx + PER_PAGE
    page_id_list = id_list[start_idx:end_idx]

    saved_papers = get_saved_papers(current_user["id"]) if current_user else get_saved_papers()
    saved_map = {str(p["pubmed_id"]): p for p in saved_papers}

    papers = []

    saved_ids = [pmid for pmid in page_id_list if str(pmid) in saved_map]
    unsaved_ids = [pmid for pmid in page_id_list if str(pmid) not in saved_map]

        # まずDB保存済み論文をそのまま使う
    for pmid in saved_ids:
        try:
            saved = saved_map.get(str(pmid))
            if not saved:
                continue

            title = saved.get("title", "") or ""
            jp_title = saved.get("jp_title", "") or title
            authors_text = saved.get("authors", "") or ""
            journal = saved.get("journal", "") or ""
            pubdate = saved.get("pubdate", "") or ""
            abstract = saved.get("abstract", "") or ""

            raw_saved_score = saved.get("clinical_score", "") or ""
            display_clinical_score = ""

            if raw_saved_score:
                try:
                    base_saved_score = float(raw_saved_score)
                    adjusted_saved_score = base_saved_score + stable_score_offset(str(pmid))
                    adjusted_saved_score = max(0.0, min(5.0, adjusted_saved_score))
                    display_clinical_score = f"{adjusted_saved_score:.2f}"
                except Exception:
                    display_clinical_score = raw_saved_score

            papers.append({
                "id": str(pmid),
                "pubmed_id": str(pmid),
                "title": title,
                "jp_title": jp_title,
                "authors": authors_text,
                "journal": journal,
                "pubdate": pubdate,
                "abstract": abstract or "",
                "tags": generate_tags(title, abstract or ""),
                "summary_jp": saved.get("summary_jp", "") or "",
                "clinical_score": display_clinical_score,
                "clinical_reason": saved.get("clinical_reason", "") or "",
                "likes": saved.get("likes", 0) if saved.get("likes") is not None else 0,
                "is_saved": True,
            })
        except Exception:
            continue

    # DBにない論文だけPubMedから取得する
    if unsaved_ids:
        try:
            handle = Entrez.efetch(
                db="pubmed",
                id=",".join(unsaved_ids),
                retmode="xml"
            )
            records = Entrez.read(handle)
            handle.close()
        except Exception:
            return templates.TemplateResponse(
                "search.html",
                {
                    "request": request,
                    "papers": papers,
                    "keyword": keyword,
                    "converted_keyword": converted_keyword,
                    "page": page,
                    "total_pages": total_pages,
                    "current_user": current_user,
                }
            )

        for article in records.get("PubmedArticle", []):
            try:
                citation = article.get("MedlineCitation", {})
                pmid = str(citation.get("PMID", ""))

                article_data = citation.get("Article", {})
                saved = saved_map.get(pmid)

                raw_title = str(article_data.get("ArticleTitle", ""))

                authors = []
                if "AuthorList" in article_data:
                    for a in article_data["AuthorList"]:
                        last = a.get("LastName", "")
                        fore = a.get("ForeName", "")
                        full_name = f"{last} {fore}".strip()
                        if full_name:
                            authors.append(full_name)

                raw_authors = ", ".join(authors)

                raw_journal = ""
                raw_pubdate = ""
                if "Journal" in article_data:
                    raw_journal = article_data["Journal"].get("Title", "")
                    issue = article_data["Journal"].get("JournalIssue", {})
                    pub = issue.get("PubDate", {})
                    year = pub.get("Year", "")
                    month = pub.get("Month", "")
                    day = pub.get("Day", "")
                    raw_pubdate = " ".join([year, month, day]).strip()

                raw_abstract = ""
                if "Abstract" in article_data:
                    parts = article_data["Abstract"].get("AbstractText", [])
                    raw_abstract = "\n\n".join([str(p) for p in parts])

                title = saved.get("title", raw_title) if saved else raw_title
                authors_text = saved.get("authors", raw_authors) if saved else raw_authors
                journal = saved.get("journal", raw_journal) if saved else raw_journal
                pubdate = saved.get("pubdate", raw_pubdate) if saved else raw_pubdate
                abstract = saved.get("abstract", raw_abstract) if saved else raw_abstract

                # 日本語タイトルを優先。なければグローバルキャッシュ→翻訳の順で取得
                jp_title = saved.get("jp_title", "") if saved else ""

                if not jp_title:
                    jp_title = get_paper_jp_title_global(pmid)

                if not jp_title and title:
                    try:
                        jp_title = translate_title_to_japanese(title)
                    except Exception:
                        jp_title = title

                    try:
                        save_paper(
                            pubmed_id=pmid,
                            title=title,
                            jp_title=jp_title,
                            authors=authors_text,
                            journal=journal,
                            pubdate=pubdate,
                            abstract=abstract or "",
                            jp=saved.get("jp", "") if saved else "",
                            summary_jp=saved.get("summary_jp", "") if saved else "",
                            folder_name=saved.get("folder_name", "") if saved else "",
                            clinical_score=saved.get("clinical_score", "") if saved else "",
                            clinical_reason=saved.get("clinical_reason", "") if saved else "",
                            user_id=current_user["id"] if current_user else None,
                        )
                    except Exception:
                        pass

                raw_saved_score = saved.get("clinical_score", "") if saved else ""
                display_clinical_score = ""

                if raw_saved_score:
                    try:
                        base_saved_score = float(raw_saved_score)
                        adjusted_saved_score = base_saved_score + stable_score_offset(pmid)
                        adjusted_saved_score = max(0.0, min(5.0, adjusted_saved_score))
                        display_clinical_score = f"{adjusted_saved_score:.2f}"
                    except Exception:
                        display_clinical_score = raw_saved_score

                papers.append({
                    "id": pmid,
                    "pubmed_id": pmid,
                    "title": title,
                    "jp_title": jp_title or title,
                    "authors": authors_text,
                    "journal": journal,
                    "pubdate": pubdate,
                    "abstract": abstract or "",
                    "tags": generate_tags(title, abstract or ""),
                    "summary_jp": saved.get("summary_jp", "") if saved else "",
                    "clinical_score": display_clinical_score,
                    "clinical_reason": saved.get("clinical_reason", "") if saved else "",
                    "likes": saved.get("likes", 0) if saved and saved.get("likes") is not None else 0,
                    "is_saved": bool(saved),
                })
            except Exception:
                continue


    papers_map = {str(p["pubmed_id"]): p for p in papers}
    papers = [papers_map[str(pmid)] for pmid in page_id_list if str(pmid) in papers_map]

    return templates.TemplateResponse(
        "search.html",
        {
            "request": request,
            "papers": papers,
            "keyword": keyword,
            "converted_keyword": converted_keyword,
            "page": page,
            "total_pages": total_pages,
            "current_user": current_user,
        }
    )

@app.get("/paper/meta")
def paper_meta(request: Request, id: str = Query("")):
    """Return basic paper metadata (title/jp_title) for board attachment preview."""
    pmid = id.strip()
    if not pmid:
        return JSONResponse({})
    current_user = get_current_user(request)
    saved = get_saved_paper_by_id(pmid, current_user["id"] if current_user else None)
    if saved:
        return JSONResponse({"title": saved.get("title", ""), "jp_title": saved.get("jp_title", "")})
    try:
        handle = Entrez.esummary(db="pubmed", id=pmid)
        record = Entrez.read(handle)
        handle.close()
        item = record[0] if record else {}
        title = str(item.get("Title", ""))
        return JSONResponse({"title": title, "jp_title": ""})
    except Exception:
        return JSONResponse({})


@app.get("/paper")
def paper(
    request: Request,
    id: str,
    translate: int = 0,
    summarize: int = 0,
    save_error: str = ""
):
    current_user = get_current_user(request)
    current_user_id = current_user["id"] if current_user else None

    handle = Entrez.efetch(
        db="pubmed",
        id=id,
        retmode="xml"
    )
    records = Entrez.read(handle)
    handle.close()

    article = records["PubmedArticle"][0]
    data = article["MedlineCitation"]["Article"]

    title = str(data.get("ArticleTitle", ""))

    authors = []

    if "AuthorList" in data:
        for a in data["AuthorList"]:
            last = a.get("LastName", "")
            fore = a.get("ForeName", "")
            full_name = f"{last} {fore}".strip()

            if full_name:
                authors.append(full_name)

    journal = ""
    pubdate = ""

    if "Journal" in data:
        journal = data["Journal"].get("Title", "")

        issue = data["Journal"].get("JournalIssue", {})
        pub = issue.get("PubDate", {})

        year = pub.get("Year", "")
        month = pub.get("Month", "")
        day = pub.get("Day", "")

        pubdate = " ".join([year, month, day]).strip()

    abstract = "abstractはありません。"

    if "Abstract" in data:
        parts = data["Abstract"].get("AbstractText", [])
        abstract_parts = []

        for p in parts:
            abstract_parts.append(str(p))

        if abstract_parts:
            abstract = "\n\n".join(abstract_parts)

    jp_title = ""
    jp = ""
    summary_jp = ""
    clinical_score = ""
    clinical_reason = ""

    saved_paper = get_saved_paper_by_id(id, user_id=current_user_id)

    if saved_paper:
        jp_title = saved_paper.get("jp_title") or ""
        jp = saved_paper.get("jp") or ""
        summary_jp = saved_paper.get("summary_jp") or ""
        clinical_reason = saved_paper.get("clinical_reason") or ""

        raw_saved_score = saved_paper.get("clinical_score") or ""

        if raw_saved_score:
            try:
                base_saved_score = float(raw_saved_score)
                adjusted_saved_score = base_saved_score + stable_score_offset(id)
                adjusted_saved_score = max(0.0, min(5.0, adjusted_saved_score))
                clinical_score = f"{adjusted_saved_score:.2f}"
            except Exception:
                clinical_score = raw_saved_score
        else:
            clinical_score = ""

    if not jp_title:
        jp_title = translate_title_to_japanese(title)

    if translate == 1 and not jp:
        jp = translate_abstract_to_japanese(abstract)

    if summarize == 1 and not summary_jp:
        summary_result = summarize_abstract_in_japanese(abstract)
        summary_jp = summary_result["summary"]
        clinical_reason = summary_result["reason"]

        try:
            base_score = float(summary_result["score"] or 0)
        except Exception:
            base_score = 0.0

        adjusted_score = base_score + stable_score_offset(id)
        adjusted_score = max(0.0, min(5.0, adjusted_score))
        clinical_score = f"{adjusted_score:.2f}"

    # サイト内キャッシュとして自動保存
    if jp_title or jp or summary_jp:
        existing_folder_name = ""

        if saved_paper:
            existing_folder_name = saved_paper.get("folder_name") or ""

        save_paper(
            pubmed_id=id,
            title=title,
            jp_title=jp_title,
            authors=", ".join(authors),
            journal=journal,
            pubdate=pubdate,
            abstract=abstract,
            jp=jp,
            summary_jp=summary_jp,
            folder_name=existing_folder_name,
            clinical_score=clinical_score,
            clinical_reason=clinical_reason,
            user_id=current_user_id,
        )

    refreshed_saved = get_saved_paper_by_id(id, user_id=current_user_id)

    paper = {
        "id": id,
        "title": title,
        "jp_title": jp_title,
        "authors": ", ".join(authors),
        "journal": journal,
        "pubdate": pubdate,
        "abstract": abstract,
        "jp": jp,
        "summary_jp": summary_jp,
        "clinical_score": clinical_score,
        "clinical_reason": clinical_reason,
        "likes": int(refreshed_saved.get("likes") or 0) if refreshed_saved else 0,
        "liked": get_paper_liked(id, current_user_id) if current_user_id else False,
        "is_favorite": int(refreshed_saved.get("is_favorite") or 0) if refreshed_saved else 0,
        "folder_name": refreshed_saved.get("folder_name") or "" if refreshed_saved else "",
        "custom_title": refreshed_saved.get("custom_title") or "" if refreshed_saved else "",
        "user_note": refreshed_saved.get("user_note") or "" if refreshed_saved else "",
    }

    save_error_message = ""

    if save_error == "login_required":
        save_error_message = "保存するにはログインが必要です。"
    elif save_error == "limit_reached":
        save_error_message = "現在のプランの保存上限に達しています。プラン変更をご検討ください。"

    return templates.TemplateResponse(
    "paper.html",
    {
        "request": request,
        "paper": paper,
        "folder_suggestions": get_folder_name_suggestions(user_id=current_user_id),
        "save_error_message": save_error_message,
    }
)


@app.post("/save")
def save_paper_route(
    request: Request,
    pubmed_id: str = Form(...),
    title: str = Form(""),
    jp_title: str = Form(""),
    authors: str = Form(""),
    journal: str = Form(""),
    pubdate: str = Form(""),
    abstract: str = Form(""),
    jp: str = Form(""),
    summary_jp: str = Form(""),
    folder_name: str = Form(""),
    clinical_score: str = Form(""),
    clinical_reason: str = Form(""),
):
    current_user = get_current_user(request)

    if not current_user:
        return JSONResponse(
            {"ok": False, "message": "ログインしてください"},
            status_code=401
        )

    save_paper(
        pubmed_id=pubmed_id,
        title=title,
        jp_title=jp_title,
        authors=authors,
        journal=journal,
        pubdate=pubdate,
        abstract=abstract,
        jp=jp,
        summary_jp=summary_jp,
        folder_name=folder_name,
        clinical_score=clinical_score,
        clinical_reason=clinical_reason,
        user_id=current_user["id"],
    )

    # 保存行動を興味タグに記録（重み3）
    try:
        _tags_from_title = [w for w in (jp_title or title or "").split() if len(w) >= 2][:5]
        if _tags_from_title:
            record_interest(current_user["id"], _tags_from_title, weight=3.0)
    except Exception:
        pass

    return JSONResponse(
        {"ok": True, "message": "保存しました"}
    )

    save_paper(
        pubmed_id=pubmed_id,
        title=title,
        jp_title=jp_title,
        authors=authors,
        journal=journal,
        pubdate=pubdate,
        abstract=abstract,
        jp=jp,
        summary_jp=summary_jp,
        folder_name=folder_name.strip(),
        clinical_score=clinical_score,
        clinical_reason=clinical_reason,
        user_id=current_user_id,
    )

    return RedirectResponse(
        url="/saved",
        status_code=303
    )


@app.get("/saved")
def saved(request: Request):
    current_user = get_current_user(request)

    if not current_user:
        papers = get_public_papers()

        folders = {}

        for paper in papers:
            folder_name = paper.get("folder_name") or "公開保存"

            if folder_name not in folders:
                folders[folder_name] = []

            folders[folder_name].append(paper)

        return templates.TemplateResponse(
            "saved.html",
            {
                "request": request,
                "folders": folders,
                "is_guest": True,
            }
        )

    current_user_id = current_user["id"]

    papers = get_saved_papers(user_id=current_user_id)

    folders = {}

    for paper in papers:
        folder_name = (paper.get("folder_name") or "").strip()

        if not folder_name:
            continue

        if folder_name == "未分類":
            continue

        if folder_name not in folders:
            folders[folder_name] = []

        folders[folder_name].append(paper)

    return templates.TemplateResponse(
        "saved.html",
        {
            "request": request,
            "folders": folders,
            "is_guest": False,
        }
    )

@app.get("/p/{pubmed_id}")
def public_paper(request: Request, pubmed_id: str):

    paper = get_saved_paper_by_id(pubmed_id)

    if not paper:
        return templates.TemplateResponse(
            "notfound.html",
            {
                "request": request,
            }
        )

    return templates.TemplateResponse(
        "public.html",
        {
            "request": request,
            "paper": paper,
        }
    )

@app.get("/saved/{folder_name}")
def saved_folder(request: Request, folder_name: str, sort: str = "saved"):
    current_user = get_current_user(request)
    current_user_id = current_user["id"] if current_user else None

    papers = get_saved_papers_by_folder(folder_name, user_id=current_user_id)

    for paper in papers:
        custom_title = (paper.get("custom_title") or "").strip()
        default_title = (paper.get("jp_title") or paper.get("title") or "").strip()
        paper["display_title"] = custom_title or default_title

    if sort == "score":
        papers = sorted(
            papers,
            key=lambda x: float(x.get("clinical_score") or 0),
            reverse=True
        )
    elif sort == "favorite":
        papers = sorted(
            papers,
            key=lambda x: (int(x.get("is_favorite") or 0), x.get("created_at", "")),
            reverse=True
        )
    else:
        papers = sorted(
            papers,
            key=lambda x: x.get("created_at", ""),
            reverse=True
        )

    return templates.TemplateResponse(
        "saved_folder.html",
        {
            "request": request,
            "folder_name": folder_name,
            "papers": papers,
            "sort": sort,
        }
    )

@app.post("/favorite/{pubmed_id}")
def favorite(request: Request, pubmed_id: str):
    current_user = get_current_user(request)
    current_user_id = current_user["id"] if current_user else None

    toggle_favorite(pubmed_id, user_id=current_user_id)

    paper = get_saved_paper_by_id(pubmed_id, user_id=current_user_id)
    is_favorite = 0

    if paper:
        is_favorite = int(paper.get("is_favorite") or 0)
        # お気に入り追加時に興味タグ記録（重み4）
        if is_favorite and current_user_id:
            try:
                _title = paper.get("jp_title") or paper.get("title") or ""
                _tags = [w for w in _title.split() if len(w) >= 2][:5]
                if _tags:
                    record_interest(current_user_id, _tags, weight=4.0)
            except Exception:
                pass

    return JSONResponse(
        {
            "ok": True,
            "is_favorite": is_favorite
        }
    )


@app.post("/like/{pubmed_id}")
def like(request: Request, pubmed_id: str):
    current_user = get_current_user(request)
    current_user_id = current_user["id"] if current_user else None

    saved_paper = get_saved_paper_by_id(pubmed_id, user_id=current_user_id)

    if not saved_paper:
        handle = Entrez.efetch(db="pubmed", id=pubmed_id, retmode="xml")
        records = Entrez.read(handle)
        handle.close()

        article = records["PubmedArticle"][0]
        data = article["MedlineCitation"]["Article"]

        title = str(data.get("ArticleTitle", ""))

        authors = []
        if "AuthorList" in data:
            for a in data["AuthorList"]:
                last = a.get("LastName", "")
                fore = a.get("ForeName", "")
                full_name = f"{last} {fore}".strip()
                if full_name:
                    authors.append(full_name)

        journal = ""
        pubdate = ""
        if "Journal" in data:
            journal = data["Journal"].get("Title", "")
            issue = data["Journal"].get("JournalIssue", {})
            pub = issue.get("PubDate", {})
            year = pub.get("Year", "")
            month = pub.get("Month", "")
            day = pub.get("Day", "")
            pubdate = " ".join([year, month, day]).strip()

        abstract = "abstractはありません。"
        if "Abstract" in data:
            parts = data["Abstract"].get("AbstractText", [])
            abstract_parts = [str(p) for p in parts]
            if abstract_parts:
                abstract = "\n\n".join(abstract_parts)

        save_paper(
            pubmed_id=pubmed_id,
            title=title,
            jp_title="",
            authors=", ".join(authors),
            journal=journal,
            pubdate=pubdate,
            abstract=abstract,
            jp="",
            summary_jp="",
            folder_name="未分類",
            clinical_score="",
            clinical_reason="",
            user_id=current_user_id,
        )

    if current_user_id:
        result = toggle_paper_like(pubmed_id, current_user_id)
        return JSONResponse({"ok": True, "likes": result["likes"], "liked": result["liked"]})

    # guest: legacy add_like (no toggle)
    add_like(pubmed_id, user_id=None)
    paper_row = get_saved_paper_by_id(pubmed_id, user_id=None)
    likes = int(paper_row["likes"] or 0) if paper_row else 0
    return JSONResponse({"ok": True, "likes": likes, "liked": True})

@app.post("/saved/{pubmed_id}/move")
def move_saved_paper(
    request: Request,
    pubmed_id: str,
    folder_name: str = Form(...)
):
    current_user = get_current_user(request)

    if not current_user:
        return JSONResponse({"ok": False, "error": "login_required"}, status_code=401)

    current_user_id = current_user["id"]
    target_folder_name = (folder_name or "").strip()

    if not target_folder_name:
        return JSONResponse({"ok": False, "message": "移動先フォルダ名を入力してください"}, status_code=400)

    saved_paper = get_saved_paper_by_id(pubmed_id, user_id=current_user_id)
    if not saved_paper:
        return JSONResponse({"ok": False, "message": "保存論文が見つかりません"}, status_code=404)

    update_saved_paper_folder(pubmed_id, target_folder_name, user_id=current_user_id)

    return JSONResponse({
        "ok": True,
        "folder_name": target_folder_name,
        "message": "フォルダを変更しました",
    })


@app.post("/saved/{pubmed_id}/rename")
def rename_saved_paper(
    request: Request,
    pubmed_id: str,
    custom_title: str = Form("")
):
    current_user = get_current_user(request)

    if not current_user:
        return JSONResponse({"ok": False, "error": "login_required"}, status_code=401)

    current_user_id = current_user["id"]

    saved_paper = get_saved_paper_by_id(pubmed_id, user_id=current_user_id)
    if not saved_paper:
        return JSONResponse({"ok": False, "message": "保存論文が見つかりません"}, status_code=404)

    clean_custom_title = (custom_title or "").strip()
    update_saved_paper_custom_title(pubmed_id, clean_custom_title, user_id=current_user_id)

    display_title = clean_custom_title or (saved_paper.get("jp_title") or saved_paper.get("title") or "")

    return JSONResponse({
        "ok": True,
        "custom_title": clean_custom_title,
        "display_title": display_title,
        "message": "表示名を更新しました",
    })


@app.post("/saved/{pubmed_id}/note")
def update_saved_paper_note_route(
    request: Request,
    pubmed_id: str,
    user_note: str = Form("")
):
    current_user = get_current_user(request)

    if not current_user:
        return JSONResponse({"ok": False, "error": "login_required"}, status_code=401)

    current_user_id = current_user["id"]

    saved_paper = get_saved_paper_by_id(pubmed_id, user_id=current_user_id)
    if not saved_paper:
        return JSONResponse({"ok": False, "message": "保存論文が見つかりません"}, status_code=404)

    clean_user_note = (user_note or "").strip()
    update_saved_paper_user_note(pubmed_id, clean_user_note, user_id=current_user_id)

    return JSONResponse({
        "ok": True,
        "user_note": clean_user_note,
        "message": "メモを更新しました",
    })

@app.post("/saved/folder/rename")
def rename_folder_route(
    request: Request,
    old_name: str = Form(...),
    new_name: str = Form(...),
):
    current_user = get_current_user(request)
    if not current_user:
        return JSONResponse({"ok": False, "error": "login_required"}, status_code=401)
    new_name = new_name.strip()
    if not new_name:
        return JSONResponse({"ok": False, "message": "フォルダ名を入力してください"})
    rename_folder(current_user["id"], old_name.strip(), new_name)
    return JSONResponse({"ok": True, "new_name": new_name})


@app.get("/ranking")
def ranking_list(request: Request, sort: str = "likes"):
    current_user = get_current_user(request)
    plan = get_user_plan(current_user)
    limits = get_plan_limits(plan)

    if sort == "trend":
        papers = get_pubmed_trending_papers(limits["ranking_limit"])
        page_title = "PubMedトレンド論文ランキング"
        page_description = "PubMedで最近アクティビティが増えている論文を表示しています。累積閲覧数順ではありません。"
    else:
        papers = get_saved_papers()

        if sort == "score" and not limits["can_view_score_ranking"]:
            return RedirectResponse(url="/?ranking_error=score_locked", status_code=303)

        if sort == "likes":
            papers = sorted(papers, key=lambda x: int(x.get("likes") or 0), reverse=True)
        elif sort == "score":
            papers = sorted(papers, key=lambda x: float(x.get("clinical_score") or 0), reverse=True)
        else:
            papers = sorted(papers, key=lambda x: x.get("created_at", ""), reverse=True)

        papers = papers[:limits["ranking_limit"]]
        page_title = "人気論文ランキング" if sort == "likes" else "臨床参考度ランキング"
        page_description = "SaaS内で保存・要約された論文の中から読めるランキング一覧です。"

    return templates.TemplateResponse(
        "public_list.html",
        {
            "request": request,
            "papers": papers,
            "sort": sort,
            "page_mode": "ranking",
            "page_title": page_title,
            "page_description": page_description,
            "base_path": "/ranking",
            "empty_message": "まだランキング対象の論文がありません。",
        }
    )

@app.get("/public")
def public_list(request: Request, sort: str = "likes"):
    papers = get_public_papers()

    if sort == "likes":
        papers = sorted(
            papers,
            key=lambda x: int(x.get("likes") or 0),
            reverse=True
        )
    elif sort == "score":
        papers = sorted(
            papers,
            key=lambda x: float(x.get("clinical_score") or 0),
            reverse=True
        )
    else:
        papers = sorted(
            papers,
            key=lambda x: x.get("created_at", ""),
            reverse=True
        )

    return templates.TemplateResponse(
        "public_list.html",
        {
            "request": request,
            "papers": papers,
            "sort": sort,
            "page_mode": "public",
            "page_title": "公開論文一覧",
            "page_description": "公開設定された論文だけをまとめた、マスター版の公開一覧ページです。",
            "base_path": "/public",
            "empty_message": "まだ公開論文がありません。保存フォルダで「公開する」を押した論文だけがここに表示されます。",
        }
    )

@app.get("/register")
def register_page(request: Request):
    current_user = get_current_user(request)

    if current_user:
        return RedirectResponse(url="/", status_code=303)

    return templates.TemplateResponse(
        "register.html",
        {
            "request": request,
            "error": "",
        }
    )


@app.post("/register")
def register(request: Request, email: str = Form(...), password: str = Form(...)):
    current_user = get_current_user(request)

    if current_user:
        return RedirectResponse(url="/", status_code=303)

    email = email.strip().lower()

    if not email or not password:
        return templates.TemplateResponse(
            "register.html",
            {
                "request": request,
                "error": "メールアドレスとパスワードを入力してください。",
            }
        )

    if len(password) < 6:
        return templates.TemplateResponse(
            "register.html",
            {
                "request": request,
                "error": "パスワードは6文字以上で入力してください。",
            }
        )

    user = create_user(email, password)

    if not user:
        return templates.TemplateResponse(
            "register.html",
            {
                "request": request,
                "error": "このメールアドレスは既に登録されています。",
            }
        )

    request.session["user_id"] = user["id"]
    return RedirectResponse(url="/", status_code=303)


@app.get("/login")
def login_page(request: Request, from_page: str = Query(default="", alias="from")):
    current_user = get_current_user(request)

    if current_user:
        return RedirectResponse(url="/", status_code=303)

    return templates.TemplateResponse(
        "login.html",
        {
            "request": request,
            "error": "",
            "from_page": from_page,
        }
    )


@app.post("/login")
def login(request: Request, email: str = Form(...), password: str = Form(...)):
    current_user = get_current_user(request)

    if current_user:
        return RedirectResponse(url="/", status_code=303)

    email = email.strip().lower()
    user = verify_user(email, password)

    if not user:
        return templates.TemplateResponse(
            "login.html",
            {
                "request": request,
                "error": "メールアドレスまたはパスワードが違います。",
            }
        )

    request.session["user_id"] = user["id"]
    return RedirectResponse(url="/", status_code=303)


@app.get("/logout")
def logout(request: Request):
    request.session.clear()
    return RedirectResponse(url="/", status_code=303)

@app.post("/public/{pubmed_id}")
def public_toggle(request: Request, pubmed_id: str):
    current_user = get_current_user(request)
    current_user_id = current_user["id"] if current_user else None

    toggle_public(pubmed_id, user_id=current_user_id)

    paper = get_saved_paper_by_id(pubmed_id, user_id=current_user_id)

    is_public = 0

    if paper:
        is_public = int(paper.get("is_public") or 0)

    return JSONResponse(
        {
            "ok": True,
            "is_public": is_public
        }
    )

# ─── メモ機能 ──────────────────────────────────────────

@app.get("/memo")
def memo_list(request: Request, tab: str = "quick"):
    current_user = get_current_user(request)
    if not current_user:
        return RedirectResponse("/login?from=memo", status_code=303)

    user_id = current_user["id"]
    plan = get_user_plan(current_user)
    limits = get_plan_limits(plan)
    memo_limit = limits["memo_limit"]

    quick_memos = get_user_memos(user_id)
    paper_memos = get_user_paper_memos(user_id)
    total_count = len(quick_memos) + len(paper_memos)

    return templates.TemplateResponse(
        "memo.html",
        {
            "request": request,
            "current_user": current_user,
            "current_plan": plan,
            "tab": tab,
            "quick_memos": quick_memos,
            "paper_memos": paper_memos,
            "total_count": total_count,
            "memo_limit": memo_limit,
        }
    )


@app.post("/memo/create")
def memo_create(request: Request, title: str = Form(""), body: str = Form("")):
    current_user = get_current_user(request)
    if not current_user:
        return JSONResponse({"ok": False, "error": "login_required"}, status_code=401)

    user_id = current_user["id"]
    plan = get_user_plan(current_user)
    limits = get_plan_limits(plan)
    memo_limit = limits["memo_limit"]

    if memo_limit is not None:
        total = count_user_all_memos(user_id)
        if total >= memo_limit:
            return RedirectResponse("/memo?tab=quick&error=limit_reached", status_code=303)

    memo_id = create_memo(user_id, title.strip(), body.strip())
    return RedirectResponse(f"/memo/{memo_id}", status_code=303)


@app.get("/memo/{memo_id}")
def memo_detail(request: Request, memo_id: int):
    current_user = get_current_user(request)
    if not current_user:
        return RedirectResponse("/login?from=memo", status_code=303)

    memo = get_memo_by_id(memo_id, current_user["id"])
    if not memo:
        return RedirectResponse("/memo", status_code=303)

    plan = get_user_plan(current_user)

    return templates.TemplateResponse(
        "memo_detail.html",
        {
            "request": request,
            "current_user": current_user,
            "current_plan": plan,
            "memo": memo,
            "memo_type": "quick",
        }
    )


@app.post("/memo/{memo_id}/update")
def memo_update(
    request: Request,
    memo_id: int,
    title: str = Form(""),
    body: str = Form(""),
):
    current_user = get_current_user(request)
    if not current_user:
        return JSONResponse({"ok": False, "error": "login_required"}, status_code=401)

    update_memo(memo_id, current_user["id"], title.strip(), body.strip())
    return JSONResponse({"ok": True})


@app.post("/memo/{memo_id}/delete")
def memo_delete(request: Request, memo_id: int):
    current_user = get_current_user(request)
    if not current_user:
        return RedirectResponse("/login?from=memo", status_code=303)

    delete_memo(memo_id, current_user["id"])
    return RedirectResponse("/memo?tab=quick", status_code=303)


@app.post("/memo/{memo_id}/delete_if_empty")
def memo_delete_if_empty(request: Request, memo_id: int):
    current_user = get_current_user(request)
    if not current_user:
        return JSONResponse({"ok": False})
    memo = get_memo_by_id(memo_id, current_user["id"])
    if memo and not memo["title"].strip() and not memo["body"].strip():
        delete_memo(memo_id, current_user["id"])
    return JSONResponse({"ok": True})


@app.get("/memo/paper/new")
def paper_memo_new_page(request: Request):
    current_user = get_current_user(request)
    if not current_user:
        return RedirectResponse("/login?from=memo", status_code=303)

    user_id = current_user["id"]
    plan = get_user_plan(current_user)
    limits = get_plan_limits(plan)
    memo_limit = limits["memo_limit"]

    if memo_limit is not None:
        total = count_user_all_memos(user_id)
        if total >= memo_limit:
            return RedirectResponse("/memo?tab=paper&error=limit_reached", status_code=303)

    saved_papers = get_saved_papers(user_id=user_id)
    paper_list = [
        {
            "pubmed_id": p["pubmed_id"],
            "display_title": (p.get("jp_title") or p.get("title") or p["pubmed_id"])[:60],
        }
        for p in saved_papers
        if (p.get("folder_name") or "").strip()
    ]

    return templates.TemplateResponse(
        "memo_paper_new.html",
        {
            "request": request,
            "current_user": current_user,
            "current_plan": plan,
            "paper_list": paper_list,
        }
    )


@app.post("/memo/paper/create")
def paper_memo_create(
    request: Request,
    pubmed_id: str = Form(...),
    paper_title: str = Form(""),
    body: str = Form(""),
):
    current_user = get_current_user(request)
    if not current_user:
        return RedirectResponse("/login?from=memo", status_code=303)

    user_id = current_user["id"]
    plan = get_user_plan(current_user)
    limits = get_plan_limits(plan)
    memo_limit = limits["memo_limit"]

    if memo_limit is not None:
        total = count_user_all_memos(user_id)
        if total >= memo_limit:
            return RedirectResponse("/memo?tab=paper&error=limit_reached", status_code=303)

    memo_id = create_paper_memo(user_id, pubmed_id.strip(), paper_title.strip(), body.strip())
    return RedirectResponse(f"/memo/paper/{memo_id}", status_code=303)


@app.get("/memo/paper/{memo_id}")
def paper_memo_detail(request: Request, memo_id: int):
    current_user = get_current_user(request)
    if not current_user:
        return RedirectResponse("/login?from=memo", status_code=303)

    memo = get_paper_memo_by_id(memo_id, current_user["id"])
    if not memo:
        return RedirectResponse("/memo?tab=paper", status_code=303)

    plan = get_user_plan(current_user)

    return templates.TemplateResponse(
        "memo_detail.html",
        {
            "request": request,
            "current_user": current_user,
            "current_plan": plan,
            "memo": memo,
            "memo_type": "paper",
        }
    )


@app.post("/memo/paper/{memo_id}/update")
def paper_memo_update(
    request: Request,
    memo_id: int,
    body: str = Form(""),
):
    current_user = get_current_user(request)
    if not current_user:
        return JSONResponse({"ok": False, "error": "login_required"}, status_code=401)

    update_paper_memo(memo_id, current_user["id"], body.strip())
    return JSONResponse({"ok": True})


@app.post("/memo/paper/{memo_id}/delete")
def paper_memo_delete(request: Request, memo_id: int):
    current_user = get_current_user(request)
    if not current_user:
        return RedirectResponse("/login?from=memo", status_code=303)

    delete_paper_memo(memo_id, current_user["id"])
    return RedirectResponse("/memo?tab=paper", status_code=303)


@app.post("/memo/paper/{memo_id}/delete_if_empty")
def paper_memo_delete_if_empty(request: Request, memo_id: int):
    current_user = get_current_user(request)
    if not current_user:
        return JSONResponse({"ok": False})
    memo = get_paper_memo_by_id(memo_id, current_user["id"])
    if memo and not memo["body"].strip():
        delete_paper_memo(memo_id, current_user["id"])
    return JSONResponse({"ok": True})


def check_trial_expired(user):

    trial_ends_at = user.get("trial_ends_at")

    if not trial_ends_at:
        return user

    today = datetime.now().date()
    end = datetime.strptime(trial_ends_at, "%Y-%m-%d").date()

    if today > end:

        update_user_plan(
            user_id=user["id"],
            plan=user["plan"],
            trial_ends_at=None,
            plan_started_at=user.get("plan_started_at"),
            plan_renews_at=user.get("plan_renews_at"),
            is_yearly=user.get("is_yearly", 0),
        )

        user = get_user_by_id(user["id"])

    return user
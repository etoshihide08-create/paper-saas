import re
import os
import hashlib
from dotenv import load_dotenv

from fastapi import FastAPI, Request, Form
from fastapi.templating import Jinja2Templates
from fastapi.responses import RedirectResponse, JSONResponse
from Bio import Entrez
from openai import OpenAI
from datetime import datetime, timedelta
from fastapi import FastAPI, Request, Query
from db import (
    init_db,
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
)
from starlette.middleware.sessions import SessionMiddleware

load_dotenv()

app = FastAPI()
app.add_middleware(SessionMiddleware, secret_key=os.getenv("SESSION_SECRET"))
templates = Jinja2Templates(directory="templates")

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

def get_current_user(request: Request):
    user_id = request.session.get("user_id")

    if not user_id:
        return None

    return get_user_by_id(user_id)

def get_user_plan(user):
    if not user:
        return "guest"

    plan = (user.get("plan") or "free").strip().lower()

    if plan not in ["free", "pro", "expert"]:
        return "free"

    return plan

def get_plan_limits(plan):

    limits = {
        "guest": {
            "daily_summary_limit": 5,
            "save_limit": 0,
            "ranking_limit": 20,
            "can_view_score_ranking": True,
        },

        "free": {
            "daily_summary_limit": 5,
            "save_limit": 10,
            "ranking_limit": 20,
            "can_view_score_ranking": True,
        },

        "pro": {
            "daily_summary_limit": 50,
            "save_limit": 150,
            "ranking_limit": 100,
            "can_view_score_ranking": True,
        },

        "expert": {
            "daily_summary_limit": None,
            "save_limit": None,
            "ranking_limit": 300,
            "can_view_score_ranking": True,
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

def get_user_plan(user):
    if not user:
        return "guest"

    plan = (user.get("plan") or "free").strip().lower()

    if plan not in ["free", "pro", "expert"]:
        return "free"

    return plan


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
    tags = []
    text = f"{title} {abstract}".lower()

    # ===== Sランク =====
    if "stroke" in text:
        tags.append("脳卒中")

    if "gait" in text or "walking" in text:
        tags.append("歩行")

    if "rehabilitation" in text:
        tags.append("リハビリ")

    if "balance" in text:
        tags.append("バランス")

    if "adl" in text or "activities of daily living" in text:
        tags.append("ADL")

    if "fall" in text or "falls" in text:
        tags.append("転倒")

    if "strength" in text or "muscle strength" in text:
        tags.append("筋力")

    if "cognitive" in text or "cognition" in text:
        tags.append("認知機能")

    # ===== Aランク =====
    if "spinal cord" in text:
        tags.append("脊髄損傷")

    if "parkinson" in text:
        tags.append("パーキンソン病")

    if "elderly" in text or "older adult" in text:
        tags.append("高齢者")

    if "exercise therapy" in text or "exercise" in text:
        tags.append("運動療法")

    if "neuroplasticity" in text:
        tags.append("神経可塑性")

    if "gait analysis" in text:
        tags.append("歩行分析")

    if "posture" in text or "postural control" in text:
        tags.append("姿勢制御")

    if "emg" in text or "electromyography" in text:
        tags.append("筋電図")

    # ===== Bランク =====
    if "respiratory" in text:
        tags.append("呼吸リハ")

    if "knee osteoarthritis" in text or "knee oa" in text:
        tags.append("膝OA")

    if "pain" in text:
        tags.append("疼痛")

    if "range of motion" in text or "rom" in text:
        tags.append("可動域")

    return tags[:5]
    
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
1.0〜5.0の範囲で、0.1刻みの数値だけを書く

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

臨床参考度の評価では以下を総合的に見ること。
・研究デザイン
・対象人数
・対象の明確さ
・介入と評価方法の明確さ
・臨床応用のしやすさ
・一般化のしやすさ
・限界の大きさ

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

    updated_papers = []

    for paper in papers:

        jp_title = paper.get("jp_title") or ""
        title = paper.get("title") or ""

        if not jp_title and title:

            jp_title = translate_title_to_japanese(title)

            save_paper(
                pubmed_id=paper["pubmed_id"],
                title=title,
                jp_title=jp_title,
                authors=paper.get("authors") or "",
                journal=paper.get("journal") or "",
                pubdate=paper.get("pubdate") or "",
                abstract=paper.get("abstract") or "",
                jp=paper.get("jp") or "",
                summary_jp=paper.get("summary_jp") or "",
                folder_name=paper.get("folder_name") or "",
                clinical_score=paper.get("clinical_score") or "",
                clinical_reason=paper.get("clinical_reason") or "",
                user_id=paper.get("user_id"),
            )

            paper["jp_title"] = jp_title

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

    return templates.TemplateResponse(
        "index.html",
        {
            "request": request,
            "popular_papers": popular_papers,
            "top_rated_papers": top_rated_papers,
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
            }
        )

    user = get_user_by_id(current_user["id"])
    user = check_trial_expired(user)

    current_plan = user.get("plan") or "free"

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

    return templates.TemplateResponse(
        "plans.html",
        {
            "request": request,
            "current_user": current_user,
            "current_plan": current_plan,
            "trial_days_left": trial_days_left,
            "plan_renews_at": plan_renews_at,
            "ref_code": ref_code,
        }
    )

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
                "converted_keyword": ""
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
                "converted_keyword": converted_keyword
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

                # 日本語タイトルを優先。なければ初回だけ翻訳して保存する
                jp_title = saved.get("jp_title", "") if saved else ""

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
        }
    )

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

    add_like(pubmed_id, user_id=current_user_id)

    paper = get_saved_paper_by_id(pubmed_id, user_id=current_user_id)
    likes = int(paper["likes"] or 0) if paper else 0

    return JSONResponse({"ok": True, "likes": likes})

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

@app.get("/ranking")
def ranking_list(request: Request, sort: str = "likes"):
    current_user = get_current_user(request)
    plan = get_user_plan(current_user)
    limits = get_plan_limits(plan)

    papers = get_saved_papers()

    if sort == "score" and not limits["can_view_score_ranking"]:
        return RedirectResponse(url="/?ranking_error=score_locked", status_code=303)

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

    papers = papers[:limits["ranking_limit"]]

    return templates.TemplateResponse(
        "public_list.html",
        {
            "request": request,
            "papers": papers,
            "sort": sort,
            "page_mode": "ranking",
            "page_title": "人気論文ランキング" if sort == "likes" else "臨床参考度ランキング",
            "page_description": "SaaS内で保存・要約された論文の中から読めるランキング一覧です。",
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
def login_page(request: Request):
    current_user = get_current_user(request)

    if current_user:
        return RedirectResponse(url="/", status_code=303)

    return templates.TemplateResponse(
        "login.html",
        {
            "request": request,
            "error": "",
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
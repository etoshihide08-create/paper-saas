import re
import os
import time
import json
import base64
import hashlib
import threading
import urllib.request as _urlreq
import urllib.parse as _urlparse
from dotenv import load_dotenv

from fastapi import FastAPI, Request, Form
from fastapi.templating import Jinja2Templates
from fastapi.responses import RedirectResponse, JSONResponse, StreamingResponse, Response
from fastapi.staticfiles import StaticFiles
from Bio import Entrez
from openai import OpenAI
from datetime import datetime, timedelta
from fastapi import FastAPI, Request, Query
from db import (
    get_connection,
    init_db,
    init_memos_tables,
    save_paper,
    get_saved_papers,
    get_all_saved_papers,
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
    get_interest_tags,
    get_friend_promo_code,
    use_friend_promo_code,
    set_friend_promo_target_email,
    apply_promo_to_user,
    apply_lifetime_promo_to_user,
    get_supporter_campaign_claim_counts,
    get_user_supporter_campaign_claim,
    claim_supporter_campaign,
    create_master_article_draft,
    get_master_article_drafts,
    get_master_article_draft,
    update_master_article_draft_content,
    update_master_article_draft_geo_review,
    mark_master_article_wordpress_posted,
    get_master_wordpress_settings,
    upsert_master_wordpress_settings,
    get_master_wordpress_autopost_settings,
    upsert_master_wordpress_autopost_settings,
    update_master_wordpress_autopost_run_state,
    create_master_wordpress_autopost_log,
    get_master_wordpress_autopost_logs,
    get_master_wordpress_autopost_enabled_settings,
    get_next_master_article_draft_for_autopost,
    record_master_article_marketing_event,
    set_user_article_attribution,
    get_master_article_marketing_summary,
)
from starlette.middleware.sessions import SessionMiddleware

load_dotenv()

app = FastAPI()
app.add_middleware(
    SessionMiddleware,
    secret_key=os.getenv("SESSION_SECRET"),
    max_age=60 * 60 * 24 * 30,  # 30日間セッションを維持
    same_site="lax",
    https_only=False,
)
templates = Jinja2Templates(directory="templates")
if os.path.isdir("static"):
    app.mount("/static", StaticFiles(directory="static"), name="static")

Entrez.email = os.getenv("ENTREZ_EMAIL")
client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
init_db()
init_memos_tables()

_master_autopost_scheduler_started = False
_master_autopost_scheduler_lock = threading.Lock()

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

COMMON_DISCOVERY_TAGS = [
    "整形",
    "中枢",
    "脳神経",
    "呼吸器",
    "循環器",
    "腎臓機能",
    "高齢者・フレイル",
    "廃用症候群",
    "リスク管理",
    "訪問・地域",
    "小児リハ",
    "ADL",
    "歩行",
    "離床",
    "装具",
    "栄養管理",
    "画像所見",
    "嚥下・言語",
    "認知",
    "目標設定・教育",
    "多職種連携",
    "評価・研究デザイン",
]

SUPPORTER_CAMPAIGNS = [
    {
        "slug": "special-supporter",
        "parent_slug": "supporter",
        "plan": "pro",
        "limit": 30,
        "title": "特別サポーター特典",
        "tier_label": "サポーター価格の中の特別枠",
        "limit_label": "先着30名限定",
        "price_line": "最初の6か月は月額500円",
        "description": "980円のサポーター枠に含まれる特別特典です。先着30名だけ、最初の6か月は月額500円で始められます。その後もサポーター価格の月額980円で継続できます。",
        "identity_copy": "あなたは特別サポーター枠です",
        "savings_note": "通常のProより最初の6か月は月額1,000円お得です。",
        "expert_upgrade_note": "サポーター価格でご利用中の方は、必要になったタイミングで Expert を優待価格（月額1480円）でご利用いただけます。",
        "cta_label": "この価格を選ぶ",
        "badge": "最優先でおすすめ",
        "highlight": True,
    },
    {
        "slug": "supporter",
        "plan": "pro",
        "limit": 100,
        "title": "サポーター価格",
        "tier_label": "サポーター価格のベース枠",
        "limit_label": "先着100名限定",
        "price_line": "Proプラン 月額980円",
        "description": "サポーター枠の基本プランです。継続中はずっと980円で利用できます。この枠の中から先着30名だけ、最初の6か月が500円になる特別特典もあります。",
        "identity_copy": "あなたは応援サポーター枠です",
        "savings_note": "通常のProより月額520円お得です。",
        "expert_upgrade_note": "サポーター価格でご利用中の方は、必要になったタイミングで Expert を優待価格（月額1480円）でご利用いただけます。",
        "cta_label": "この価格を選ぶ",
        "badge": "継続しやすい定番",
        "highlight": False,
    },
]


def get_supporter_campaigns(user_id=None):
    claim_counts = get_supporter_campaign_claim_counts()
    user_claim = get_user_supporter_campaign_claim(user_id) if user_id else None
    campaigns = []
    supporter_family_claimed = int(claim_counts.get("supporter", 0)) + int(claim_counts.get("special-supporter", 0))

    for campaign in SUPPORTER_CAMPAIGNS:
        item = campaign.copy()
        limit = int(item.get("limit") or 0)
        if item["slug"] == "supporter":
            used = supporter_family_claimed
        else:
            used = int(claim_counts.get(item["slug"], 0))
        item["claimed_count"] = used
        item["spots_left"] = max(limit - used, 0)
        item["is_full"] = item["spots_left"] <= 0
        item["is_claimed"] = bool(
            user_claim and (
                user_claim.get("campaign_slug") == item["slug"]
                or (item["slug"] == "supporter" and user_claim.get("campaign_slug") == "special-supporter")
            )
        )
        campaigns.append(item)

    return campaigns


def get_supporter_campaign_by_slug(campaign_slug: str):
    slug = (campaign_slug or "").strip().lower()
    for campaign in SUPPORTER_CAMPAIGNS:
        if campaign["slug"] == slug:
            return campaign.copy()
    return None


def get_supporter_benefits(user_id=None):
    claim = get_user_supporter_campaign_claim(user_id) if user_id else None
    return {
        "claim": claim,
        "expert_monthly_price": 1480 if claim else 4980,
        "expert_upgrade_eligible": bool(claim),
    }


def get_supporter_offer_state(user_id=None):
    campaigns = {c["slug"]: c for c in get_supporter_campaigns(user_id)}
    base = campaigns.get("supporter")
    special = campaigns.get("special-supporter")
    if not base or not special:
        return None

    user_claim = get_user_supporter_campaign_claim(user_id) if user_id else None
    return {
        "slug": "supporter",
        "title": "応援サポーター枠",
        "tier_label": "通常1500円 → サポーター価格980円",
        "price_line": "月額980円",
        "identity_copy": "あなたは応援サポーター枠を選択中です",
        "savings_note": "通常のProより月額520円お得です。",
        "description": "先着30名だけ、さらに最初の6か月は月額500円の特別価格が適用されます。",
        "regular_price": 1500,
        "supporter_price": 980,
        "special_price": 500,
        "special_months": 6,
        "supporter_spots_left": base["spots_left"],
        "special_spots_left": special["spots_left"],
        "spots_left": base["spots_left"],
        "supporter_is_full": base["is_full"],
        "special_is_full": special["is_full"],
        "expert_price": 1480,
        "is_claimed": bool(user_claim and user_claim.get("campaign_slug") in ("supporter", "special-supporter")),
        "has_special_applied": bool(user_claim and user_claim.get("campaign_slug") == "special-supporter"),
        "claim_slug": (user_claim or {}).get("campaign_slug", ""),
    }


def get_campaign_display(campaign_slug: str, user_id=None):
    if not campaign_slug:
        return None
    slug = (campaign_slug or "").strip().lower()
    if slug in ("supporter", "special-supporter"):
        return get_supporter_offer_state(user_id)
    for campaign in get_supporter_campaigns(user_id):
        if campaign["slug"] == slug:
            return campaign
    return None


def claim_supporter_campaign_for_user(user_id: int, campaign_slug: str):
    requested_slug = (campaign_slug or "").strip().lower()
    if requested_slug not in ("supporter", "special-supporter"):
        return False, "invalid"

    user = get_user_by_id(user_id)
    effective_plan = get_user_plan(user)
    if effective_plan in ("pro", "expert"):
        return False, "already_pro"

    if get_user_supporter_campaign_claim(user_id):
        return False, "already_claimed"

    campaigns = {c["slug"]: c for c in get_supporter_campaigns(user_id)}
    base_campaign = campaigns.get("supporter")
    special_campaign = campaigns.get("special-supporter")
    if not base_campaign:
        return False, "invalid"

    if base_campaign["is_full"]:
        return False, "sold_out"

    actual_slug = "supporter"
    if special_campaign and not special_campaign["is_full"]:
        actual_slug = "special-supporter"

    campaign = get_supporter_campaign_by_slug(actual_slug)
    if not campaign:
        return False, "invalid"

    ok, reason, _claim = claim_supporter_campaign(
        user_id=user_id,
        campaign_slug=campaign["slug"],
        campaign_limit=int(campaign["limit"]),
    )
    return ok, reason if not ok else campaign["slug"]


def get_private_promo_offer_display(promo_code: str):
    code = (promo_code or "").strip().upper()
    if not code:
        return None
    row = get_friend_promo_code(code)
    if not row or not int(row.get("is_active") or 0):
        return None
    plan_to_grant = (row.get("plan_to_grant") or "pro").strip().lower()
    plan_label = "Expert" if plan_to_grant == "expert" else "Pro"
    if code.startswith("MASTER-"):
        return {
            "code": code,
            "kind": "lifetime",
            "label": "マスター枠",
            "title": "あなたはマスター枠です",
            "price_line": f"{plan_label}プランを永年無料",
            "description": "このURLは運営用の専用マスター枠です。メールアドレスを入力して登録またはログインすると、この枠がそのアカウントに適用されます。",
        }
    if int(row.get("grant_lifetime") or 0):
        return {
            "code": code,
            "kind": "lifetime",
            "label": "特別永年無料枠",
            "title": "あなたは特別永年無料枠です",
            "price_line": f"{plan_label}プランを永年無料",
            "description": "このURLに紐づく招待内容です。メールアドレスを入力して登録またはログインすると、この特別枠がそのアカウントに適用されます。",
        }
    return {
        "code": code,
        "kind": "timed",
        "label": "特別招待枠",
        "title": "あなたは特別招待枠です",
        "price_line": f"{plan_label}プランを{int(row.get('free_days') or 0)}日間無料",
        "description": "このURLに紐づく招待内容です。メールアドレスを入力して登録またはログインすると、この特別枠がそのアカウントに適用されます。",
    }


def get_active_promo_display(user):
    if not user:
        return None
    promo_code = (user.get("promo_code_used") or "").strip().upper()
    promo_plan = (user.get("promo_plan") or "").strip().lower()
    if promo_plan not in ("pro", "expert") or not promo_code:
        return None
    plan_label = "Expert" if promo_plan == "expert" else "Pro"
    if int(user.get("promo_is_lifetime") or 0) and promo_code.startswith("MASTER-"):
        return {
            "kind": "lifetime",
            "label": "マスター枠",
            "title": "あなたはマスター枠です",
            "price_line": f"{plan_label}プランを永年無料で利用中",
            "description": "このアカウントには運営用の専用マスター枠が適用されています。",
        }
    if int(user.get("promo_is_lifetime") or 0):
        return {
            "kind": "lifetime",
            "label": "特別永年無料枠",
            "title": "あなたは特別永年無料枠です",
            "price_line": f"{plan_label}プランを永年無料で利用中",
            "description": "この特別招待は継続中です。一般公開されていない招待枠が適用されています。",
        }
    promo_ends_at = (user.get("promo_ends_at") or "").strip()
    if promo_ends_at:
        try:
            today = datetime.now().date()
            end = datetime.strptime(promo_ends_at, "%Y-%m-%d").date()
            diff = (end - today).days
            if diff >= 0:
                return {
                    "kind": "timed",
                    "label": "特別招待枠",
                    "title": "あなたは特別招待枠です",
                    "price_line": f"{plan_label}プランをあと{diff}日無料で利用中",
                    "description": "この特別招待は限られた方だけに適用されています。期間内は無料でご利用いただけます。",
                }
        except ValueError:
            pass
    return None


def is_valid_email_address(email: str) -> bool:
    value = (email or "").strip().lower()
    return bool(re.match(r"^[^@\s]+@[^@\s]+\.[^@\s]+$", value))


def reserve_friend_promo_for_email(promo_code: str, email: str):
    code = (promo_code or "").strip().upper()
    user_email = (email or "").strip().lower()
    if not code:
        return False, "empty"
    if not is_valid_email_address(user_email):
        return False, "invalid_email"

    code_row = get_friend_promo_code(code)
    if not code_row:
        return False, "not_found"
    if not int(code_row.get("is_active") or 0):
        return False, "inactive"
    if int(code_row.get("used_count") or 0) >= int(code_row.get("max_uses") or 1):
        return False, "limit_reached"

    target_email = (code_row.get("target_email") or "").strip().lower()
    if target_email and target_email != user_email:
        return False, "email_mismatch"
    if not target_email:
        reserved = set_friend_promo_target_email(code_row["id"], user_email)
        if not reserved:
            refreshed = get_friend_promo_code(code)
            refreshed_target = (refreshed.get("target_email") or "").strip().lower() if refreshed else ""
            if refreshed_target and refreshed_target != user_email:
                return False, "email_mismatch"
    return True, "ok"


def has_active_private_promo(user):
    return get_active_promo_display(user) is not None


def is_master_user(user) -> bool:
    if not user:
        return False
    promo_code = (user.get("promo_code_used") or "").strip().upper()
    return promo_code.startswith("MASTER-") and int(user.get("promo_is_lifetime") or 0) == 1


def require_master_user(request: Request):
    current_user = get_current_user(request)
    if not current_user:
        return None, RedirectResponse("/login", status_code=303)
    user = get_user_by_id(current_user["id"])
    user = check_trial_expired(user)
    if not is_master_user(user):
        return user, RedirectResponse("/plans", status_code=303)
    return user, None


def get_master_article_candidates(limit: int = 18) -> list[dict]:
    seen: set[str] = set()
    candidates: list[dict] = []
    min_clinical_score = 4.3

    def push(paper: dict, source_kind: str):
        pid = str(paper.get("pubmed_id") or paper.get("id") or "").strip()
        if not pid or pid in seen:
            return
        if not ((paper.get("summary_jp") or "").strip() or (paper.get("abstract") or "").strip()):
            return
        score_value = _safe_float(paper.get("clinical_score")) or 0.0
        if score_value < min_clinical_score:
            return
        seen.add(pid)
        item = dict(paper)
        item["pubmed_id"] = pid
        item["source_kind"] = source_kind
        item["clinical_score_value"] = score_value
        item["likes_value"] = int(paper.get("likes") or 0)
        candidates.append(item)

    for paper in get_public_papers():
        push(paper, "public")
    for paper in get_all_saved_papers():
        push(paper, "global")

    candidates.sort(
        key=lambda p: (
            p.get("source_kind") == "public",
            p.get("clinical_score_value") or 0.0,
            p.get("likes_value") or 0,
            p.get("created_at") or "",
        ),
        reverse=True,
    )
    return candidates[:limit]


def get_master_article_candidate(pubmed_id: str) -> dict | None:
    target = str(pubmed_id or "").strip()
    if not target:
        return None
    for paper in get_master_article_candidates(limit=80):
        if str(paper.get("pubmed_id") or "") == target:
            return paper
    return None


def _slugify_article_title(text: str) -> str:
    value = (text or "").strip().lower()
    value = re.sub(r"<[^>]+>", "", value)
    value = re.sub(r"[^a-z0-9]+", "-", value)
    value = re.sub(r"-{2,}", "-", value).strip("-")
    return value[:80] or f"rehabilitation-paper-{int(time.time())}"


def _extract_tagged_block(text: str, heading: str) -> str:
    pattern = rf"【{re.escape(heading)}】\s*(.*?)(?=\n【|\Z)"
    match = re.search(pattern, text, flags=re.S)
    return match.group(1).strip() if match else ""


def _parse_geo_score(text: str) -> int:
    value = _extract_tagged_block(text, "SCORE")
    digits = re.findall(r"\d+", value or "")
    if not digits:
        patterns = [
            r"(?:SCORE|スコア|点数|評価)\D{0,10}(100|[1-9]?\d)",
            r"\b(100|[1-9]?\d)\s*(?:点|/100)\b",
        ]
        for pattern in patterns:
            match = re.search(pattern, text or "", flags=re.IGNORECASE)
            if match:
                digits = [match.group(1)]
                break
    if not digits:
        return 0
    score = int(digits[0])
    return max(0, min(100, score))


def _plain_text_from_html(value: str) -> str:
    text = re.sub(r"<[^>]+>", " ", value or "")
    return re.sub(r"\s+", " ", text).strip()


def _contains_any(text: str, keywords: list[str]) -> bool:
    normalized = text or ""
    return any(keyword in normalized for keyword in keywords)


def _ensure_geo_article_sections(article_html: str, source: dict) -> str:
    html = (article_html or "").strip()
    if not html:
        return html

    today_label = datetime.now().strftime("%Y-%m-%d")
    source_title = (
        source.get("source_jp_title")
        or source.get("jp_title")
        or source.get("source_title")
        or source.get("title")
        or "元論文"
    ).strip()
    source_title_en = (
        source.get("source_title")
        or source.get("title")
        or ""
    ).strip()
    source_pubmed_id = (source.get("pubmed_id") or "").strip()
    source_clinical_score = (source.get("source_clinical_score") or source.get("clinical_score") or "").strip()
    source_clinical_reason = (source.get("source_clinical_reason") or source.get("clinical_reason") or "").strip()

    plain_text = _plain_text_from_html(html)

    if not re.search(r"<h2[^>]*>\s*(よくある質問|FAQ|よくある疑問)\s*</h2>", html, flags=re.IGNORECASE):
        html += """
<h2>よくある質問</h2>
<h3>この結果はすべての患者にそのまま当てはまりますか？</h3>
<p>結論として、そのまま当てはまるとは限りません。論文の対象条件、介入内容、評価指標、追跡期間が自施設の患者像と一致するかを先に確認する必要があります。</p>
<h3>臨床で最初に確認したいポイントは何ですか？</h3>
<p>対象、介入、結果、限界の4点を近い位置で確認することが重要です。特に対象条件と評価指標が自施設で再現できるかを先に見ると判断しやすくなります。</p>
"""
    if not re.search(r"<h2[^>]*>\s*(注意点と限界|注意点|限界)\s*</h2>", html, flags=re.IGNORECASE):
        html += """
<h2>注意点と限界</h2>
<p>結論として、今回の論文情報は臨床判断の参考情報です。対象条件や研究デザインが異なる場合は同じ結果にならない可能性があるため、適用前に患者背景と評価条件を確認する必要があります。</p>
"""
    if not re.search(r"<h2[^>]*>\s*(参考情報|参考文献|出典)\s*</h2>", html, flags=re.IGNORECASE):
        html += f"""
<h2>参考情報</h2>
<ul>
  <li>元論文: {source_title}</li>
  {f"<li>英語タイトル: {source_title_en}</li>" if source_title_en and source_title_en != source_title else ""}
  {f"<li>PMID: {source_pubmed_id}</li>" if source_pubmed_id else ""}
  {f"<li>臨床参考度: {source_clinical_score}</li>" if source_clinical_score else ""}
  {f"<li>参考理由: {source_clinical_reason}</li>" if source_clinical_reason else ""}
</ul>
"""
    if not re.search(r"<h2[^>]*>\s*(更新情報|更新日|最終更新)\s*</h2>", html, flags=re.IGNORECASE):
        html += f"""
<h2>更新情報</h2>
<ul>
  <li>最終更新日: {today_label}</li>
  <li>版: v1.0</li>
  <li>この記事は公開論文情報をもとに作成しています。新しい研究やガイドラインが出た場合は内容の見直しが必要です。</li>
</ul>
"""
    return html


def _build_geo_feedback_lines(article_title: str, article_excerpt: str, article_html: str) -> list[str]:
    plain_text = _plain_text_from_html(article_html or "")
    intro_text = plain_text[:240]
    lines: list[str] = []
    question_h2_count = len(
        re.findall(
            r"<h2[^>]*>.*?(何ですか|なぜ|どんな|どう|違い|比較|注意点|限界|質問|FAQ).*?</h2>",
            article_html or "",
            flags=re.IGNORECASE | re.S,
        )
    )

    if len(article_title or "") < 18:
        lines.append("記事タイトルが短めです。検索意図が伝わる語句をもう少し足すと強くなります。")
    elif len(article_title or "") > 46:
        lines.append("記事タイトルがやや長めです。要点を保ったまま少し圧縮すると見出し力が上がります。")
    else:
        lines.append("記事タイトルは検索意図が伝わりやすい長さに収まっています。")

    if len(article_excerpt or "") < 60:
        lines.append("抜粋が短いので、対象・結果・臨床への意味を一文で補うと引用されやすくなります。")
    else:
        lines.append("抜粋は記事の価値を短時間で伝えられる構成です。")

    if not _contains_any(intro_text, ["結論", "とは", "重要", "判断", "ポイント"]):
        lines.append("冒頭の結論ファーストが弱めです。冒頭2〜4文で結論・対象範囲・得られることを先に示すと強くなります。")
    else:
        lines.append("冒頭で要点が先に伝わっており、AIにも人にも理解しやすい構成です。")

    h2_count = len(re.findall(r"<h2\b", article_html or "", flags=re.IGNORECASE))
    if h2_count < 5:
        lines.append("h2見出しが少ないため、読者の疑問ごとに章を分けると GEO で拾われやすくなります。")
    else:
        lines.append("h2見出しで論点が整理されていて、構造は良好です。")

    if question_h2_count < 3:
        lines.append("質問文や定義文の見出しが少ないため、AIが回答候補として理解しやすい見出しに寄せると安定します。")
    else:
        lines.append("質問文・定義文の見出しが多く、AIが意味を拾いやすい構成です。")

    if not re.search(r"(よくある質問|FAQ|疑問|Q&A|確認したいポイント)", plain_text, flags=re.IGNORECASE):
        lines.append("FAQ的な疑問への先回りを1節入れると、生成AIに引用されやすくなります。")
    else:
        lines.append("読者の疑問に先回りする要素が入っており、引用されやすい構成です。")

    if not re.search(r"(注意点と限界|限界|例外|注意が必要)", plain_text):
        lines.append("注意点や例外条件を明示すると、誤解されにくく信頼性も上がります。")
    else:
        lines.append("注意点と限界が明示されていて、断定を避けられています。")

    if not re.search(r"(参考情報|参考文献|出典|PMID|更新情報|最終更新)", plain_text):
        lines.append("参考情報や更新情報を明示すると、信頼シグナルが強くなります。")
    else:
        lines.append("参考情報や更新情報が入り、信頼シグナルが確保されています。")

    return lines[:6]


def _estimate_geo_score_locally(draft: dict) -> dict:
    article_title = (draft.get("article_title") or "").strip()
    article_excerpt = (draft.get("article_excerpt") or "").strip()
    article_html = _ensure_geo_article_sections((draft.get("article_html") or "").strip(), draft)
    plain_text = _plain_text_from_html(article_html)
    intro_text = plain_text[:280]

    score = 46
    if 18 <= len(article_title) <= 46:
        score += 8
    elif article_title:
        score += 3

    if 60 <= len(article_excerpt) <= 140:
        score += 7
    elif article_excerpt:
        score += 3

    h2_count = len(re.findall(r"<h2\b", article_html, flags=re.IGNORECASE))
    h3_count = len(re.findall(r"<h3\b", article_html, flags=re.IGNORECASE))
    p_count = len(re.findall(r"<p\b", article_html, flags=re.IGNORECASE))
    list_count = len(re.findall(r"<li\b", article_html, flags=re.IGNORECASE))
    table_count = len(re.findall(r"<table\b", article_html, flags=re.IGNORECASE))
    ol_count = len(re.findall(r"<ol\b", article_html, flags=re.IGNORECASE))
    question_h2_count = len(
        re.findall(
            r"<h2[^>]*>.*?(何ですか|なぜ|どんな|どう|違い|比較|注意点|限界|質問|FAQ).*?</h2>",
            article_html,
            flags=re.IGNORECASE | re.S,
        )
    )

    if _contains_any(intro_text, ["結論", "とは", "重要", "判断", "ポイント"]):
        score += 8

    score += min(h2_count, 6) * 3
    score += min(h3_count, 4) * 2
    score += min(p_count, 10)
    if list_count >= 3:
        score += 5
    if table_count >= 1:
        score += 5
    if ol_count >= 1:
        score += 3
    if question_h2_count >= 3:
        score += 6

    text_length = len(plain_text)
    if 1800 <= text_length <= 5000:
        score += 10
    elif 1200 <= text_length < 1800 or 5000 < text_length <= 6500:
        score += 6
    elif text_length > 0:
        score += 2

    if draft.get("source_clinical_score"):
        score += 4
    if draft.get("source_clinical_reason"):
        score += 4
    if re.search(r"(臨床で確認したいポイント|現場での活かし方|臨床で注目したい理由)", plain_text):
        score += 6
    if re.search(r"(対象|結果|介入|評価指標)", plain_text):
        score += 5
    if re.search(r"(よくある質問|FAQ|疑問|Q&A|確認したいポイント)", plain_text, flags=re.IGNORECASE):
        score += 4
    if re.search(r"(注意点と限界|限界|例外|注意が必要)", plain_text):
        score += 4
    if re.search(r"(参考情報|参考文献|出典|PMID)", plain_text):
        score += 4
    if re.search(r"(更新情報|更新日|最終更新|版: v1\\.0)", plain_text):
        score += 3

    score = max(55, min(96, score))
    feedback = "\n".join(_build_geo_feedback_lines(article_title, article_excerpt, article_html))
    return {
        "score": score,
        "feedback": feedback,
        "title": article_title or "記事下書き",
        "excerpt": article_excerpt,
        "slug": draft.get("article_slug") or _slugify_article_title(article_title or "記事下書き"),
        "html": article_html,
        "raw": "",
    }


def _fallback_master_article(paper: dict) -> dict:
    title = (paper.get("jp_title") or paper.get("title") or "リハビリ論文の要点").strip()
    summary = (paper.get("summary_jp") or "").strip()
    abstract = (paper.get("abstract") or "").strip()
    excerpt = f"{title} とは何か、なぜ臨床で重要か、どの患者に当てはめる時に注意が必要かを整理した下書きです。"
    body_parts = [
        f"<p>結論として、{title} は臨床での判断材料になりやすい論文です。理由は、対象・介入・結果を順に確認しやすく、明日の臨床へつなげる視点を整理しやすいからです。ただし、対象条件が異なる場合は適用判断が変わる点に注意が必要です。</p>",
        "<h2>この論文は何を示していますか？</h2>",
        f"<p>結論として、この論文の要点は次のとおりです。{summary or abstract or '論文要約はこれから生成されます。'}</p>",
        "<h2>なぜ臨床で重要なのですか？</h2>",
        "<p>結論として、論文の結果を読むだけでなく、対象条件・介入内容・評価指標・追跡期間を近い位置で確認できることが重要です。これにより、自施設で再現できる部分と慎重に見るべき部分を切り分けやすくなります。</p>",
    ]
    if paper.get("clinical_reason"):
        body_parts.extend([
            "<h2>臨床で注目したい理由は何ですか？</h2>",
            f"<p>結論として、注目点は {paper.get('clinical_reason')} です。理由が実際の患者像に近いかを確認すると、論文の使いどころが明確になります。</p>",
        ])
    body_parts.extend([
        "<h2>現場ではどう活かせますか？</h2>",
        "<p>結論として、対象、介入、評価指標、期間の順で確認するのが実践的です。まず対象条件が近いかを見て、次に介入の再現可能性、最後に評価指標が自施設で追えるかを確認します。例外として、研究環境と自施設の体制差が大きい場合は、そのまま適用しない判断も必要です。</p>",
    ])
    html = _ensure_geo_article_sections("\n".join(body_parts), paper)
    return {
        "title": title,
        "excerpt": excerpt,
        "slug": _slugify_article_title(title),
        "html": html,
        "raw": "",
    }


def generate_master_geo_article(paper: dict) -> dict:
    title_en = (paper.get("title") or "").strip()
    title_jp = (paper.get("jp_title") or "").strip()
    abstract = (paper.get("abstract") or "").strip()
    summary_jp = (paper.get("summary_jp") or "").strip()
    clinical_score = (paper.get("clinical_score") or "").strip()
    clinical_reason = (paper.get("clinical_reason") or "").strip()
    today_label = datetime.now().strftime("%Y-%m-%d")

    prompt = f"""
以下の医学論文情報をもとに、日本の理学療法士・作業療法士向けに、WordPressへ掲載できるGEO/AIO重視の記事下書きを日本語で作成してください。

条件:
- 医療記事として誇張しない
- abstractや要約にない断定は避ける
- 読みやすく、プロライターが書いたような自然な文体
- HTMLのみで本文を書く
- 使ってよいタグは h2, h3, p, ul, ol, li, strong, table, thead, tbody, tr, th, td のみ
- 文字数は本文全体で 2500〜4500字程度を目安
- 結論ファーストで始める
- 冒頭は2〜4文で「結論」「対象範囲」「この記事で得られること」を明示する
- h2見出しはできるだけ質問文か定義文にする
- 各章冒頭は「結論1文 → 理由2〜3個 → 例外/注意1つ」を意識する
- 主張の近くに根拠を置く
- FAQ、注意点と限界、参考情報、更新情報を入れる
- 表・チェックリスト・手順のいずれかを1つ以上入れる
- AIに要点が引用されやすく、人間は続きが読みたくなるように、冒頭で答えを出しつつ詳細条件は本文で丁寧に説明する
- 最後に「この記事を読んだあとに確認したいポイント」を箇条書きで入れる
- 更新情報には {today_label} / v1.0 を入れる

必ず次の形式で出力してください。

【TITLE】
記事タイトル

【EXCERPT】
120字以内の抜粋

【SLUG】
半角英小文字・数字・ハイフンのみ

【HTML】
ここに本文HTML

推奨する章構成:
- 導入: 結論・対象範囲・この記事で得られること
- 〇〇とは何ですか？
- なぜ臨床で重要なのですか？
- どんなケースに適していますか？
- 実践で確認したいポイント
- 他の考え方との違い、または判断の目安
- よくある質問
- 注意点と限界
- 参考情報
- 更新情報

論文タイトル（英語）:
{title_en}

論文タイトル（日本語）:
{title_jp}

日本語要約:
{summary_jp}

臨床参考度:
{clinical_score}

臨床参考度の理由:
{clinical_reason}

abstract:
{abstract}
"""

    try:
        response = client.responses.create(
            model=os.getenv("MASTER_ARTICLE_MODEL", "gpt-4.1"),
            input=prompt,
        )
        raw = response.output_text.strip()
        article_title = _extract_tagged_block(raw, "TITLE")
        article_excerpt = _extract_tagged_block(raw, "EXCERPT")
        article_slug = _extract_tagged_block(raw, "SLUG")
        article_html = _extract_tagged_block(raw, "HTML")
        if not article_title or not article_html:
            raise ValueError("missing article sections")
        article_html = _ensure_geo_article_sections(article_html, paper)
        return {
            "title": article_title,
            "excerpt": article_excerpt[:180],
            "slug": _slugify_article_title(article_slug or article_title),
            "html": article_html,
            "raw": raw,
        }
    except Exception:
        return _fallback_master_article(paper)


def review_and_improve_master_geo_article(draft: dict) -> dict:
    prompt = f"""
以下の日本語記事下書きを、GEO（生成AIに選ばれやすく、かつ検索意図に強い記事品質）重視で100点満点で評価してください。
そのうえで、95点未満なら95点以上を狙えるようにタイトル・抜粋・HTML本文を改善してください。
95点以上でも、必要なら軽く磨いてください。

採点は次の10項目を各10点で考えてください:
- 冒頭で結論が伝わるか
- 用語やテーマの定義が明確か
- 検索意図と読者の疑問に合っているか
- 主張の近くに根拠があるか
- 実践手順や判断の目安があるか
- 具体例や比較があるか
- FAQで先回りできているか
- 注意点・限界・例外があるか
- 参考情報・更新情報など信頼シグナルがあるか
- 一文一義でAIが要約しやすいか

ルール:
- 日本語で出力
- 本文は HTML のみ
- 使ってよいタグは h2, h3, p, ul, ol, li, strong, table, thead, tbody, tr, th, td のみ
- abstractや元論文情報にない断定は避ける
- 見出しや導入を必要に応じて改善してよい
- h2見出しは質問文か定義文を優先する
- FAQ、注意点と限界、参考情報、更新情報が弱ければ補う
- SEOだけでなく、AIが引用しやすい「切り出しても意味が崩れない段落」を作る

必ず次の形式で出力してください。

【SCORE】
0〜100の整数

【FEEDBACK】
改善観点を3〜6行で簡潔に

【TITLE】
改善後の記事タイトル

【EXCERPT】
改善後の抜粋

【SLUG】
半角英小文字・数字・ハイフンのみ

【HTML】
改善後の本文HTML

元論文タイトル:
{draft.get("source_jp_title") or draft.get("source_title") or ""}

元論文の臨床参考度:
{draft.get("source_clinical_score") or ""}

元論文の参考理由:
{draft.get("source_clinical_reason") or ""}

現状の記事タイトル:
{draft.get("article_title") or ""}

現状の抜粋:
{draft.get("article_excerpt") or ""}

現状の本文HTML:
{draft.get("article_html") or ""}
"""

    try:
        response = client.responses.create(
            model=os.getenv("MASTER_ARTICLE_REVIEW_MODEL", os.getenv("MASTER_ARTICLE_MODEL", "gpt-4.1")),
            input=prompt,
        )
        raw = response.output_text.strip()
        score = _parse_geo_score(raw)
        feedback = _extract_tagged_block(raw, "FEEDBACK")
        title = _extract_tagged_block(raw, "TITLE") or (draft.get("article_title") or "")
        excerpt = _extract_tagged_block(raw, "EXCERPT") or (draft.get("article_excerpt") or "")
        slug = _extract_tagged_block(raw, "SLUG") or (draft.get("article_slug") or "")
        html = _extract_tagged_block(raw, "HTML") or (draft.get("article_html") or "")
        html = _ensure_geo_article_sections(html, draft)
        if not title or not html:
            raise ValueError("missing review output")
        heuristic = _estimate_geo_score_locally(
            {
                **draft,
                "article_title": title,
                "article_excerpt": excerpt,
                "article_slug": slug,
                "article_html": html,
            }
        )
        if score <= 0:
            score = int(heuristic["score"])
        if not feedback.strip():
            feedback = heuristic["feedback"]
        return {
            "score": score,
            "feedback": feedback,
            "title": title,
            "excerpt": excerpt,
            "slug": _slugify_article_title(slug or title),
            "html": html,
            "raw": raw,
        }
    except Exception as exc:
        heuristic = _estimate_geo_score_locally(draft)
        return {
            "score": int(heuristic["score"]),
            "feedback": f"{heuristic['feedback']}\n\nGEO診断APIを実行できなかったため、現在はローカル診断で表示しています。({exc})",
            "title": heuristic["title"],
            "excerpt": heuristic["excerpt"],
            "slug": heuristic["slug"],
            "html": heuristic["html"],
            "raw": "",
        }


def optimize_master_article_to_target(draft: dict, target_score: int = 95, max_rounds: int = 3) -> dict:
    working = dict(draft)
    latest = {
        "score": int(working.get("geo_score") or 0),
        "feedback": working.get("geo_feedback") or "",
        "title": working.get("article_title") or "",
        "excerpt": working.get("article_excerpt") or "",
        "slug": working.get("article_slug") or "",
        "html": working.get("article_html") or "",
        "raw": "",
    }
    feedbacks: list[str] = []

    for _ in range(max_rounds):
        reviewed = review_and_improve_master_geo_article(working)
        latest = reviewed
        if reviewed.get("feedback"):
            feedbacks.append(str(reviewed["feedback"]).strip())
        working.update(
            {
                "article_title": reviewed["title"],
                "article_excerpt": reviewed["excerpt"],
                "article_slug": reviewed["slug"],
                "article_html": reviewed["html"],
                "geo_score": reviewed["score"],
                "geo_feedback": reviewed["feedback"],
            }
        )
        if int(reviewed.get("score") or 0) >= target_score:
            break

    latest["feedback"] = "\n\n".join([f for f in feedbacks if f]).strip() or latest.get("feedback") or ""
    return latest


def ensure_master_article_geo_review(draft: dict | None, force: bool = False) -> dict | None:
    if not draft:
        return None

    needs_review = force or int(draft.get("geo_score") or 0) <= 0 or not str(draft.get("geo_feedback") or "").strip()
    if not needs_review:
        return draft

    optimized = optimize_master_article_to_target(draft)
    update_master_article_draft_geo_review(
        draft_id=draft["id"],
        geo_score=optimized["score"],
        geo_feedback=optimized["feedback"],
        article_title=optimized["title"],
        article_excerpt=optimized["excerpt"],
        article_slug=optimized["slug"],
        article_html=optimized["html"],
    )
    return get_master_article_draft(draft["id"])


def _normalize_wordpress_site_url(site_url: str) -> str:
    normalized = (site_url or "").strip()
    if not normalized:
        return ""
    if not re.match(r"^https?://", normalized, flags=re.IGNORECASE):
        normalized = f"https://{normalized.lstrip('/')}"
    return normalized.rstrip("/")


def _hash_client_value(value: str) -> str:
    return hashlib.sha256((value or "").encode("utf-8")).hexdigest()[:24] if value else ""


def _normalize_wordpress_app_password(app_password: str) -> str:
    return re.sub(r"\s+", "", (app_password or "").strip())


def get_app_base_url_for_user(user_id: int | None = None, request: Request | None = None) -> str:
    if user_id:
        saved = get_master_wordpress_settings(user_id)
        if saved and saved.get("app_base_url"):
            return _normalize_wordpress_site_url(saved.get("app_base_url") or "")

    env_url = _normalize_wordpress_site_url(os.getenv("APP_BASE_URL", ""))
    if env_url:
        return env_url

    if request is not None:
        return str(request.base_url).rstrip("/")

    return ""


def get_wordpress_config_for_user(user_id: int | None = None) -> dict | None:
    if user_id:
        saved = get_master_wordpress_settings(user_id)
        if saved and saved.get("site_url") and saved.get("username") and saved.get("app_password"):
            return {
                "site_url": _normalize_wordpress_site_url(saved.get("site_url") or ""),
                "username": (saved.get("username") or "").strip(),
                "app_password": _normalize_wordpress_app_password(saved.get("app_password") or ""),
                "source": "saved",
            }

    env_site_url = _normalize_wordpress_site_url(os.getenv("WP_SITE_URL", ""))
    env_username = (os.getenv("WP_USERNAME", "") or "").strip()
    env_password = _normalize_wordpress_app_password(os.getenv("WP_APP_PASSWORD", ""))
    if env_site_url and env_username and env_password:
        return {
            "site_url": env_site_url,
            "username": env_username,
            "app_password": env_password,
            "source": "env",
        }
    return None


def is_wordpress_configured(user_id: int | None = None) -> bool:
    return bool(get_wordpress_config_for_user(user_id))


def get_master_marketing_variant_meta(draft: dict | None) -> dict:
    variant = ((draft or {}).get("marketing_variant") or "A").strip().upper() or "A"
    source_title = (
        (draft or {}).get("source_jp_title")
        or (draft or {}).get("source_title")
        or (draft or {}).get("article_title")
        or "このテーマ"
    ).strip()
    source_title_short = source_title[:32] + ("..." if len(source_title) > 32 else "")
    clinical_score = _safe_float((draft or {}).get("source_clinical_score")) or 0.0
    clinical_line = (
        f"臨床参考度 {clinical_score:.1f} の高評価論文に近い研究も続けて探せます。"
        if clinical_score >= 4.3
        else "近いテーマの論文も続けて見つけやすくなります。"
    )

    variants = {
        "A": {
            "label": "理解短縮型",
            "headline": "英語論文を日本語で最短で理解したい方へ",
            "copy": f"『{source_title_short}』のような論文も、日本語要約・臨床ポイント・保存まで一気に進められます。{clinical_line}",
            "button_label": "無料で日本語要約を試す",
            "bullets": [
                "英語論文を日本語で要点把握",
                "結果と臨床活用をすぐ確認",
                "気になる論文をそのまま保存",
            ],
        },
        "B": {
            "label": "知識資産型",
            "headline": "読んだ論文を、あとで使える知識に変えたい方へ",
            "copy": f"『{source_title_short}』のような論文を、保存・メモ・フォルダ整理までつなげて知的資産化できます。",
            "button_label": "無料で保存とメモを始める",
            "bullets": [
                "論文ごとに保存フォルダを整理",
                "気づきを論文メモに残せる",
                "学習内容をあとから再利用しやすい",
            ],
        },
        "C": {
            "label": "継続活用型",
            "headline": "明日の臨床に使える論文を、継続的に見つけたい方へ",
            "copy": f"『{source_title_short}』に近いテーマを起点に、おすすめ論文や関連研究を継続的に再発見できます。",
            "button_label": "無料でおすすめ論文を受け取る",
            "bullets": [
                "興味に合う論文がおすすめに出る",
                "関連論文を再発見しやすい",
                "臨床で使えるテーマを継続的に追える",
            ],
        },
    }
    meta = variants.get(variant, variants["A"]).copy()
    meta["variant"] = variant
    return meta


def build_master_article_marketing_assets(draft: dict, user_id: int | None = None) -> dict:
    base_url = get_app_base_url_for_user(user_id)
    if not base_url or not draft.get("id"):
        return {
            "base_url": base_url,
            "variant": (draft.get("marketing_variant") or "A"),
            "click_url": "",
            "pixel_url": "",
            "cta_html": "",
            "pixel_html": "",
        }

    meta = get_master_marketing_variant_meta(draft)
    variant = meta["variant"]
    draft_id = int(draft["id"])
    click_url = f"{base_url}/go/master-article/{draft_id}?variant={variant}"
    pixel_url = f"{base_url}/track/master-article/{draft_id}.gif?variant={variant}"
    bullets_html = "\n".join([f"<li>{item}</li>" for item in meta["bullets"]])
    cta_html = f"""
<h2>臨床で使える論文を、もっと探しやすくしたい方へ</h2>
<p><strong>{meta["headline"]}</strong></p>
<p>{meta["copy"]}</p>
<ul>
{bullets_html}
</ul>
<p><a href="{click_url}">{meta["button_label"]}</a></p>
"""
    pixel_html = f'<img src="{pixel_url}" alt="" width="1" height="1" style="display:none;" />'
    return {
        "base_url": base_url,
        "variant": variant,
        "variant_label": meta["label"],
        "headline": meta["headline"],
        "copy": meta["copy"],
        "button_label": meta["button_label"],
        "click_url": click_url,
        "pixel_url": pixel_url,
        "cta_html": cta_html.strip(),
        "pixel_html": pixel_html,
    }


def inject_marketing_into_article_html(draft: dict, user_id: int | None = None) -> str:
    article_html = (draft.get("article_html") or "").strip()
    assets = build_master_article_marketing_assets(draft, user_id)
    extras = [block for block in [assets.get("cta_html"), assets.get("pixel_html")] if block]
    if not extras:
        return article_html
    return "\n".join([article_html, *extras]).strip()


def test_wordpress_connection(config: dict):
    endpoint = f"{config['site_url']}/wp-json/wp/v2/users/me?context=edit"
    auth = base64.b64encode(
        f"{config['username']}:{config['app_password']}".encode("utf-8")
    ).decode("ascii")
    req = _urlreq.Request(
        endpoint,
        headers={
            "Authorization": f"Basic {auth}",
            "Content-Type": "application/json",
        },
        method="GET",
    )
    try:
        with _urlreq.urlopen(req, timeout=20) as resp:
            data = json.loads(resp.read().decode("utf-8"))
        display_name = data.get("name") or data.get("slug") or config["username"]
        return True, f"WordPressへ接続できました。接続先: {config['site_url']} / {display_name}"
    except Exception as exc:
        return False, f"WordPress接続確認に失敗しました。{exc}"


def publish_master_article_to_wordpress(draft: dict, user_id: int | None = None):
    config = get_wordpress_config_for_user(user_id)
    if not config:
        return False, "WordPress接続設定が未設定です。"

    site_url = config["site_url"]
    username = config["username"]
    app_password = config["app_password"]
    endpoint = f"{site_url}/wp-json/wp/v2/posts"

    payload = {
        "title": draft.get("article_title") or "",
        "content": inject_marketing_into_article_html(draft, user_id),
        "excerpt": draft.get("article_excerpt") or "",
        "slug": draft.get("article_slug") or "",
        "status": "draft",
    }
    auth = base64.b64encode(f"{username}:{app_password}".encode("utf-8")).decode("ascii")
    req = _urlreq.Request(
        endpoint,
        data=json.dumps(payload).encode("utf-8"),
        headers={
            "Authorization": f"Basic {auth}",
            "Content-Type": "application/json",
        },
        method="POST",
    )
    try:
        with _urlreq.urlopen(req, timeout=20) as resp:
            data = json.loads(resp.read().decode("utf-8"))
        return True, data
    except Exception as exc:
        return False, str(exc)


def _normalize_master_autopost_time(value: str) -> str:
    raw = (value or "").strip()
    match = re.fullmatch(r"([01]?\d|2[0-3]):([0-5]\d)", raw)
    if not match:
        return "09:00"
    return f"{int(match.group(1)):02d}:{match.group(2)}"


def _build_master_autopost_summary(settings: dict | None) -> dict:
    current = settings or {}
    is_enabled = bool(int(current.get("is_enabled") or 0))
    daily_time = _normalize_master_autopost_time(current.get("daily_time") or "09:00")
    last_attempted_date = (current.get("last_attempted_date") or "").strip()
    last_success_date = (current.get("last_success_date") or "").strip()

    now = datetime.now()
    hour, minute = map(int, daily_time.split(":"))
    next_run = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
    if next_run <= now:
        next_run = next_run + timedelta(days=1)

    return {
        "is_enabled": is_enabled,
        "daily_time": daily_time,
        "last_attempted_date": last_attempted_date,
        "last_success_date": last_success_date,
        "next_run_label": next_run.strftime("%Y-%m-%d %H:%M"),
    }


def _build_master_article_marketing_summary_map(created_by_user_id: int) -> dict[int, dict]:
    summaries = {}
    for row in get_master_article_marketing_summary(created_by_user_id):
        impressions = int(row.get("impressions") or 0)
        clicks = int(row.get("clicks") or 0)
        registrations = int(row.get("registrations") or 0)
        paid_users = int(row.get("paid_users") or 0)
        ctr = round((clicks / impressions) * 100, 1) if impressions else 0.0
        reg_rate = round((registrations / clicks) * 100, 1) if clicks else 0.0
        paid_rate = round((paid_users / registrations) * 100, 1) if registrations else 0.0
        summaries[int(row["draft_id"])] = {
            "impressions": impressions,
            "clicks": clicks,
            "registrations": registrations,
            "paid_users": paid_users,
            "ctr": ctr,
            "reg_rate": reg_rate,
            "paid_rate": paid_rate,
        }
    return summaries


def _run_master_wordpress_autopost_for_user(user_id: int):
    user = get_user_by_id(user_id)
    if not user or not is_master_user(user):
        create_master_wordpress_autopost_log(
            user_id=user_id,
            draft_id=None,
            status="blocked",
            message="マスター枠ユーザーではないため、自動投稿を実行しませんでした。",
        )
        return False, "master_required", None

    if not is_wordpress_configured(user_id):
        create_master_wordpress_autopost_log(
            user_id=user_id,
            draft_id=None,
            status="blocked",
            message="WordPress接続設定が未設定のため、自動投稿を実行しませんでした。",
        )
        return False, "wp_not_configured", None

    draft = get_next_master_article_draft_for_autopost(user_id)
    if not draft:
        create_master_wordpress_autopost_log(
            user_id=user_id,
            draft_id=None,
            status="skipped",
            message="自動投稿できる未投稿の下書きがありませんでした。",
        )
        return False, "no_draft", None

    optimized = optimize_master_article_to_target(draft)
    update_master_article_draft_geo_review(
        draft_id=draft["id"],
        geo_score=optimized["score"],
        geo_feedback=optimized["feedback"],
        article_title=optimized["title"],
        article_excerpt=optimized["excerpt"],
        article_slug=optimized["slug"],
        article_html=optimized["html"],
    )
    draft = get_master_article_draft(draft["id"])

    ok, result = publish_master_article_to_wordpress(draft, user_id)
    if not ok:
        create_master_wordpress_autopost_log(
            user_id=user_id,
            draft_id=draft["id"],
            status="error",
            message=f"WordPress投稿に失敗しました。{result}",
        )
        return False, str(result), draft

    wordpress_post_id = str(result.get("id") or "")
    wordpress_status = str(result.get("status") or "draft")
    mark_master_article_wordpress_posted(
        draft_id=draft["id"],
        wordpress_post_id=wordpress_post_id,
        wordpress_status=wordpress_status,
    )
    create_master_wordpress_autopost_log(
        user_id=user_id,
        draft_id=draft["id"],
        status="success",
        message=f"GEOスコア {optimized['score']} 点の完成版を WordPress へ自動投稿しました。",
        wordpress_post_id=wordpress_post_id,
    )
    return True, wordpress_status, draft


def _run_master_autopost_scheduler_once(now: datetime | None = None):
    current = now or datetime.now()
    today_label = current.strftime("%Y-%m-%d")

    for settings in get_master_wordpress_autopost_enabled_settings():
        user_id = int(settings.get("user_id") or 0)
        if not user_id:
            continue

        daily_time = _normalize_master_autopost_time(settings.get("daily_time") or "09:00")
        run_hour, run_minute = map(int, daily_time.split(":"))
        if (current.hour, current.minute) < (run_hour, run_minute):
            continue
        if (settings.get("last_attempted_date") or "").strip() == today_label:
            continue

        with _master_autopost_scheduler_lock:
            refreshed = get_master_wordpress_autopost_settings(user_id) or {}
            if (refreshed.get("last_attempted_date") or "").strip() == today_label:
                continue
            ok, _message, _draft = _run_master_wordpress_autopost_for_user(user_id)
            update_master_wordpress_autopost_run_state(
                user_id=user_id,
                attempted_date=today_label,
                success_date=today_label if ok else None,
            )


def _master_autopost_scheduler_loop():
    while True:
        try:
            _run_master_autopost_scheduler_once()
        except Exception as exc:
            try:
                print(f"[master-autopost] scheduler error: {exc}")
            except Exception:
                pass
        time.sleep(45)


def redeem_friend_promo_for_user(user_id: int, promo_code: str):
    promo_code = (promo_code or "").strip().upper()
    if not promo_code:
        return False, "empty"

    user = get_user_by_id(user_id)
    if not user:
        return False, "login_required"

    if (user.get("promo_code_used") or "").strip():
        return False, "already_used"

    effective_plan = get_user_plan(user)
    if effective_plan in ("pro", "expert"):
        return False, "already_pro"

    code_row = get_friend_promo_code(promo_code)
    if not code_row:
        return False, "not_found"

    if not code_row["is_active"]:
        return False, "inactive"

    expires_at = (code_row.get("expires_at") or "").strip()
    if expires_at:
        today = datetime.now().date()
        try:
            exp = datetime.strptime(expires_at, "%Y-%m-%d").date()
            if today > exp:
                return False, "expired"
        except ValueError:
            pass

    if code_row["used_count"] >= code_row["max_uses"]:
        return False, "limit_reached"

    target_email = (code_row.get("target_email") or "").strip().lower()
    if target_email and target_email != (user.get("email") or "").strip().lower():
        return False, "email_mismatch"

    plan_to_grant = code_row.get("plan_to_grant") or "pro"
    today = datetime.now()
    if int(code_row.get("grant_lifetime") or 0):
        apply_lifetime_promo_to_user(
            user_id=user_id,
            plan=plan_to_grant,
            code=promo_code,
        )
    else:
        ends_at = (today + timedelta(days=int(code_row["free_days"]))).strftime("%Y-%m-%d")
        apply_promo_to_user(
            user_id=user_id,
            plan=plan_to_grant,
            ends_at=ends_at,
            code=promo_code,
        )
    use_friend_promo_code(code_row["id"])
    return True, "ok"


@app.on_event("startup")
def startup_event():
    global _master_autopost_scheduler_started
    init_db()
    init_memos_tables()
    if not _master_autopost_scheduler_started:
        threading.Thread(target=_master_autopost_scheduler_loop, daemon=True).start()
        _master_autopost_scheduler_started = True

def get_current_user(request: Request):
    user_id = request.session.get("user_id")

    if not user_id:
        return None

    return get_user_by_id(user_id)


def get_article_marketing_attribution(request: Request) -> dict:
    data = request.session.get("article_marketing_attribution") or {}
    if isinstance(data, dict):
        return data
    return {}


def clear_article_marketing_attribution(request: Request) -> None:
    try:
        request.session.pop("article_marketing_attribution", None)
    except Exception:
        pass

def get_user_plan(user):
    if not user:
        return "guest"

    today = datetime.now().date()

    # ① promo が有効なら最優先（users.plan は free のまま）
    if int(user.get("promo_is_lifetime") or 0):
        promo_plan = (user.get("promo_plan") or "").strip().lower()
        if promo_plan in ("pro", "expert"):
            return promo_plan

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
def set_plan(
    request: Request,
    plan: str = Form(...),
    campaign_slug: str = Form(""),
):
    current_user = get_current_user(request)

    if not current_user:
        return RedirectResponse("/login", status_code=303)

    user = get_user_by_id(current_user["id"])
    plan = (plan or "").strip().lower()
    campaign_slug = (campaign_slug or "").strip().lower()

    if plan not in ["free", "pro", "expert"]:
        return RedirectResponse("/plans", status_code=303)

    if campaign_slug and plan != "pro":
        campaign_slug = ""

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

    supporter_success_slug = ""
    if campaign_slug:
        ok, result = claim_supporter_campaign_for_user(current_user["id"], campaign_slug)
        if not ok:
            return RedirectResponse(f"/plans?supporter_error={result}&campaign={campaign_slug}", status_code=303)
        supporter_success_slug = result

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

    redirect_url = "/plans"
    if supporter_success_slug:
        redirect_url = f"/plans?supporter_success={supporter_success_slug}"
    return RedirectResponse(redirect_url, status_code=303)

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
    ok, reason = redeem_friend_promo_for_user(current_user["id"], promo_code)
    if not ok:
        return RedirectResponse(f"/plans?promo_error={reason}", status_code=303)
    return RedirectResponse("/plans?promo_success=1", status_code=303)


@app.post("/supporter-campaigns/apply")
def apply_supporter_campaign(request: Request, campaign_slug: str = Form(...)):
    current_user = get_current_user(request)
    if not current_user:
        return RedirectResponse(f"/login?from=plans&campaign={campaign_slug}", status_code=303)

    return RedirectResponse(f"/plans?campaign={campaign_slug}", status_code=303)


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

    normalized: list[str] = []
    seen_normalized: set[str] = set()
    for tag in result:
        mapped = normalize_discovery_tag(tag)
        if not mapped or mapped in seen_normalized:
            continue
        seen_normalized.add(mapped)
        normalized.append(mapped)
        if len(normalized) >= 5:
            break

    return normalized


def normalize_discovery_tag(tag: str) -> str:
    mapping = {
        "整形": "整形",
        "骨折": "整形",
        "中枢": "中枢",
        "脳神経": "脳神経",
        "呼吸器": "呼吸器",
        "循環器": "循環器",
        "糖尿病": "腎臓機能",
        "腎臓リハ": "腎臓機能",
        "高齢者・フレイル": "高齢者・フレイル",
        "小児リハ": "小児リハ",
        "スポーツ": "整形",
        "歩行・バランス": "歩行",
        "上肢": "整形",
        "ADL・生活機能": "ADL",
        "嚥下障害": "嚥下・言語",
        "疼痛": "リスク管理",
        "転倒": "リスク管理",
        "廃用症候群": "廃用症候群",
        "低栄養": "栄養管理",
        "栄養管理": "栄養管理",
        "拘縮予防": "廃用症候群",
        "離床": "離床",
        "装具": "装具",
        "評価・研究デザイン": "評価・研究デザイン",
        "リスク管理": "リスク管理",
        "多職種連携": "多職種連携",
        "訪問・地域": "訪問・地域",
        "ポリファーマシー": "リスク管理",
        "便秘": "リスク管理",
        "再入院予防": "訪問・地域",
        "血液検査": "リスク管理",
        "画像所見": "画像所見",
        "嚥下・言語・認知": "認知",
    }
    return mapping.get(tag, tag)


def _safe_float(value) -> float | None:
    try:
        if value in (None, ""):
            return None
        return float(value)
    except Exception:
        return None


def _paper_display_title(paper: dict) -> str:
    return (
        (paper.get("custom_title") or "").strip()
        or (paper.get("jp_title") or "").strip()
        or (paper.get("title") or "").strip()
        or str(paper.get("pubmed_id") or "").strip()
    )


def _build_recommendation_sections(user_id: int) -> list[dict]:
    user_saved_map = {str(p["pubmed_id"]): p for p in get_saved_papers(user_id=user_id)}

    def decorate_user_state(paper: dict):
        pid = str(paper.get("pubmed_id") or "")
        saved = user_saved_map.get(pid)
        paper["is_saved"] = saved is not None
        paper["liked"] = get_paper_liked(pid, user_id) if pid else False
        if saved:
            paper["likes"] = int(saved.get("likes") or paper.get("likes") or 0)
            paper["folder_name"] = saved.get("folder_name") or paper.get("folder_name") or ""
        else:
            paper["likes"] = int(paper.get("likes") or 0)

    conn = get_connection()
    cur = conn.cursor()
    cur.execute("""
        SELECT *
        FROM saved_papers
        WHERE summary_jp IS NOT NULL AND summary_jp != ''
        ORDER BY created_at DESC
    """)
    summary_papers = [dict(row) for row in cur.fetchall()]
    conn.close()
    if not summary_papers:
        return []

    sections: list[dict] = []
    seen: set[str] = set()

    def push_section(title: str, subtitle: str, items: list[dict]):
        if not items:
            return
        sections.append({
            "title": title,
            "subtitle": subtitle,
            "items": items,
        })

    personal_items = []
    for item in get_recommended_papers(user_id, limit=10):
        paper = dict(item.get("paper") or {})
        pid = str(paper.get("pubmed_id") or "")
        if not pid or pid in seen:
            continue
        paper["display_title"] = _paper_display_title(paper)
        decorate_user_state(paper)
        personal_items.append({
            "paper": paper,
            "reason": item.get("reason") or "あなた向けのおすすめです",
            "matched_tags": item.get("matched_tags") or [],
            "cta_label": "詳細を見る",
        })
        seen.add(pid)
    push_section("あなたへのおすすめ", "保存や興味タグから、今読む価値が高そうな論文を先に並べています。", personal_items)

    popular_items = []
    for paper in sorted(summary_papers, key=lambda p: (int(p.get("likes") or 0), p.get("created_at") or ""), reverse=True):
        pid = str(paper.get("pubmed_id") or "")
        if not pid or pid in seen:
            continue
        item_paper = dict(paper)
        item_paper["display_title"] = _paper_display_title(item_paper)
        decorate_user_state(item_paper)
        popular_items.append({
            "paper": item_paper,
            "reason": f"保存数・いいねが多い人気論文です",
            "matched_tags": generate_tags(item_paper.get("title", ""), item_paper.get("abstract", "") or "")[:2],
            "cta_label": "人気の理由を見る",
        })
        seen.add(pid)
        if len(popular_items) >= 8:
            break
    push_section("みんなが読んでいる論文", "迷ったときに外しにくい定番どころをまとめています。", popular_items)

    clinical_items = []
    for paper in sorted(
        summary_papers,
        key=lambda p: ((_safe_float(p.get("clinical_score")) or -1), int(p.get("likes") or 0)),
        reverse=True,
    ):
        pid = str(paper.get("pubmed_id") or "")
        score = _safe_float(paper.get("clinical_score"))
        if not pid or pid in seen or score is None:
            continue
        item_paper = dict(paper)
        item_paper["display_title"] = _paper_display_title(item_paper)
        decorate_user_state(item_paper)
        clinical_items.append({
            "paper": item_paper,
            "reason": f"臨床参考度 {score:.1f} の高評価論文です",
            "matched_tags": generate_tags(item_paper.get("title", ""), item_paper.get("abstract", "") or "")[:2],
            "cta_label": "臨床での使いどころを見る",
        })
        seen.add(pid)
        if len(clinical_items) >= 8:
            break
    push_section("臨床で使いやすい論文", "すぐ臨床に結びつけやすい、高評価の論文を優先しています。", clinical_items)

    tag_items = []
    interest_tags = [normalize_discovery_tag(t["tag"]) for t in get_interest_tags(user_id, limit=4)]
    interest_tags = [t for i, t in enumerate(interest_tags) if t and t not in interest_tags[:i]]
    for tag in interest_tags:
        for paper in summary_papers:
            pid = str(paper.get("pubmed_id") or "")
            if not pid or pid in seen:
                continue
            generated = generate_tags(paper.get("title", ""), paper.get("abstract", "") or "")
            if tag not in generated:
                continue
            item_paper = dict(paper)
            item_paper["display_title"] = _paper_display_title(item_paper)
            decorate_user_state(item_paper)
            tag_items.append({
                "paper": item_paper,
                "reason": f"よく見る「{tag}」から広がる論文です",
                "matched_tags": [tag],
                "cta_label": "関連テーマをもっと見る",
            })
            seen.add(pid)
            if len(tag_items) >= 8:
                break
        if len(tag_items) >= 8:
            break
    push_section("興味タグから広がる論文", "今の関心に近いテーマから、次の1本へつながるように並べています。", tag_items)

    return sections
    
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

@app.get("/healthz")
def healthz():
    return {"ok": True}

@app.get("/plans")
def plans(request: Request):

    current_user = get_current_user(request)
    selected_campaign_slug = (request.query_params.get("campaign", "") or "").strip().lower()
    offer_preview_code = (request.query_params.get("offer_preview", "") or "").strip().upper()
    supporter_error = request.query_params.get("supporter_error", "")
    supporter_success = request.query_params.get("supporter_success", "")
    supporter_error_messages = {
        "already_pro": "現在のプランではサポーター価格の申込対象外です。",
        "invalid": "対象のキャンペーンが見つかりませんでした。",
        "already_claimed": "すでにキャンペーン申込済みです。料金プランの上部をご確認ください。",
        "sold_out": "このキャンペーンは上限に達しました。",
    }
    supporter_message = supporter_error_messages.get(supporter_error, "")
    supporter_benefits = get_supporter_benefits()
    supporter_offer = get_supporter_offer_state()

    if not current_user:
        return templates.TemplateResponse(
            "plans.html",
            {
                "request": request,
                "current_user": None,
                "current_plan": "guest",
                "supporter_campaigns": get_supporter_campaigns(),
                "supporter_claim": None,
                "supporter_message": supporter_message,
                "supporter_is_success": False,
                "supporter_benefits": supporter_benefits,
                "supporter_offer": supporter_offer,
                "active_promo_offer": None,
                "selected_campaign": get_campaign_display(selected_campaign_slug),
                "selected_campaign_slug": selected_campaign_slug,
                "offer_preview": get_private_promo_offer_display(offer_preview_code),
                "promo_days_left": None,
                "promo_plan_label": "Pro",
                "promo_message": "",
                "promo_is_success": False,
                "referral_message": "",
                "referral_success": False,
            }
        )

    user = get_user_by_id(current_user["id"])
    user = check_trial_expired(user)

    current_plan = get_user_plan(user)
    supporter_claim = get_user_supporter_campaign_claim(current_user["id"])
    supporter_benefits = get_supporter_benefits(current_user["id"])
    supporter_offer = get_supporter_offer_state(current_user["id"])
    active_promo_offer = get_active_promo_display(user)
    selected_campaign = get_campaign_display(selected_campaign_slug, current_user["id"])
    offer_preview = get_private_promo_offer_display(offer_preview_code)

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
    promo_is_lifetime = int(user.get("promo_is_lifetime") or 0) == 1
    promo_plan_label = "Expert" if (user.get("promo_plan") or "").strip().lower() == "expert" else "Pro"
    promo_ends_at = (user.get("promo_ends_at") or "").strip()
    if promo_is_lifetime:
        promo_days_left = -1
    elif promo_ends_at:
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
    if supporter_success:
        selected_campaign = get_supporter_campaign_by_slug(supporter_success)
        if selected_campaign:
            supporter_message = f"{selected_campaign['title']}が契約時に適用されました。"
    supporter_is_success = bool(supporter_success and supporter_message)

    return templates.TemplateResponse(
        "plans.html",
        {
            "request": request,
            "current_user": current_user,
            "current_plan": current_plan,
            "supporter_campaigns": get_supporter_campaigns(current_user["id"]),
            "supporter_claim": supporter_claim,
            "supporter_message": supporter_message,
            "supporter_is_success": supporter_is_success,
            "supporter_benefits": supporter_benefits,
            "supporter_offer": supporter_offer,
            "active_promo_offer": active_promo_offer,
            "selected_campaign": selected_campaign,
            "selected_campaign_slug": selected_campaign_slug,
            "offer_preview": offer_preview,
            "trial_days_left": trial_days_left,
            "plan_renews_at": plan_renews_at,
            "ref_code": ref_code,
            "promo_days_left": promo_days_left,
            "promo_plan_label": promo_plan_label,
            "promo_is_lifetime": promo_is_lifetime,
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
        return RedirectResponse("/login?from=mypage", status_code=303)

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
            "supporter_campaigns": get_supporter_campaigns(current_user["id"]),
            "supporter_claim": get_user_supporter_campaign_claim(current_user["id"]),
            "supporter_offer": get_supporter_offer_state(current_user["id"]),
            "limits": limits,
            "trial_days_left": trial_days_left,
            "plan_renews_at": plan_renews_at,
            "ref_code": ref_code,
            "trial_extend_days": trial_extend_days,
            "daily_usage": daily_usage,
            "saved_count": saved_count,
            "memo_count": memo_count,
            "recent_papers": recent_papers,
            "is_master_user": is_master_user(user),
        }
    )


@app.get("/master/content")
def master_content_page(request: Request, draft_id: int = Query(0), notice: str = Query(""), error: str = Query("")):
    user, redirect = require_master_user(request)
    if redirect:
        return redirect

    drafts = get_master_article_drafts(limit=30)
    selected_draft = None
    if draft_id:
        selected_draft = get_master_article_draft(draft_id)
    if not selected_draft and drafts:
        selected_draft = drafts[0]
    selected_draft = ensure_master_article_geo_review(selected_draft)

    notice_messages = {
        "generated": "記事下書きを生成しました。",
        "updated": "記事下書きとGEO診断を更新しました。",
        "reviewed": "GEO診断と改善を反映しました。",
        "posted": "WordPressへ下書き投稿しました。",
    }
    error_messages = {
        "paper_not_found": "候補論文が見つかりませんでした。",
        "draft_not_found": "記事下書きが見つかりませんでした。",
        "wp_not_configured": "WordPress接続設定が未設定です。",
    }
    wordpress_settings = get_master_wordpress_settings(user["id"]) or {}
    wordpress_config = get_wordpress_config_for_user(user["id"]) or {}
    autopost_summary = _build_master_autopost_summary(get_master_wordpress_autopost_settings(user["id"]))
    marketing_summary_map = _build_master_article_marketing_summary_map(user["id"])
    marketing_variant_meta_map = {
        int(draft["id"]): get_master_marketing_variant_meta(draft)
        for draft in drafts
    }
    selected_marketing_assets = build_master_article_marketing_assets(selected_draft, user["id"]) if selected_draft else {}

    return templates.TemplateResponse(
        "master_content.html",
        {
            "request": request,
            "current_user": user,
            "current_plan": get_user_plan(user),
            "candidate_papers": get_master_article_candidates(),
            "drafts": drafts,
            "selected_draft": selected_draft,
            "notice_message": notice_messages.get(notice, notice),
            "error_message": error_messages.get(error, _urlparse.unquote(error or "")),
            "wordpress_configured": bool(wordpress_config),
            "wordpress_config_source": wordpress_config.get("source") or "",
            "wordpress_site_url": wordpress_settings.get("site_url") or wordpress_config.get("site_url") or "",
            "wordpress_username": wordpress_settings.get("username") or wordpress_config.get("username") or "",
            "autopost_summary": autopost_summary,
            "marketing_summary_map": marketing_summary_map,
            "marketing_variant_meta_map": marketing_variant_meta_map,
            "selected_marketing_assets": selected_marketing_assets,
        }
    )


@app.post("/master/content/generate")
def master_content_generate(request: Request, pubmed_id: str = Form(...)):
    user, redirect = require_master_user(request)
    if redirect:
        return redirect

    paper = get_master_article_candidate(pubmed_id)
    if not paper:
        return RedirectResponse("/master/content?error=paper_not_found", status_code=303)

    article = generate_master_geo_article(paper)
    optimized = optimize_master_article_to_target(
        {
            "source_title": paper.get("title") or "",
            "source_jp_title": paper.get("jp_title") or "",
            "source_clinical_score": paper.get("clinical_score") or "",
            "source_clinical_reason": paper.get("clinical_reason") or "",
            "article_title": article["title"],
            "article_excerpt": article["excerpt"],
            "article_slug": article["slug"],
            "article_html": article["html"],
        }
    )
    draft_id = create_master_article_draft(
        pubmed_id=str(paper.get("pubmed_id") or ""),
        source_title=paper.get("title") or "",
        source_jp_title=paper.get("jp_title") or "",
        source_summary_jp=paper.get("summary_jp") or "",
        source_abstract=paper.get("abstract") or "",
        source_clinical_score=paper.get("clinical_score") or "",
        source_clinical_reason=paper.get("clinical_reason") or "",
        article_title=optimized["title"],
        article_excerpt=optimized["excerpt"],
        article_slug=optimized["slug"],
        article_html=optimized["html"],
        created_by_user_id=user["id"],
    )
    update_master_article_draft_geo_review(
        draft_id=draft_id,
        geo_score=optimized["score"],
        geo_feedback=optimized["feedback"],
        article_title=optimized["title"],
        article_excerpt=optimized["excerpt"],
        article_slug=optimized["slug"],
        article_html=optimized["html"],
    )
    return RedirectResponse(f"/master/content?draft_id={draft_id}&notice=generated", status_code=303)


@app.post("/master/content/{draft_id}/update")
def master_content_update(
    request: Request,
    draft_id: int,
    article_title: str = Form(...),
    article_excerpt: str = Form(""),
    article_slug: str = Form(""),
    article_html: str = Form(""),
):
    _user, redirect = require_master_user(request)
    if redirect:
        return redirect

    draft = get_master_article_draft(draft_id)
    if not draft:
        return RedirectResponse("/master/content?error=draft_not_found", status_code=303)

    update_master_article_draft_content(
        draft_id=draft_id,
        article_title=(article_title or "").strip() or draft.get("article_title") or "記事下書き",
        article_excerpt=(article_excerpt or "").strip(),
        article_slug=_slugify_article_title((article_slug or "").strip() or article_title),
        article_html=(article_html or "").strip(),
    )
    updated_draft = get_master_article_draft(draft_id)
    ensure_master_article_geo_review(updated_draft, force=True)
    return RedirectResponse(f"/master/content?draft_id={draft_id}&notice=updated", status_code=303)


@app.post("/master/content/{draft_id}/publish-wordpress")
def master_content_publish_wordpress(request: Request, draft_id: int):
    user, redirect = require_master_user(request)
    if redirect:
        return redirect

    draft = get_master_article_draft(draft_id)
    if not draft:
        return RedirectResponse("/master/content?error=draft_not_found", status_code=303)
    if not is_wordpress_configured(user["id"]):
        return RedirectResponse(f"/master/content?draft_id={draft_id}&error=wp_not_configured", status_code=303)

    optimized = optimize_master_article_to_target(draft)
    update_master_article_draft_geo_review(
        draft_id=draft_id,
        geo_score=optimized["score"],
        geo_feedback=optimized["feedback"],
        article_title=optimized["title"],
        article_excerpt=optimized["excerpt"],
        article_slug=optimized["slug"],
        article_html=optimized["html"],
    )
    draft = get_master_article_draft(draft_id)

    ok, result = publish_master_article_to_wordpress(draft, user["id"])
    if not ok:
        return RedirectResponse(
            f"/master/content?draft_id={draft_id}&error={_urlparse.quote(str(result))}",
            status_code=303,
        )

    mark_master_article_wordpress_posted(
        draft_id=draft_id,
        wordpress_post_id=str(result.get("id") or ""),
        wordpress_status=str(result.get("status") or "draft"),
    )
    return RedirectResponse(f"/master/content?draft_id={draft_id}&notice=posted", status_code=303)


@app.get("/master/settings")
def master_settings_page(request: Request, notice: str = Query(""), error: str = Query("")):
    user, redirect = require_master_user(request)
    if redirect:
        return redirect

    saved_settings = get_master_wordpress_settings(user["id"]) or {}
    effective_config = get_wordpress_config_for_user(user["id"]) or {}
    autopost_summary = _build_master_autopost_summary(get_master_wordpress_autopost_settings(user["id"]))
    autopost_logs = get_master_wordpress_autopost_logs(user["id"], limit=8)
    return templates.TemplateResponse(
        "master_settings.html",
        {
            "request": request,
            "current_user": user,
            "current_plan": get_user_plan(user),
            "notice_message": _urlparse.unquote(notice or ""),
            "error_message": _urlparse.unquote(error or ""),
            "saved_site_url": saved_settings.get("site_url") or "",
            "saved_username": saved_settings.get("username") or "",
            "saved_app_base_url": saved_settings.get("app_base_url") or "",
            "has_saved_password": bool(saved_settings.get("app_password")),
            "wordpress_configured": bool(effective_config),
            "wordpress_config_source": effective_config.get("source") or "",
            "effective_site_url": effective_config.get("site_url") or "",
            "effective_username": effective_config.get("username") or "",
            "effective_app_base_url": get_app_base_url_for_user(user["id"], request),
            "autopost_summary": autopost_summary,
            "autopost_logs": autopost_logs,
        }
    )


@app.post("/master/settings")
def master_settings_save(
    request: Request,
    site_url: str = Form(""),
    username: str = Form(""),
    app_password: str = Form(""),
    app_base_url: str = Form(""),
):
    user, redirect = require_master_user(request)
    if redirect:
        return redirect

    normalized_site_url = _normalize_wordpress_site_url(site_url)
    normalized_username = (username or "").strip()
    normalized_password = _normalize_wordpress_app_password(app_password)
    normalized_app_base_url = _normalize_wordpress_site_url(app_base_url)
    existing = get_master_wordpress_settings(user["id"]) or {}

    if not normalized_site_url:
        return RedirectResponse(
            f"/master/settings?error={_urlparse.quote('WordPressサイトURLを入力してください。')}",
            status_code=303,
        )
    if not normalized_username:
        return RedirectResponse(
            f"/master/settings?error={_urlparse.quote('WordPressユーザー名を入力してください。')}",
            status_code=303,
        )
    if not normalized_password and not existing.get("app_password"):
        return RedirectResponse(
            f"/master/settings?error={_urlparse.quote('初回保存ではアプリパスワードの入力が必要です。')}",
            status_code=303,
        )

    upsert_master_wordpress_settings(
        user_id=user["id"],
        site_url=normalized_site_url,
        username=normalized_username,
        app_password=normalized_password,
        app_base_url=normalized_app_base_url,
    )
    return RedirectResponse(
        f"/master/settings?notice={_urlparse.quote('WordPress接続設定を保存しました。')}",
        status_code=303,
    )


@app.post("/master/settings/test")
def master_settings_test(request: Request):
    user, redirect = require_master_user(request)
    if redirect:
        return redirect

    config = get_wordpress_config_for_user(user["id"])
    if not config:
        return RedirectResponse(
            f"/master/settings?error={_urlparse.quote('先にWordPress接続設定を保存してください。')}",
            status_code=303,
        )

    ok, message = test_wordpress_connection(config)
    key = "notice" if ok else "error"
    return RedirectResponse(
        f"/master/settings?{key}={_urlparse.quote(str(message))}",
        status_code=303,
    )


@app.post("/master/settings/autopost")
def master_settings_autopost(
    request: Request,
    is_enabled: str = Form("0"),
    daily_time: str = Form("09:00"),
):
    user, redirect = require_master_user(request)
    if redirect:
        return redirect

    normalized_time = _normalize_master_autopost_time(daily_time)
    enabled = 1 if str(is_enabled).strip() in ("1", "true", "on", "yes") else 0
    if enabled and not is_wordpress_configured(user["id"]):
        return RedirectResponse(
            f"/master/settings?error={_urlparse.quote('先にWordPress接続設定を保存してから、自動投稿を有効にしてください。')}",
            status_code=303,
        )
    upsert_master_wordpress_autopost_settings(
        user_id=user["id"],
        is_enabled=enabled,
        daily_time=normalized_time,
    )
    notice = "自動投稿を有効化しました。" if enabled else "自動投稿を停止しました。"
    return RedirectResponse(
        f"/master/settings?notice={_urlparse.quote(notice)}",
        status_code=303,
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
    recommend_sections = []
    if current_user:
        try:
            recommended = get_recommended_papers(current_user["id"], limit=10)
        except Exception:
            recommended = []
        try:
            recommend_sections = _build_recommendation_sections(current_user["id"])
        except Exception:
            recommend_sections = []
    return templates.TemplateResponse("learn.html", {
        "request": request,
        "current_user": current_user,
        "posts": posts,
        "active_tag": tag,
        "tab": tab,
        "recommended": recommended,
        "recommend_sections": recommend_sections,
        "board_tags": COMMON_DISCOVERY_TAGS,
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
    user = get_user_by_id(current_user["id"])
    plan = get_user_plan(user)
    if plan not in ("pro", "expert"):
        return RedirectResponse("/plans?error=export_requires_pro", status_code=303)
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
                "discovery_tags": COMMON_DISCOVERY_TAGS,
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
                "discovery_tags": COMMON_DISCOVERY_TAGS,
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
                "discovery_tags": COMMON_DISCOVERY_TAGS,
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
                    "discovery_tags": COMMON_DISCOVERY_TAGS,
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
            "discovery_tags": COMMON_DISCOVERY_TAGS,
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


@app.get("/board/attachable-papers")
def board_attachable_papers(request: Request, q: str = Query("")):
    current_user = get_current_user(request)
    if not current_user:
        return JSONResponse({"papers": []})

    query = (q or "").strip().lower()
    saved_papers = get_saved_papers(current_user["id"])
    saved_papers = sorted(saved_papers, key=lambda x: x.get("created_at", ""), reverse=True)

    items = []
    for paper in saved_papers:
        display_title = (
            (paper.get("custom_title") or "").strip()
            or (paper.get("jp_title") or "").strip()
            or (paper.get("title") or "").strip()
            or str(paper.get("pubmed_id") or "").strip()
        )
        haystack = " ".join([
            str(paper.get("pubmed_id") or ""),
            str(paper.get("title") or ""),
            str(paper.get("jp_title") or ""),
            str(paper.get("custom_title") or ""),
            str(paper.get("journal") or ""),
        ]).lower()
        if query and query not in haystack:
            continue
        items.append({
            "pubmed_id": str(paper.get("pubmed_id") or ""),
            "title": str(paper.get("title") or ""),
            "jp_title": str(paper.get("jp_title") or ""),
            "display_title": display_title,
            "journal": str(paper.get("journal") or ""),
            "pubdate": str(paper.get("pubdate") or ""),
        })
        if len(items) >= 12:
            break

    return JSONResponse({"papers": items})


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
        paper["liked"] = get_paper_liked(paper["pubmed_id"], current_user_id) if current_user_id else False

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

_MASTER_ARTICLE_TRACKING_GIF = (
    b"GIF89a\x01\x00\x01\x00\x80\x00\x00\x00\x00\x00\xff\xff\xff!"
    b"\xf9\x04\x01\x00\x00\x00\x00,\x00\x00\x00\x00\x01\x00\x01\x00"
    b"\x00\x02\x02D\x01\x00;"
)


@app.get("/track/master-article/{draft_id}.gif")
def track_master_article_impression(request: Request, draft_id: int, variant: str = Query("A")):
    draft = get_master_article_draft(draft_id)
    if draft:
        record_master_article_marketing_event(
            draft_id=draft_id,
            event_type="impression",
            variant=(variant or draft.get("marketing_variant") or "A").strip().upper(),
            source="wordpress_article",
            user_id=None,
            ip_hash=_hash_client_value(request.client.host if request.client else ""),
            user_agent=request.headers.get("user-agent", "")[:300],
        )
    return Response(content=_MASTER_ARTICLE_TRACKING_GIF, media_type="image/gif")


@app.get("/go/master-article/{draft_id}")
def go_master_article(request: Request, draft_id: int, variant: str = Query("A")):
    draft = get_master_article_draft(draft_id)
    if not draft:
        return RedirectResponse(url="/register", status_code=303)

    resolved_variant = (variant or draft.get("marketing_variant") or "A").strip().upper()
    record_master_article_marketing_event(
        draft_id=draft_id,
        event_type="click",
        variant=resolved_variant,
        source="wordpress_article",
        user_id=None,
        ip_hash=_hash_client_value(request.client.host if request.client else ""),
        user_agent=request.headers.get("user-agent", "")[:300],
    )
    if get_current_user(request):
        clear_article_marketing_attribution(request)
        return RedirectResponse(url="/", status_code=303)
    request.session["article_marketing_attribution"] = {
        "draft_id": draft_id,
        "variant": resolved_variant,
        "source": "wordpress_article",
        "clicked_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }
    return RedirectResponse(url="/register?from=master_article", status_code=303)


@app.get("/register")
def register_page(
    request: Request,
    ref_code: str = Query(""),
    campaign: str = Query(""),
    promo_code: str = Query(""),
    locked_email: str = Query(""),
    from_page: str = Query(default="", alias="from"),
):
    current_user = get_current_user(request)

    if current_user and not (from_page == "special_offer" and promo_code):
        return RedirectResponse(url="/", status_code=303)

    article_attribution = get_article_marketing_attribution(request)

    return templates.TemplateResponse(
        "register.html",
        {
            "request": request,
            "error": "",
            "supporter_campaigns": get_supporter_campaigns(),
            "supporter_offer": get_supporter_offer_state(),
            "selected_campaign": get_campaign_display(campaign),
            "campaign_slug": (campaign or "").strip().lower(),
            "selected_promo_offer": get_private_promo_offer_display(promo_code),
            "promo_code": (promo_code or "").strip().upper(),
            "locked_email": (locked_email or "").strip().lower(),
            "ref_code": (ref_code or "").strip().upper(),
            "from_page": from_page,
            "article_marketing_attribution": article_attribution,
        }
    )


@app.post("/register")
def register(
    request: Request,
    email: str = Form(...),
    password: str = Form(...),
    ref_code: str = Form(""),
    campaign_slug: str = Form(""),
    promo_code: str = Form(""),
    locked_email: str = Form(""),
    from_page: str = Form(""),
):
    current_user = get_current_user(request)
    article_attribution = get_article_marketing_attribution(request)

    if current_user and not (from_page == "special_offer" and promo_code):
        return RedirectResponse(url="/", status_code=303)

    email = email.strip().lower()
    locked_email = (locked_email or "").strip().lower()
    ref_code = (ref_code or "").strip().upper()
    campaign_slug = (campaign_slug or "").strip().lower()
    promo_code = (promo_code or "").strip().upper()

    if not email or not password:
        return templates.TemplateResponse(
            "register.html",
            {
                "request": request,
                "error": "メールアドレスとパスワードを入力してください。",
                "supporter_campaigns": get_supporter_campaigns(),
                "supporter_offer": get_supporter_offer_state(),
                "selected_campaign": get_campaign_display(campaign_slug),
                "campaign_slug": campaign_slug,
                "selected_promo_offer": get_private_promo_offer_display(promo_code),
                "promo_code": promo_code,
                "locked_email": locked_email,
                "ref_code": ref_code,
                "from_page": from_page,
                "article_marketing_attribution": article_attribution,
            }
        )

    if locked_email and email != locked_email:
        return templates.TemplateResponse(
            "register.html",
            {
                "request": request,
                "error": "この招待枠は指定されたメールアドレスでのみ登録できます。",
                "supporter_campaigns": get_supporter_campaigns(),
                "supporter_offer": get_supporter_offer_state(),
                "selected_campaign": get_campaign_display(campaign_slug),
                "campaign_slug": campaign_slug,
                "selected_promo_offer": get_private_promo_offer_display(promo_code),
                "promo_code": promo_code,
                "locked_email": locked_email,
                "ref_code": ref_code,
                "from_page": from_page,
                "article_marketing_attribution": article_attribution,
            }
        )

    if len(password) < 6:
        return templates.TemplateResponse(
            "register.html",
            {
                "request": request,
                "error": "パスワードは6文字以上で入力してください。",
                "supporter_campaigns": get_supporter_campaigns(),
                "supporter_offer": get_supporter_offer_state(),
                "selected_campaign": get_campaign_display(campaign_slug),
                "campaign_slug": campaign_slug,
                "selected_promo_offer": get_private_promo_offer_display(promo_code),
                "promo_code": promo_code,
                "locked_email": locked_email,
                "ref_code": ref_code,
                "from_page": from_page,
                "article_marketing_attribution": article_attribution,
            }
        )

    if promo_code:
        ok, reason = reserve_friend_promo_for_email(promo_code, email)
        if not ok and reason != "ok":
            error_messages = {
                "invalid_email": "有効なメールアドレスを入力してください。",
                "not_found": "この特別枠が見つかりません。",
                "inactive": "この特別枠は現在利用できません。",
                "limit_reached": "この特別枠はすでに利用済みです。",
                "email_mismatch": "この特別枠は別のメールアドレスで固定済みです。案内されたメールアドレスで登録してください。",
            }
            return templates.TemplateResponse(
                "register.html",
                {
                    "request": request,
                    "error": error_messages.get(reason, "この特別枠を利用できませんでした。"),
                    "supporter_campaigns": get_supporter_campaigns(),
                    "supporter_offer": get_supporter_offer_state(),
                    "selected_campaign": get_campaign_display(campaign_slug),
                    "campaign_slug": campaign_slug,
                    "selected_promo_offer": get_private_promo_offer_display(promo_code),
                    "promo_code": promo_code,
                    "locked_email": locked_email,
                    "ref_code": ref_code,
                    "from_page": from_page,
                }
            )

    user = create_user(email, password)

    if not user:
        return templates.TemplateResponse(
            "register.html",
            {
                "request": request,
                "error": "このメールアドレスは既に登録されています。",
                "supporter_campaigns": get_supporter_campaigns(),
                "supporter_offer": get_supporter_offer_state(),
                "selected_campaign": get_campaign_display(campaign_slug),
                "campaign_slug": campaign_slug,
                "selected_promo_offer": get_private_promo_offer_display(promo_code),
                "promo_code": promo_code,
                "locked_email": locked_email,
                "ref_code": ref_code,
                "from_page": from_page,
            }
        )

    if ref_code:
        referrer = get_user_by_ref_code(ref_code)
        if referrer:
            ok, reason = apply_referral_bonus(
                referrer_id=referrer["id"],
                referred_user_id=user["id"]
            )
            if ok:
                today = datetime.now()
                trial_end = today + timedelta(days=7)
                update_user_plan(
                    user_id=user["id"],
                    plan="pro",
                    trial_ends_at=trial_end.strftime("%Y-%m-%d"),
                    plan_started_at=today.strftime("%Y-%m-%d"),
                    plan_renews_at=trial_end.strftime("%Y-%m-%d"),
                    is_yearly=0,
                    trial_used=1,
                )
                user = get_user_by_id(user["id"])
            elif reason not in ("not_found", "already_used", "self_referral"):
                return templates.TemplateResponse(
                    "register.html",
                    {
                        "request": request,
                        "error": "紹介コードの適用でエラーが発生しました。",
                        "supporter_campaigns": get_supporter_campaigns(),
                        "supporter_offer": get_supporter_offer_state(),
                        "selected_campaign": get_campaign_display(campaign_slug),
                        "campaign_slug": campaign_slug,
                        "selected_promo_offer": get_private_promo_offer_display(promo_code),
                        "promo_code": promo_code,
                        "locked_email": locked_email,
                        "ref_code": ref_code,
                        "from_page": from_page,
                        "article_marketing_attribution": article_attribution,
                    }
                )

    request.session["user_id"] = user["id"]

    if article_attribution.get("draft_id"):
        try:
            draft_id = int(article_attribution["draft_id"])
        except Exception:
            draft_id = 0
        variant = str(article_attribution.get("variant") or "").strip().upper()
        if draft_id > 0:
            set_user_article_attribution(
                user_id=user["id"],
                channel=str(article_attribution.get("source") or "wordpress_article"),
                draft_id=draft_id,
                variant=variant,
            )
            record_master_article_marketing_event(
                draft_id=draft_id,
                event_type="registration",
                variant=variant,
                source=str(article_attribution.get("source") or "wordpress_article"),
                user_id=user["id"],
                ip_hash=_hash_client_value(request.client.host if request.client else ""),
                user_agent=request.headers.get("user-agent", "")[:300],
            )
            clear_article_marketing_attribution(request)

    if promo_code:
        ok, reason = redeem_friend_promo_for_user(user["id"], promo_code)
        if ok:
            return RedirectResponse(url="/plans?promo_success=1", status_code=303)
        return RedirectResponse(url=f"/plans?promo_error={reason}", status_code=303)

    if campaign_slug:
        return RedirectResponse(url=f"/plans?campaign={campaign_slug}", status_code=303)

    return RedirectResponse(url="/", status_code=303)


@app.get("/login")
def login_page(
    request: Request,
    from_page: str = Query(default="", alias="from"),
    campaign: str = Query(""),
    promo_code: str = Query(""),
    locked_email: str = Query(""),
):
    current_user = get_current_user(request)

    if current_user and not (from_page == "special_offer" and promo_code):
        return RedirectResponse(url="/", status_code=303)

    article_attribution = get_article_marketing_attribution(request)

    return templates.TemplateResponse(
        "login.html",
        {
            "request": request,
            "error": "",
            "from_page": from_page,
            "supporter_offer": get_supporter_offer_state(),
            "selected_campaign": get_campaign_display(campaign),
            "campaign_slug": (campaign or "").strip().lower(),
            "selected_promo_offer": get_private_promo_offer_display(promo_code),
            "promo_code": (promo_code or "").strip().upper(),
            "locked_email": (locked_email or "").strip().lower(),
            "article_marketing_attribution": article_attribution,
        }
    )


@app.post("/login")
def login(
    request: Request,
    email: str = Form(...),
    password: str = Form(...),
    campaign_slug: str = Form(""),
    promo_code: str = Form(""),
    locked_email: str = Form(""),
    from_page: str = Form(""),
):
    current_user = get_current_user(request)
    article_attribution = get_article_marketing_attribution(request)

    if current_user and not (from_page == "special_offer" and promo_code):
        return RedirectResponse(url="/", status_code=303)

    email = email.strip().lower()
    locked_email = (locked_email or "").strip().lower()

    if locked_email and email != locked_email:
        return templates.TemplateResponse(
            "login.html",
            {
                "request": request,
                "error": "この招待枠は指定されたメールアドレスでのみ利用できます。",
                "from_page": from_page,
                "supporter_offer": get_supporter_offer_state(),
                "selected_campaign": get_campaign_display(campaign_slug),
                "campaign_slug": (campaign_slug or "").strip().lower(),
                "selected_promo_offer": get_private_promo_offer_display(promo_code),
                "promo_code": (promo_code or "").strip().upper(),
                "locked_email": locked_email,
                "article_marketing_attribution": article_attribution,
            }
        )

    user = verify_user(email, password)

    if not user:
        return templates.TemplateResponse(
            "login.html",
            {
                "request": request,
                "error": "メールアドレスまたはパスワードが違います。",
                "from_page": from_page,
                "supporter_offer": get_supporter_offer_state(),
                "selected_campaign": get_campaign_display(campaign_slug),
                "campaign_slug": (campaign_slug or "").strip().lower(),
                "selected_promo_offer": get_private_promo_offer_display(promo_code),
                "promo_code": (promo_code or "").strip().upper(),
                "locked_email": locked_email,
            }
        )

    promo_code = (promo_code or "").strip().upper()
    if promo_code:
        ok, reason = reserve_friend_promo_for_email(promo_code, email)
        if not ok and reason != "ok":
            error_messages = {
                "invalid_email": "有効なメールアドレスを入力してください。",
                "not_found": "この特別枠が見つかりません。",
                "inactive": "この特別枠は現在利用できません。",
                "limit_reached": "この特別枠はすでに利用済みです。",
                "email_mismatch": "この特別枠は別のメールアドレスで固定済みです。案内されたメールアドレスでログインしてください。",
            }
            return templates.TemplateResponse(
                "login.html",
                {
                    "request": request,
                    "error": error_messages.get(reason, "この特別枠を利用できませんでした。"),
                    "from_page": from_page,
                    "supporter_offer": get_supporter_offer_state(),
                    "selected_campaign": get_campaign_display(campaign_slug),
                    "campaign_slug": (campaign_slug or "").strip().lower(),
                    "selected_promo_offer": get_private_promo_offer_display(promo_code),
                    "promo_code": promo_code,
                    "locked_email": locked_email,
                    "article_marketing_attribution": article_attribution,
                }
            )

    request.session["user_id"] = user["id"]
    if article_attribution:
        clear_article_marketing_attribution(request)

    if promo_code:
        ok, reason = redeem_friend_promo_for_user(user["id"], promo_code)
        if ok:
            return RedirectResponse(url="/plans?promo_success=1", status_code=303)
        return RedirectResponse(url=f"/plans?promo_error={reason}", status_code=303)

    campaign_slug = (campaign_slug or "").strip().lower()
    if campaign_slug:
        return RedirectResponse(url=f"/plans?campaign={campaign_slug}", status_code=303)

    return RedirectResponse(url="/", status_code=303)


@app.get("/logout")
def logout(request: Request):
    request.session.clear()
    return RedirectResponse(url="/", status_code=303)


@app.get("/special-offer/{promo_code}")
def special_offer_entry(request: Request, promo_code: str):
    code = (promo_code or "").strip().upper()
    offer = get_private_promo_offer_display(code)
    if not offer:
        return RedirectResponse("/plans?promo_error=not_found", status_code=303)

    code_row = get_friend_promo_code(code)
    if not code_row or not int(code_row.get("is_active") or 0):
        return RedirectResponse("/plans?promo_error=inactive", status_code=303)

    return RedirectResponse(f"/register?from=special_offer&promo_code={code}", status_code=303)


@app.post("/special-offer/{promo_code}/lock-email")
def special_offer_lock_email(request: Request, promo_code: str, email: str = Form(...)):
    code = (promo_code or "").strip().upper()
    offer = get_private_promo_offer_display(code)
    code_row = get_friend_promo_code(code)
    email = (email or "").strip().lower()

    if not offer or not code_row or not int(code_row.get("is_active") or 0):
        return RedirectResponse("/plans?promo_error=not_found", status_code=303)

    if not is_valid_email_address(email):
        return templates.TemplateResponse(
            "special_offer_gate.html",
            {
                "request": request,
                "selected_promo_offer": offer,
                "promo_code": code,
                "locked_email_exists": bool((code_row.get("target_email") or "").strip()),
                "error": "有効なメールアドレスを入力してください。",
            }
        )

    if int(code_row.get("used_count") or 0) >= int(code_row.get("max_uses") or 1):
        return templates.TemplateResponse(
            "special_offer_gate.html",
            {
                "request": request,
                "selected_promo_offer": offer,
                "promo_code": code,
                "locked_email_exists": True,
                "error": "この特別枠はすでに利用済みです。",
            }
        )

    target_email = (code_row.get("target_email") or "").strip().lower()
    if target_email and target_email != email:
        return templates.TemplateResponse(
            "special_offer_gate.html",
            {
                "request": request,
                "selected_promo_offer": offer,
                "promo_code": code,
                "locked_email_exists": True,
                "error": "この特別枠は別のメールアドレスで固定済みです。案内されたメールアドレスで続けてください。",
            }
        )

    if not target_email:
        set_friend_promo_target_email(code_row["id"], email)

    return RedirectResponse(
        url=f"/register?from=special_offer&promo_code={code}&locked_email={email}",
        status_code=303,
    )

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
        return templates.TemplateResponse("memo.html", {
            "request": request,
            "current_user": None,
            "current_plan": "guest",
            "tab": tab,
            "quick_memos": [],
            "paper_memos": [],
            "total_count": 0,
            "memo_limit": 0,
        })

    user_id = current_user["id"]
    plan = get_user_plan(current_user)
    limits = get_plan_limits(plan)
    memo_limit = limits["memo_limit"]

    # タイトルも本文も空のクイックメモは一覧に表示しない（pagehideで削除されるはずだが念のため除外）
    quick_memos = [m for m in get_user_memos(user_id) if (m.get("title") or "").strip() or (m.get("body") or "").strip()]
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

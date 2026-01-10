import os
import base64
import json
import tempfile
import subprocess
import uuid
import datetime as dt
import requests
from flask import Flask, request, jsonify
import boto3
from botocore.config import Config
import math
import random
from threading import Thread
import logging
import gspread
from google.oauth2.service_account import Credentials

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger(__name__)

app = Flask(__name__)

# Config R2 (S3 compatibile)
R2_ACCESS_KEY_ID = os.environ.get("R2_ACCESS_KEY_ID")
R2_SECRET_ACCESS_KEY = os.environ.get("R2_SECRET_ACCESS_KEY")
R2_BUCKET_NAME = os.environ.get("R2_BUCKET_NAME")
R2_PUBLIC_BASE_URL = os.environ.get("R2_PUBLIC_BASE_URL")
R2_REGION = os.environ.get("R2_REGION", "auto")
R2_ACCOUNT_ID = os.environ.get("R2_ACCOUNT_ID")

# Pexels / Pixabay API
PEXELS_API_KEY = os.environ.get("PEXELS_API_KEY")
PIXABAY_API_KEY = os.environ.get("PIXABAY_API_KEY")
GOOGLE_CREDENTIALS_JSON = os.environ.get("GOOGLE_CREDENTIALS_JSON", "")

# ✅ ID FISSO AI TOOL MASTER ITALIA
SPREADSHEET_ID = "1hNABNms2MdhcjY4XJK86U4e142MaCZMDIrSx5JVftqo"

jobs = {}
MAX_JOBS = 50

def get_gspread_client():
    """Client Google Sheets per update Video_URL"""
    try:
        if not GOOGLE_CREDENTIALS_JSON:
            return None
        creds_dict = json.loads(GOOGLE_CREDENTIALS_JSON)
        scopes = ['https://www.googleapis.com/auth/spreadsheets']
        credentials = Credentials.from_service_account_info(creds_dict, scopes=scopes)
        return gspread.authorize(credentials)
    except Exception as e:
        logger.error(f"Google Sheets client error: {e}")
        return None

def get_s3_client():
    """Client S3 configurato per Cloudflare R2"""
    if R2_ACCOUNT_ID:
        endpoint_url = f"https://{R2_ACCOUNT_ID}.r2.cloudflarestorage.com"
    else:
        endpoint_url = None
    if endpoint_url is None:
        raise RuntimeError("Endpoint R2 non configurato: imposta R2_ACCOUNT_ID in Railway")
    
    session = boto3.session.Session()
    s3_client = session.client(
        service_name="s3",
        region_name=R2_REGION,
        endpoint_url=endpoint_url,
        aws_access_key_id=R2_ACCESS_KEY_ID,
        aws_secret_access_key=R2_SECRET_ACCESS_KEY,
        config=Config(s3={"addressing_style": "virtual"}),
    )
    return s3_client

def cleanup_old_videos(s3_client, current_key):
    """Cancella tutti i video MP4 in R2 TRANNE quello appena caricato"""
    try:
        paginator = s3_client.get_paginator("list_objects_v2")
        pages = paginator.paginate(Bucket=R2_BUCKET_NAME, Prefix="videos/")
        deleted_count = 0
        for page in pages:
            if "Contents" not in page:
                continue
            for obj in page["Contents"]:
                key = obj["Key"]
                if key.endswith(".mp4") and key != current_key:
                    s3_client.delete_object(Bucket=R2_BUCKET_NAME, Key=key)
                    deleted_count += 1
                    print(f"🗑️ Cancellato vecchio video: {key}", flush=True)
        if deleted_count > 0:
            print(f"✅ Rotazione completata: {deleted_count} video vecchi rimossi", flush=True)
        else:
            print("✅ Nessun video vecchio da rimuovere", flush=True)
    except Exception as e:
        print(f"⚠️ Errore rotazione R2 (video vecchi restano): {str(e)}", flush=True)

# -------------------------------------------------
# Mapping SCENA → QUERY visiva (canale AI TOOL MASTER ITALIA)
# -------------------------------------------------
def pick_visual_query(context: str, keywords_text: str = "") -> str:
    """Query ottimizzate per B-roll AI/TECH: laptop, coding, AI, workflow, productivity, developer life."""
    ctx = (context or "").lower()
    kw = (keywords_text or "").lower()
    
    base = "ai tool technology laptop coding workflow productivity developer"
    
    # AI / Intelligenza Artificiale
    if any(w in ctx for w in ["ai", "intelligenza", "artificial", "machine learning", "ml"]):
        return "ai artificial intelligence technology futuristic, machine learning data visualization, neural network ai tool"
    
    # Coding / Programmazione / Sviluppo
    if any(w in ctx for w in ["cod", "programm", "svilupp", "debug", "code", "python", "javascript"]):
        return "programmer coding laptop computer, software developer typing code, programming workflow productivity"
    
    # Workflow / Automazione / Processi
    if any(w in ctx for w in ["workflow", "automaz", "process", "flusso", "n8n", "zapier"]):
        return "automation workflow productivity tools, business process optimization, workflow management software"
    
    # Productivity / Produttività / Tool
    if any(w in ctx for w in ["produttività", "tool", "strument", "efficienza", "task"]):
        return "productivity tools laptop work, time management productivity app, efficient workflow digital tools"
    
    # Data / Analytics / Grafici
    if any(w in ctx for w in ["data", "analisi", "grafic", "chart", "dashboard"]):
        return "data analytics dashboard visualization, business intelligence charts graphs, data science analysis tools"
    
    # API / Integrazione / Cloud
    if any(w in ctx for w in ["api", "integrazion", "cloud", "server", "database"]):
        return "api integration developer tools, cloud computing server infrastructure, database management software"
    
    # Se abbiamo keywords specifiche dallo Sheet
    if kw and kw != "none":
        return f"{kw}, ai tool technology, laptop coding workflow, developer productivity, software engineering"
    
    # Fallback AI/TECH generico
    return base

def is_ai_tool_video_metadata(video_data, source):
    """🔧 FIX A: Filtro PERMISSIVO - NO banned (neutral OK) + keywords extra"""
    # Keywords AI/TECH ESPANSIVE (più match)
    tech_keywords = ["ai", "coding", "code", "data", "laptop", "computer", "developer", "programmer", 
                     "technology", "tech", "workflow", "screen", "keyboard", "office", "work", "business", 
                     "software", "digital", "analytics", "dashboard"]
    # BANNED non-tech
    banned = ["dog", "cat", "animal", "food", "cooking", "fitness", "gym", "sports", "nature", "beach", "mountain", "wedding"]
    
    if source == "pexels":
        text = (video_data.get("description", "") + " " + " ".join(video_data.get("tags", []))).lower()
    else:
        text = " ".join(video_data.get("tags", [])).lower()
    
    tech_count = sum(1 for kw in tech_keywords if kw in text)
    has_banned = any(kw in text for kw in banned)
    
    if has_banned:
        status = "❌ BANNED"
    elif tech_count >= 1:
        status = f"✅ TECH({tech_count})"
    else:
        status = f"⚠️ NEUTRAL(tech:{tech_count})"
    
    print(f"🔍 [{source}] '{text[:60]}...' → {status}", flush=True)
    
    # 🔧 PERMISSIVO: accetta TUTTO tranne banned (neutral OK!)
    return not has_banned

def download_file(url: str) -> str:
    """Download video con chunk grandi per velocità"""
    tmp_clip = tempfile.NamedTemporaryFile(delete=False, suffix=".mp4")
    clip_resp = requests.get(url, stream=True, timeout=30)
    clip_resp.raise_for_status()
    for chunk in clip_resp.iter_content(chunk_size=1024 * 1024):
        if chunk:
            tmp_clip.write(chunk)
    tmp_clip.close()
    return tmp_clip.name

def fetch_clip_for_scene(scene_number: int, query: str, avg_scene_duration: float):
    """🎯 Canale AI TOOL: B-roll tech. Fallback Pixabay se Pexels 0."""
    target_duration = min(4.0, avg_scene_duration)
    
    def try_pexels():
        if not PEXELS_API_KEY:
            return None
        headers = {"Authorization": PEXELS_API_KEY}
        params = {
            "query": f"{query} laptop coding ai tool technology workflow",
            "orientation": "landscape",
            "per_page": 25,
            "page": random.randint(1, 3),
        }
        resp = requests.get("https://api.pexels.com/videos/search", headers=headers, params=params, timeout=20)
        if resp.status_code != 200:
            return None
        videos = resp.json().get("videos", [])
        tech_videos = [v for v in videos if is_ai_tool_video_metadata(v, "pexels")]
        print(f"🎯 Pexels: {len(videos)} totali → {len(tech_videos)} OK (no banned)", flush=True)
        if tech_videos:
            video = random.choice(tech_videos)
            for vf in video.get("video_files", []):
                if vf.get("width", 0) >= 1280:
                    return download_file(vf["link"])
        return None
    
    def try_pixabay():
        if not PIXABAY_API_KEY:
            return None
        params = {
            "key": PIXABAY_API_KEY,
            "q": f"{query} laptop coding ai tool technology workflow",
            "per_page": 25,
            "safesearch": "true",
            "min_width": 1280,
        }
        resp = requests.get("https://pixabay.com/api/videos/", params=params, timeout=20)
        if resp.status_code != 200:
            return None
        hits = resp.json().get("hits", [])
        for hit in hits:
            if is_ai_tool_video_metadata(hit, "pixabay"):
                videos = hit.get("videos", {})
                for quality in ["large", "medium", "small"]:
                    if quality in videos and "url" in videos[quality]:
                        return download_file(videos[quality]["url"])
        return None
    
    # Priorità: Pexels → Pixabay
    for source_name, func in [("Pexels", try_pexels), ("Pixabay", try_pixabay)]:
        try:
            path = func()
            if path:
                print(f"🎥 Scena {scene_number}: '{query[:40]}...' → {source_name} ✓", flush=True)
                return path, target_duration
        except Exception as e:
            print(f"⚠️ {source_name}: {e}", flush=True)
    
    print(f"⚠️ NO CLIP per scena {scene_number}: '{query}'", flush=True)
    return None, None

@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "healthy", "jobs": len(jobs)})

@app.route("/ffmpeg-test", methods=["GET"])
def ffmpeg_test():
    result = subprocess.run(["ffmpeg", "-version"], stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    firstline = result.stdout.splitlines()[0] if result.stdout else "no output"
    return jsonify({"ffmpeg_output": firstline})

@app.route("/status/<job_id>", methods=["GET"])
def get_status(job_id):
    job = jobs.get(job_id)
    if not job:
        return jsonify({"error": "Job not found"}), 404
    
    response = {
        "job_id": job_id,
        "status": job["status"],
        "created_at": job.get("created_at")
    }
    if job['status'] == 'completed':
        response['video_url'] = job.get('video_url')
        response['duration'] = job.get('duration')
        response['clips_used'] = job.get('clips_used')
    elif job['status'] == 'failed':
        response['error'] = job.get('error')
    
    return jsonify(response)

def process_video_async(job_id, data):
    """Processa video in background thread"""
    job = jobs[job_id]
    job["status"] = "processing"
    
    audiopath = None
    audio_wav_path = None
    video_looped_path = None
    final_video_path = None
    scene_paths = []
    
    try:
        if not all([R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY, R2_BUCKET_NAME, R2_PUBLIC_BASE_URL]):
            raise RuntimeError("Config R2 mancante")
        
        audiobase64 = data.get("audio_base64") or data.get("audiobase64")
        raw_script = (data.get("script") or data.get("script_chunk") or data.get("script_audio") or data.get("script_completo") or "")
        script = (" ".join(str(p).strip() for p in raw_script) if isinstance(raw_script, list) else str(raw_script).strip())
        raw_keywords = data.get("keywords", "")
        sheet_keywords = (", ".join(str(k).strip() for k in raw_keywords) if isinstance(raw_keywords, list) else str(raw_keywords).strip())
        
        # 🔧 FIX B/C: Parsing row_number ULTRA-ROBUSTO (gestisce nested/oggetti strani n8n)
        row_number_raw = data.get("row_number")
        if isinstance(row_number_raw, dict):
            row_number = int(row_number_raw.get('row', row_number_raw.get('row_number', 1)))
        elif isinstance(row_number_raw, str):
            row_number = int(row_number_raw) if row_number_raw.isdigit() else 1
        elif isinstance(row_number_raw, (int, float)):
            row_number = int(row_number_raw)
        else:
            row_number = 1

        print("=" * 80, flush=True)
        print(f"🎬 START AI TOOL MASTER: {len(script)} char script, keywords: '{sheet_keywords}', row: {row_number}", flush=True)
        print(f"🔍 DEBUG row_number RAW: '{row_number_raw}' → PARSED: '{row_number}'", flush=True)
        print(f"🔍 DEBUG GOOGLE_CREDENTIALS_JSON: {'PRESENTE ({len(GOOGLE_CREDENTIALS_JSON)} char)' if GOOGLE_CREDENTIALS_JSON else 'MANCANTE'}", flush=True)
        
        if not audiobase64:
            raise RuntimeError("audiobase64 mancante")
        
        # Audio processing
        audio_bytes = base64.b64decode(audiobase64)
        with tempfile.NamedTemporaryFile(delete=False, suffix=".bin") as f:
            f.write(audio_bytes)
        audiopath_tmp = f.name
        
        audio_wav_tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".wav")
        audio_wav_path = audio_wav_tmp.name
        audio_wav_tmp.close()
        
        subprocess.run([
            "ffmpeg", "-y", "-loglevel", "error", "-i", audiopath_tmp,
            "-acodec", "pcm_s16le", "-ar", "48000", audio_wav_path
        ], timeout=60, check=True)
        os.unlink(audiopath_tmp)
        audiopath = audio_wav_path
        
        # Real duration
        probe = subprocess.run([
            "ffprobe", "-v", "error", "-show_entries", "format=duration",
            "-of", "default=noprint_wrappers=1:nokey=1", audiopath
        ], stdout=subprocess.PIPE, text=True, timeout=10)
        real_duration = float(probe.stdout.strip() or 720.0)
        print(f"⏱️ Durata audio: {real_duration/60:.1f}min ({real_duration:.0f}s)", flush=True)
        
        # Scene sync
        script_words = script.lower().split()
        words_per_second = (len(script_words) / real_duration if real_duration > 0 else 2.5)
        avg_scene_duration = real_duration / 25
        scene_assignments = []
        
        for i in range(25):
            timestamp = i * avg_scene_duration
            word_index = int(timestamp * words_per_second)
            scene_context = " ".join(script_words[word_index: word_index + 7]) if word_index < len(script_words) else "ai tool technology laptop coding workflow"
            scene_query = pick_visual_query(scene_context, sheet_keywords)
            scene_assignments.append({
                "scene": i + 1, "timestamp": round(timestamp, 1),
                "context": scene_context[:60], "query": scene_query[:80]
            })
        
        # Download clips (ora dovrebbe prendere 20+ clip!)
        for assignment in scene_assignments:
            print(f"📍 Scene {assignment['scene']}: {assignment['timestamp']}s → '{assignment['context']}'", flush=True)
            clip_path, clip_dur = fetch_clip_for_scene(
                assignment["scene"], assignment["query"], avg_scene_duration
            )
            if clip_path and clip_dur:
                scene_paths.append((clip_path, clip_dur))
        
        print(f"✅ CLIPS SCARICATE: {len(scene_paths)}/25", flush=True)
        if len(scene_paths) < 5:
            raise RuntimeError(f"Troppe poche clip: {len(scene_paths)}/25")
        
        # Normalize clips
        normalized_clips = []
        for i, (clip_path, _dur) in enumerate(scene_paths):
            try:
                normalized_tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".mp4")
                normalized_path = normalized_tmp.name
                normalized_tmp.close()
                subprocess.run([
                    "ffmpeg", "-y", "-loglevel", "error", "-i", clip_path,
                    "-vf", "scale=1920:1080:force_original_aspect_ratio=increase,crop=1920:1080,fps=30",
                    "-c:v", "libx264", "-preset", "ultrafast", "-crf", "23", "-an", normalized_path
                ], timeout=120, check=True)
                if os.path.exists(normalized_path) and os.path.getsize(normalized_path) > 1000:
                    normalized_clips.append(normalized_path)
            except Exception:
                pass
        
        if not normalized_clips:
            raise RuntimeError("Nessuna clip normalizzata")
        
        # Concat
        def get_duration(p):
            out = subprocess.run([
                "ffprobe", "-v", "error", "-show_entries", "format=duration",
                "-of", "default=noprint_wrappers=1:nokey=1", p
            ], stdout=subprocess.PIPE, text=True, timeout=10).stdout.strip()
            return float(out or 4.0)
        
        total_clips_duration = sum(get_duration(p) for p in normalized_clips)
        concat_list_tmp = tempfile.NamedTemporaryFile(mode="w", delete=False, suffix=".txt")
        entries_written = 0
        MAX_CONCAT_ENTRIES = 150
        
        if total_clips_duration < real_duration and len(normalized_clips) > 1:
            loops_needed = math.ceil(real_duration / total_clips_duration)
            for _ in range(loops_needed):
                for norm_path in normalized_clips:
                    if entries_written >= MAX_CONCAT_ENTRIES:
                        break
                    concat_list_tmp.write(f"file '{norm_path}'\n")
                    entries_written += 1
                if entries_written >= MAX_CONCAT_ENTRIES:
                    break
        else:
            for norm_path in normalized_clips:
                concat_list_tmp.write(f"file '{norm_path}'\n")
                entries_written += 1
        
        concat_list_tmp.close()
        
        video_looped_tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".mp4")
        video_looped_path = video_looped_tmp.name
        video_looped_tmp.close()
        
        subprocess.run([
            "ffmpeg", "-y", "-loglevel", "error",
            "-f", "concat", "-safe", "0", "-i", concat_list_tmp.name,
            "-vf", "fps=30,format=yuv420p", "-c:v", "libx264", "-preset", "fast", "-crf", "23",
            "-t", str(real_duration), video_looped_path
        ], timeout=600, check=True)
        os.unlink(concat_list_tmp.name)
        
        # Final merge
        final_video_tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".mp4")
        final_video_path = final_video_tmp.name
        final_video_tmp.close()
        
        subprocess.run([
            "ffmpeg", "-y", "-loglevel", "error",
            "-i", video_looped_path, "-i", audiopath,
            "-filter_complex", "[0:v]scale=1920:1080:force_original_aspect_ratio=increase,crop=1920:1080,format=yuv420p[v]",
            "-map", "[v]", "-map", "1:a", "-c:v", "libx264", "-preset", "veryfast", "-crf", "23",
            "-c:a", "aac", "-b:a", "192k", "-shortest", final_video_path
        ], timeout=600, check=True)
        
        # R2 upload
        s3_client = get_s3_client()
        today = dt.datetime.utcnow().strftime("%Y-%m-%d")
        object_key = f"videos/{today}/{uuid.uuid4().hex}.mp4"
        s3_client.upload_file(
            Filename=final_video_path,
            Bucket=R2_BUCKET_NAME,
            Key=object_key,
            ExtraArgs={"ContentType": "video/mp4"}
        )
        public_url = f"{R2_PUBLIC_BASE_URL.rstrip('/')}/{object_key}"
        cleanup_old_videos(s3_client, object_key)
        
        # 🔧 Sheets update BULLETPROOF
        gc = get_gspread_client()
        print(f"🔍 DEBUG gspread client: {'OK' if gc else 'FAILED'}", flush=True)
        if gc and row_number > 0:
            try:
                sheet = gc.open_by_key(SPREADSHEET_ID).sheet1
                sheet.update_cell(row_number, 13, public_url)
                print(f"📊 ✅ Google Sheet row {row_number} col M(13) UPDATED: {public_url}", flush=True)
            except Exception as e:
                print(f"❌ Google Sheet update fallito row {row_number}: {str(e)}", flush=True)
        else:
            print(f"⚠️ Sheets SKIP: gc={'NO' if not gc else 'OK'} | row={row_number}", flush=True)
        
        # Cleanup
        paths_to_cleanup = [audiopath, video_looped_path, final_video_path] + normalized_clips + [p[0] for p in scene_paths]
        for path in paths_to_cleanup:
            try:
                os.unlink(path)
            except Exception:
                pass
        
        print(f"✅ 🎬 VIDEO AI TOOL MASTER COMPLETO: {real_duration/60:.1f}min → {public_url}", flush=True)
        
        job.update({
            "status": "completed",
            "video_url": public_url,
            "duration": real_duration,
            "clips_used": len(scene_paths),
            "row_number": row_number
        })
        
    except Exception as e:
        print(f"❌ ERRORE PROCESSING: {e}", flush=True)
        job.update({"status": "failed", "error": str(e)})
    
    finally:
        # Cleanup job dopo 1h
        Thread(target=lambda: cleanup_job_delayed(job_id), daemon=True).start()

def cleanup_job_delayed(job_id, delay=3600):
    """Cancella job dopo delay secondi"""
    import time
    time.sleep(delay)
    if job_id in jobs:
        del jobs[job_id]

@app.route("/generate", methods=["POST"])
def generate():
    """Endpoint principale: crea job async"""
    try:
        data = request.get_json(force=True) or {}
        job_id = str(uuid.uuid4())
        
        jobs[job_id] = {
            "status": "queued",
            "created_at": dt.datetime.utcnow().isoformat(),
            "data": data
        }
        
        # Pulizia jobs vecchi
        if len(jobs) > MAX_JOBS:
            old_jobs = sorted(jobs.keys(), key=lambda k: jobs[k]["created_at"])[:len(jobs)-MAX_JOBS]
            for oj in old_jobs:
                del jobs[oj]
        
        # Avvia processing async
        Thread(target=process_video_async, args=(job_id, data), daemon=True).start()
        
        print(f"🚀 Job {job_id} QUEUED: raw_row={data.get('row_number')}", flush=True)
        return jsonify({
            "success": True,
            "job_id": job_id,
            "status": "queued",
            "message": "Video generation started (check /status/<job_id>)"
        })
    
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port, threaded=True)

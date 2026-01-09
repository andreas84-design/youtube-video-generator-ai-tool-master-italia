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

# 🔥 WEBHOOK n8n
N8N_WEBHOOK_URL = os.environ.get("N8N_WEBHOOK_URL")

# 🔥 In-memory job storage (upgrade to Redis in produzione!)
jobs = {}


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
                    print(f"🗑️  Cancellato vecchio video: {key}", flush=True)

        if deleted_count > 0:
            print(f"✅ Rotazione completata: {deleted_count} video vecchi rimossi", flush=True)
        else:
            print("✅ Nessun video vecchio da rimuovere", flush=True)

    except Exception as e:
        print(f"⚠️  Errore rotazione R2 (video vecchi restano): {str(e)}", flush=True)


def pick_visual_query(context: str, keywords_text: str = "") -> str:
    """Query ottimizzate per B‑roll tech"""
    ctx = (context or "").lower()
    kw = (keywords_text or "").lower()

    base = "ai workstation, laptop, coding, workflow, office, technology, screens"

    if any(w in ctx for w in ["produttivit", "lavoro", "task", "automat", "workflow", "routine"]):
        return "person at laptop automation workflow screen, modern office, productivity, ai interface"

    if any(w in ctx for w in ["prompt", "chatgpt", "gpt", "llm", "modello linguistico"]):
        return "close up of computer screen with chat interface, prompt highlighted, dark background, green code"

    if any(w in ctx for w in ["n8n", "webhook", "api", "integrazione", "scenario", "flow"]):
        return "monitor with colorful flowchart automation nodes, glowing lines connecting apps, dark tech background"

    if any(w in ctx for w in ["excel", "foglio", "sheets", "google sheets", "dati", "report", "tabella"]):
        return "person working on spreadsheet on laptop, charts and tables on screen, clean office desk"

    if any(w in ctx for w in ["tastiera", "mouse", "supporto", "laptop", "webcam", "monitor"]):
        return "minimal desk setup with laptop on stand, ergonomic keyboard and mouse, soft rgb lights, tech workspace"

    if any(w in ctx for w in ["libro", "studia", "formazione", "corso", "lezione", "impara"]):
        return "open book next to laptop with ai interface, notes and highlighters on desk, cozy learning environment"

    if kw and kw != "none":
        return f"{kw}, modern office, laptop, screens, technology"

    return base


def fetch_clip_for_scene(scene_number: int, query: str, avg_scene_duration: float):
    """Fetch B-roll tech clips"""
    target_duration = min(4.0, avg_scene_duration)

    def is_tech_video_metadata(video_data, source):
        banned = ["dog", "cat", "animal", "wildlife", "bird", "fish", "horse", "fitness", "yoga", "workout", "kitchen", "cooking", "food"]
        if source == "pexels":
            text = (video_data.get("description", "") + " " + " ".join(video_data.get("tags", []))).lower()
        else:
            text = " ".join(video_data.get("tags", [])).lower()

        has_banned = any(kw in text for kw in banned)
        return not has_banned

    def download_file(url: str) -> str:
        tmp_clip = tempfile.NamedTemporaryFile(delete=False, suffix=".mp4")
        clip_resp = requests.get(url, stream=True, timeout=30)
        clip_resp.raise_for_status()
        for chunk in clip_resp.iter_content(chunk_size=1024 * 1024):
            if chunk:
                tmp_clip.write(chunk)
        tmp_clip.close()
        return tmp_clip.name

    def try_pexels():
        if not PEXELS_API_KEY:
            return None
        headers = {"Authorization": PEXELS_API_KEY}
        params = {
            "query": f"{query} laptop computer screen coding technology office",
            "orientation": "landscape",
            "per_page": 25,
            "page": random.randint(1, 3),
        }
        resp = requests.get("https://api.pexels.com/videos/search", headers=headers, params=params, timeout=20)
        if resp.status_code != 200:
            return None

        videos = resp.json().get("videos", [])
        tech_videos = [v for v in videos if is_tech_video_metadata(v, "pexels")]

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
            "q": f"{query} laptop computer screen coding technology office",
            "per_page": 25,
            "safesearch": "true",
            "min_width": 1280,
        }
        resp = requests.get("https://pixabay.com/api/videos/", params=params, timeout=20)
        if resp.status_code != 200:
            return None

        hits = resp.json().get("hits", [])
        for hit in hits:
            if is_tech_video_metadata(hit, "pixabay"):
                videos = hit.get("videos", {})
                for quality in ["large", "medium", "small"]:
                    if quality in videos and "url" in videos[quality]:
                        return download_file(videos[quality]["url"])
        return None

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


# 🔥 ASYNC VIDEO PROCESSING
def process_video_async(job_id: str, data: dict):
    """Background thread per generazione video (può durare 20+ minuti!)"""
    audiopath = None
    audio_wav_path = None
    video_looped_path = None
    final_video_path = None
    scene_paths = []

    try:
        print(f"🎬 [{job_id}] START processing...", flush=True)
        jobs[job_id]['status'] = 'processing'

        audiobase64 = data.get("audio_base64") or data.get("audiobase64")
        raw_script = data.get("script") or data.get("script_chunk") or data.get("script_audio") or data.get("script_completo") or ""
        script = " ".join(str(p).strip() for p in raw_script) if isinstance(raw_script, list) else str(raw_script).strip()

        raw_keywords = data.get("keywords", "")
        sheet_keywords = ", ".join(str(k).strip() for k in raw_keywords) if isinstance(raw_keywords, list) else str(raw_keywords).strip()

        if not audiobase64:
            raise ValueError("audiobase64 mancante")

        # Audio processing
        audio_bytes = base64.b64decode(audiobase64)
        with tempfile.NamedTemporaryFile(delete=False, suffix=".bin") as f:
            f.write(audio_bytes)
            audiopath_tmp = f.name

        audio_wav_tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".wav")
        audio_wav_path = audio_wav_tmp.name
        audio_wav_tmp.close()

        subprocess.run(["ffmpeg", "-y", "-loglevel", "error", "-i", audiopath_tmp, "-acodec", "pcm_s16le", "-ar", "48000", audio_wav_path], timeout=60, check=True)
        os.unlink(audiopath_tmp)
        audiopath = audio_wav_path

        # Duration
        probe = subprocess.run(["ffprobe", "-v", "error", "-show_entries", "format=duration", "-of", "default=noprint_wrappers=1:nokey=1", audiopath], stdout=subprocess.PIPE, text=True, timeout=10)
        real_duration = float(probe.stdout.strip()) if probe.stdout.strip() else 720.0

        print(f"⏱️  [{job_id}] Durata: {real_duration/60:.1f}min", flush=True)

        # Scene sync
        script_words = script.lower().split()
        words_per_second = len(script_words) / real_duration if real_duration > 0 else 2.5
        avg_scene_duration = real_duration / 25

        scene_assignments = []
        for i in range(25):
            timestamp = i * avg_scene_duration
            word_index = int(timestamp * words_per_second)
            scene_context = " ".join(script_words[word_index: word_index + 7]) if word_index < len(script_words) else "ai workstation laptop coding workflow"
            scene_query = pick_visual_query(scene_context, sheet_keywords)
            scene_assignments.append({"scene": i + 1, "timestamp": round(timestamp, 1), "context": scene_context[:60], "query": scene_query[:80]})

        # Download clips
        for assignment in scene_assignments:
            clip_path, clip_dur = fetch_clip_for_scene(assignment["scene"], assignment["query"], avg_scene_duration)
            if clip_path and clip_dur:
                scene_paths.append((clip_path, clip_dur))

        print(f"✅ [{job_id}] CLIPS: {len(scene_paths)}/25", flush=True)

        if len(scene_paths) < 5:
            raise RuntimeError(f"Troppe poche clip: {len(scene_paths)}/25")

        # Normalize + concat + merge (tuo code esistente)
        normalized_clips = []
        for i, (clip_path, _dur) in enumerate(scene_paths):
            try:
                normalized_tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".mp4")
                normalized_path = normalized_tmp.name
                normalized_tmp.close()

                subprocess.run(["ffmpeg", "-y", "-loglevel", "error", "-i", clip_path, "-vf", "scale=1920:1080:force_original_aspect_ratio=increase,crop=1920:1080,fps=30", "-c:v", "libx264", "-preset", "ultrafast", "-crf", "23", "-an", normalized_path], timeout=120, check=True)

                if os.path.exists(normalized_path) and os.path.getsize(normalized_path) > 1000:
                    normalized_clips.append(normalized_path)
            except Exception:
                pass

        if not normalized_clips:
            raise RuntimeError("Nessuna clip normalizzata")

        # Concat
        def get_duration(p):
            out = subprocess.run(["ffprobe", "-v", "error", "-show_entries", "format=duration", "-of", "default=noprint_wrappers=1:nokey=1", p], stdout=subprocess.PIPE, text=True, timeout=10).stdout.strip()
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

        subprocess.run(["ffmpeg", "-y", "-loglevel", "error", "-f", "concat", "-safe", "0", "-i", concat_list_tmp.name, "-vf", "fps=30,format=yuv420p", "-c:v", "libx264", "-preset", "fast", "-crf", "23", "-t", str(real_duration), video_looped_path], timeout=600, check=True)
        os.unlink(concat_list_tmp.name)

        # Final merge
        final_video_tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".mp4")
        final_video_path = final_video_tmp.name
        final_video_tmp.close()

        subprocess.run(["ffmpeg", "-y", "-loglevel", "error", "-i", video_looped_path, "-i", audiopath, "-filter_complex", "[0:v]scale=1920:1080:force_original_aspect_ratio=increase,crop=1920:1080,format=yuv420p[v]", "-map", "[v]", "-map", "1:a", "-c:v", "libx264", "-preset", "veryfast", "-crf", "23", "-c:a", "aac", "-b:a", "192k", "-shortest", final_video_path], timeout=600, check=True)

        # R2 upload
        s3_client = get_s3_client()
        today = dt.datetime.utcnow().strftime("%Y-%m-%d")
        object_key = f"videos/{today}/{uuid.uuid4().hex}.mp4"

        s3_client.upload_file(Filename=final_video_path, Bucket=R2_BUCKET_NAME, Key=object_key, ExtraArgs={"ContentType": "video/mp4"})
        public_url = f"{R2_PUBLIC_BASE_URL.rstrip('/')}/{object_key}"
        cleanup_old_videos(s3_client, object_key)

        # Cleanup
        for path in [audiopath, video_looped_path, final_video_path] + normalized_clips + [p[0] for p in scene_paths]:
            try:
                os.unlink(path)
            except Exception:
                pass

        print(f"✅ [{job_id}] VIDEO COMPLETO: {public_url}", flush=True)

        # Update job
        jobs[job_id]['status'] = 'completed'
        jobs[job_id]['video_url'] = public_url
        jobs[job_id]['duration'] = real_duration
        jobs[job_id]['clips_used'] = len(scene_paths)

        # 🔥 WEBHOOK CALLBACK A n8n!
        if N8N_WEBHOOK_URL:
            try:
                callback_payload = {
                    'job_id': job_id,
                    'status': 'completed',
                    'video_url': public_url,
                    'duration': real_duration,
                    'clips_used': len(scene_paths),
                    'original_data': data
                }
                resp = requests.post(N8N_WEBHOOK_URL, json=callback_payload, timeout=30)
                print(f"🔔 [{job_id}] Webhook callback sent! Status: {resp.status_code}", flush=True)
            except Exception as e:
                print(f"⚠️  [{job_id}] Webhook callback failed: {e}", flush=True)

    except Exception as e:
        print(f"❌ [{job_id}] ERROR: {e}", flush=True)
        jobs[job_id]['status'] = 'failed'
        jobs[job_id]['error'] = str(e)

        # Cleanup
        for path in [audiopath, audio_wav_path, video_looped_path, final_video_path] + [p[0] for p in scene_paths]:
            try:
                os.unlink(path)
            except Exception:
                pass

        # Webhook error callback
        if N8N_WEBHOOK_URL:
            try:
                requests.post(N8N_WEBHOOK_URL, json={'job_id': job_id, 'status': 'failed', 'error': str(e), 'original_data': data}, timeout=30)
            except Exception:
                pass


@app.route("/ffmpeg-test", methods=["GET"])
def ffmpeg_test():
    result = subprocess.run(["ffmpeg", "-version"], stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    firstline = result.stdout.splitlines()[0] if result.stdout else "no output"
    return jsonify({"ffmpeg_output": firstline})


@app.route("/generate", methods=["POST"])
def generate():
    """🔥 ASYNC ENDPOINT: risponde subito con job_id!"""
    try:
        if not all([R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY, R2_BUCKET_NAME, R2_PUBLIC_BASE_URL]):
            return jsonify({"success": False, "error": "Config R2 mancante", "video_url": None}), 500

        data = request.get_json(force=True) or {}
        job_id = str(uuid.uuid4())

        # Salva job
        jobs[job_id] = {
            'status': 'queued',
            'data': data,
            'created_at': dt.datetime.utcnow().isoformat()
        }

        # 🔥 Start background thread!
        thread = Thread(target=process_video_async, args=(job_id, data))
        thread.daemon = True
        thread.start()

        print(f"🚀 [{job_id}] Job created! Processing in background...", flush=True)

        # Risposta IMMEDIATA (< 1 secondo!)
        return jsonify({
            "success": True,
            "job_id": job_id,
            "status": "processing",
            "message": "Video generation started. You will receive webhook callback when ready."
        }), 202

    except Exception as e:
        print(f"❌ Job creation error: {e}", flush=True)
        return jsonify({"success": False, "error": str(e)}), 500


@app.route("/status/<job_id>", methods=["GET"])
def get_status(job_id):
    """Endpoint per check status (fallback se webhook fallisce)"""
    if job_id not in jobs:
        return jsonify({'error': 'Job not found'}), 404

    job = jobs[job_id]
    response = {
        'job_id': job_id,
        'status': job['status'],
        'created_at': job.get('created_at')
    }

    if job['status'] == 'completed':
        response['video_url'] = job.get('video_url')
        response['duration'] = job.get('duration')
        response['clips_used'] = job.get('clips_used')
    elif job['status'] == 'failed':
        response['error'] = job.get('error')

    return jsonify(response)


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port)
    

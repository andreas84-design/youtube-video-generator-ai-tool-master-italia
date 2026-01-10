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

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger(__name__)

app = Flask(__name__)

R2_ACCESS_KEY_ID = os.environ.get("R2_ACCESS_KEY_ID")
R2_SECRET_ACCESS_KEY = os.environ.get("R2_SECRET_ACCESS_KEY")
R2_BUCKET_NAME = os.environ.get("R2_BUCKET_NAME")
R2_PUBLIC_BASE_URL = os.environ.get("R2_PUBLIC_BASE_URL", "")
R2_ACCOUNT_ID = os.environ.get("R2_ACCOUNT_ID", "")
R2_REGION = os.environ.get("R2_REGION", "auto")

PEXELS_API_KEY = os.environ.get("PEXELS_API_KEY", "")
PIXABAY_API_KEY = os.environ.get("PIXABAY_API_KEY", "")
N8N_WEBHOOK_URL = os.environ.get("N8N_WEBHOOK_URL", "")

jobs = {}
MAX_JOBS = 50

def get_s3_client():
    try:
        if not R2_ACCOUNT_ID:
            raise RuntimeError("R2_ACCOUNT_ID mancante")
        endpoint_url = f"https://{R2_ACCOUNT_ID}.r2.cloudflarestorage.com"
        
        session = boto3.session.Session()
        return session.client(
            's3',
            region_name=R2_REGION,
            endpoint_url=endpoint_url,
            aws_access_key_id=R2_ACCESS_KEY_ID,
            aws_secret_access_key=R2_SECRET_ACCESS_KEY,
            config=Config(signature_version='s3v4', addressing_style='virtual')
        )
    except Exception as e:
        logger.error(f"R2 client fail: {e}")
        raise

def cleanup_jobs():
    global jobs
    if len(jobs) > MAX_JOBS:
        old_keys = sorted(jobs.keys(), key=lambda k: jobs[k].get('created_at', ''))[:len(jobs)-MAX_JOBS]
        for k in old_keys:
            del jobs[k]

def pick_visual_query(context: str, keywords_text: str = "") -> str:
    if isinstance(context, list):
        context = ' '.join(str(c) for c in context)
    ctx = (context or "").lower()
    if isinstance(keywords_text, list):
    keywords_text = ' '.join(str(k) for k in keywords_text)
    kw = (keywords_text or "").lower()
    base = "ai workstation laptop coding workflow office technology screens"
    
    queries = {
        "produttivit": "person laptop automation workflow modern office productivity",
        "prompt": "computer screen chat interface prompt dark background",
        "n8n": "monitor flowchart automation nodes glowing lines",
        "excel": "spreadsheet laptop charts tables clean desk",
        "tastiera": "desk laptop ergonomic keyboard mouse",
        "libro": "book laptop ai interface notes"
    }
    
    for key, q in queries.items():
        if key in ctx:
            return q
    
    return f"{kw} laptop computer coding office" if kw else base

def fetch_clip_for_scene(scene_number: int, query: str, avg_duration: float):
    target_duration = min(4.0, avg_duration)
    
    def download_file(url: str):
        tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".mp4")
        try:
            resp = requests.get(url, stream=True, timeout=30)
            resp.raise_for_status()
            for chunk in resp.iter_content(1024*1024):
                if chunk: tmp.write(chunk)
            tmp.close()
            return tmp.name
        except:
            try: os.unlink(tmp.name)
            except: pass
            return None
    
    if PEXELS_API_KEY:
        try:
            resp = requests.get("https://api.pexels.com/videos/search", 
                              headers={"Authorization": PEXELS_API_KEY},
                              params={"query": query, "orientation": "landscape", "per_page": 15, "page": random.randint(1,3)},
                              timeout=20)
            if resp.status_code == 200:
                videos = resp.json().get("videos", [])
                if videos:
                    vf = random.choice(videos)["video_files"][0]
                    path = download_file(vf["link"])
                    if path: return path, target_duration
        except: pass
    
    if PIXABAY_API_KEY:
        try:
            resp = requests.get("https://pixabay.com/api/videos/", 
                              params={"key": PIXABAY_API_KEY, "q": query, "per_page": 15},
                              timeout=20)
            if resp.status_code == 200:
                for hit in resp.json().get("hits", []):
                    url = hit["videos"].get("medium", {}).get("url")
                    if url:
                        path = download_file(url)
                        if path: return path, target_duration
        except: pass
    
    return None, None

def process_video_async(job_id: str, data: dict):
    audiopath = video_looped = final_video = None
    scene_paths = []
    
    try:
        logger.info(f"[{job_id}] START Row:{data.get('row_number','N/A')}")
        cleanup_jobs()
        
        webhook_url = data.get('webhook_url') or N8N_WEBHOOK_URL
        row_number = int(data.get('row_number') or 1)
        
        jobs[job_id] = {
            'status': 'processing',
            'created_at': dt.datetime.utcnow().isoformat(),
            'webhook_url': webhook_url,
            'row_number': row_number
        }
        
        # 🔥 FIX: gestisci audio_base64 O audiobase64
        audio_b64 = data.get('audio_base64') or data.get('audiobase64')
        if not audio_b64:
            logger.error(f"[{job_id}] MISSING AUDIO - keys: {list(data.keys())}")
            raise ValueError("No audio_base64 or audiobase64 in request")
        
        logger.info(f"[{job_id}] Audio len: {len(audio_b64)}")
        
        # Decode audio
        audio_bytes = base64.b64decode(audio_b64)
        audiopath = tempfile.mktemp(suffix='.mp3')
        with open(audiopath, 'wb') as f:
            f.write(audio_bytes)
        
        # Convert to WAV
        audio_wav = tempfile.mktemp(suffix='.wav')
        subprocess.run(["ffmpeg", "-y", "-i", audiopath, "-acodec", "pcm_s16le", "-ar", "48000", audio_wav], 
                      timeout=120, check=True, capture_output=True)
        os.unlink(audiopath)
        audiopath = audio_wav
        
        # Duration
        probe = subprocess.run(["ffprobe", "-v", "error", "-show_entries", "format=duration", 
                               "-of", "csv=p=0", audiopath], 
                              stdout=subprocess.PIPE, text=True, timeout=10)
        duration = float(probe.stdout.strip() or 600)
        
        logger.info(f"[{job_id}] Duration: {duration/60:.1f}min")
        
        # Scene fetch (max 20)
        script_raw = data.get("script_chunk") or data.get("script") or ""
        if isinstance(script_raw, list):
            script_raw = ' '.join(str(s) for s in script_raw)
        logger.info(f"[{job_id}] script_raw type: {type(script_raw)}, len: {len(script_raw)}")
        script = script_raw.lower().split()

        keywords = data.get("keywords", "")
        avg_dur = duration / 20
        
        for i in range(20):
            word_idx = int((i * avg_dur) * (len(script) / duration)) if script else 0
            context = " ".join(script[word_idx:word_idx+5]) if word_idx < len(script) else ""
            query = pick_visual_query(context, keywords)
            
            path, _ = fetch_clip_for_scene(i+1, query, avg_dur)
            if path:
                scene_paths.append(path)
            if len(scene_paths) >= 15: break
        
        logger.info(f"[{job_id}] Clips: {len(scene_paths)}")
        if len(scene_paths) < 3:
            raise RuntimeError("Clip insufficienti")
        
        # Normalize
        normalized = []
        for i, path in enumerate(scene_paths):
            norm = tempfile.mktemp(suffix='.mp4')
            try:
                subprocess.run(["ffmpeg", "-y", "-i", path, 
                              "-vf", "scale=1920:1080:force_original_aspect_ratio=decrease,pad=1920:1080:(ow-iw)/2:(oh-ih)/2,fps=30", 
                              "-c:v", "libx264", "-preset", "ultrafast", "-crf", "28", "-an", norm], 
                             timeout=90, check=True, capture_output=True)
                if os.path.exists(norm) and os.path.getsize(norm) > 50000:
                    normalized.append(norm)
            except:
                try: os.unlink(norm)
                except: pass
        
        if not normalized:
            raise RuntimeError("Nessuna clip valida")
        
        # Concat
        concat_list = tempfile.mktemp(suffix='.txt')
        with open(concat_list, 'w') as f:
            loops = max(1, math.ceil(duration / (len(normalized) * 4)))
            for _ in range(min(loops, 3)):
                for p in normalized:
                    f.write(f"file '{p}'\n")
        
        video_looped = tempfile.mktemp(suffix='.mp4')
        subprocess.run(["ffmpeg", "-y", "-f", "concat", "-safe", "0", "-i", concat_list, 
                      "-c", "copy", "-t", str(duration), video_looped], 
                     timeout=300, check=True, capture_output=True)
        os.unlink(concat_list)
        
        # Merge
        final_video = tempfile.mktemp(suffix='.mp4')
        subprocess.run(["ffmpeg", "-y", "-i", video_looped, "-i", audiopath, 
                      "-c:v", "libx264", "-preset", "veryfast", "-crf", "25", 
                      "-c:a", "aac", "-shortest", final_video], 
                     timeout=300, check=True, capture_output=True)
        
        # Upload R2
        s3 = get_s3_client()
        key = f"videos/{dt.datetime.utcnow().strftime('%Y%m%d_%H%M')}_{job_id}.mp4"
        s3.upload_file(final_video, R2_BUCKET_NAME, key, ExtraArgs={'ContentType': 'video/mp4'})
        video_url = f"{R2_PUBLIC_BASE_URL.rstrip('/')}/{key}"
        
        # Cleanup
        for path in [audiopath, video_looped, final_video] + normalized + scene_paths:
            try: os.unlink(path)
            except: pass
        
        jobs[job_id].update({
            'status': 'completed',
            'video_url': video_url,
            'duration': duration,
            'clips_used': len(scene_paths)
        })
        logger.info(f"[{job_id}] SUCCESS: {video_url}")
        
        if webhook_url:
            try:
                requests.post(webhook_url, json={
                    'jobid': job_id,
                    'status': 'completed',
                    'videourl': video_url,
                    'duration': duration,
                    'clipsused': len(scene_paths),
                    'row_number': row_number,
                    'originaldata': data
                }, timeout=10)
            except: pass
            
    except Exception as e:
        logger.error(f"[{job_id}] FAIL: {str(e)}")
        jobs[job_id]['status'] = 'failed'
        jobs[job_id]['error'] = str(e)
        
        if 'webhook_url' in jobs[job_id] and jobs[job_id]['webhook_url']:
            try:
                requests.post(jobs[job_id]['webhook_url'], 
                            json={'jobid': job_id, 'status': 'failed', 'error': str(e)}, 
                            timeout=10)
            except: pass
        
        for path in [audiopath, video_looped, final_video] + scene_paths:
            try: 
                if path: os.unlink(path)
            except: pass

@app.route("/generate", methods=["POST"])
def generate():
    data = request.get_json() or {}
    job_id = str(uuid.uuid4())
    
    # 🔥 DEBUG
    logger.info(f"POST /generate keys: {list(data.keys())}")
    logger.info(f"audio_base64: {'YES' if data.get('audio_base64') else 'NO'} | audiobase64: {'YES' if data.get('audiobase64') else 'NO'}")
    
    if not all([R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY, R2_BUCKET_NAME]):
        return jsonify({'success': False, 'error': 'R2 config mancante'}), 500
    
    jobs[job_id] = {'status': 'queued'}
    Thread(target=process_video_async, args=(job_id, data), daemon=True).start()
    
    logger.info(f"Job queued: {job_id}")
    return jsonify({'success': True, 'job_id': job_id, 'status': 'processing'}), 202

@app.route("/status/<job_id>", methods=["GET"])
def status(job_id):
    job = jobs.get(job_id, {'status': 'not_found'})
    resp = {'jobid': job_id, 'status': job['status']}
    if job['status'] == 'completed':
        resp.update({k: job.get(k) for k in ['video_url', 'duration', 'clips_used', 'row_number']})
    elif job['status'] == 'failed':
        resp['error'] = job.get('error')
    return jsonify(resp)

@app.route("/health", methods=["GET"])
def health():
    return jsonify({'status': 'ok', 'jobs': len(jobs)})

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port, debug=False)

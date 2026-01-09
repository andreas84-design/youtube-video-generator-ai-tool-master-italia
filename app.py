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

# Logging esteso Railway
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger(__name__)

app = Flask(__name__)

# Config R2 con fallback
R2_ACCESS_KEY_ID = os.environ.get("R2_ACCESS_KEY_ID")
R2_SECRET_ACCESS_KEY = os.environ.get("R2_SECRET_ACCESS_KEY")
R2_BUCKET_NAME = os.environ.get("R2_BUCKET_NAME")
R2_PUBLIC_BASE_URL = os.environ.get("R2_PUBLIC_BASE_URL", "")
R2_ACCOUNT_ID = os.environ.get("R2_ACCOUNT_ID", "")
R2_REGION = os.environ.get("R2_REGION", "auto")

# API fallback
PEXELS_API_KEY = os.environ.get("PEXELS_API_KEY", "")
PIXABAY_API_KEY = os.environ.get("PIXABAY_API_KEY", "")
N8N_WEBHOOK_URL = os.environ.get("N8N_WEBHOOK_URL", "")

# Job storage limitato (max 50 per anti-memoria)
jobs = {}
MAX_JOBS = 50

def get_s3_client():
    """R2 client con fallback"""
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
    """Limita jobs in memoria"""
    global jobs
    if len(jobs) > MAX_JOBS:
        old_keys = sorted(jobs.keys(), key=lambda k: jobs[k].get('created_at', ''))[:len(jobs)-MAX_JOBS]
        for k in old_keys:
            del jobs[k]
        logger.info(f"Cleanup: {len(old_keys)} old jobs rimossi")

def pick_visual_query(context: str, keywords_text: str = "") -> str:
    ctx = (context or "").lower()
    kw = (keywords_text or "").lower()
    base = "ai workstation laptop coding workflow office technology screens"
    
    queries = {
        "produttivit": "person laptop automation workflow modern office productivity ai interface",
        "prompt": "computer screen chat interface prompt dark background green code",
        "n8n": "monitor flowchart automation nodes glowing lines dark tech",
        "excel": "spreadsheet laptop charts tables clean desk",
        "tastiera": "desk laptop stand ergonomic keyboard mouse rgb lights",
        "libro": "book laptop ai interface notes cozy learning"
    }
    
    for key, q in queries.items():
        if key in ctx:
            return q
    
    return f"{kw} laptop computer screen coding office" if kw else base

def fetch_clip_for_scene(scene_number: int, query: str, avg_duration: float):
    """Fetch clip con timeout"""
    target_duration = min(4.0, avg_duration)
    
    def download_file(url: str) -> str:
        tmp_clip = tempfile.NamedTemporaryFile(delete=False, suffix=".mp4")
        try:
            resp = requests.get(url, stream=True, timeout=30)
            resp.raise_for_status()
            for chunk in resp.iter_content(1024*1024):
                if chunk: tmp_clip.write(chunk)
            tmp_clip.close()
            return tmp_clip.name
        except:
            try: os.unlink(tmp_clip.name)
            except: pass
            return None
    
    # Pexels
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
    
    # Pixabay fallback
    if PIXABAY_API_KEY:
        try:
            resp = requests.get("https://pixabay.com/api/videos/", 
                              params={"key": PIXABAY_API_KEY, "q": query, "per_page": 15, "min_width": 1280},
                              timeout=20)
            if resp.status_code == 200:
                for hit in resp.json().get("hits", []):
                    url = hit["videos"].get("medium", {}).get("url")
                    if url:
                        path = download_file(url)
                        if path: return path, target_duration
        except: pass
    
    logger.warning(f"No clip scena {scene_number}: {query}")
    return None, None

def process_video_async(job_id: str, data: dict):
    """Processo principale con try/except totali"""
    audiopath = video_looped_path = final_video_path = None
    scene_paths = []
    
    try:
        logger.info(f"[{job_id}] START | Row: {data.get('row_number', 'N/A')}")
        cleanup_jobs()
        jobs[job_id] = {'status': 'processing', 'created_at': dt.datetime.utcnow().isoformat(), 'data': data}
        
        webhook_url = data.get('webhook_url') or N8N_WEBHOOK_URL
        row_number = int(data.get('row_number') or data.get('RowID') or 1)
        jobs[job_id]['webhook_url'] = webhook_url
        jobs[job_id]['row_number'] = row_number
        
        # Audio decode/normalize
        audio_bytes = base64.b64decode(data.get("audio_base64") or "")
        audiopath = tempfile.mktemp(suffix='.wav')
        with open(audiopath, 'wb') as f:
            subprocess.run(["ffmpeg", "-y", "-i", f"data:audio/mp3;base64,{data['audio_base64']}", 
                          "-acodec", "pcm_s16le", "-ar", "48000", audiopath], 
                         timeout=120, check=True, capture_output=True)
        
        # Durata reale
        duration_cmd = subprocess.run(["ffprobe", "-v", "error", "-show_entries", "format=duration", 
                                     "-of", "default=nw=1:nk=1", audiopath], 
                                    stdout=subprocess.PIPE, text=True, timeout=10)
        real_duration = float(duration_cmd.stdout.strip() or 720)
        
        logger.info(f"[{job_id}] Durata: {real_duration/60:.1f}m | Words: {len(data.get('script',''))}")
        
        # Scene (max 20 anti-timeout)
        avg_scene_dur = real_duration / 20
        scene_assignments = []
        script_words = (data.get("script", "") or "").lower().split()
        words_per_sec = max(len(script_words) / real_duration, 2.5)
        
        for i in range(20):
            ts = i * avg_scene_dur
            word_idx = int(ts * words_per_sec)
            context = " ".join(script_words[word_idx:word_idx+7]) if word_idx < len(script_words) else ""
            query = pick_visual_query(context, data.get("keywords", ""))
            scene_assignments.append({"query": query})
        
        # Download clips (max 20)
        for assign in scene_assignments:
            path, _ = fetch_clip_for_scene(len(scene_paths)+1, assign["query"], avg_scene_dur)
            if path:
                scene_paths.append(path)
            if len(scene_paths) >= 15: break  # Limite anti-mem
        
        logger.info(f"[{job_id}] Clips: {len(scene_paths)}/20")
        if len(scene_paths) < 3:
            raise RuntimeError("Clip insufficienti")
        
        # Normalize clips
        normalized_clips = []
        for i, path in enumerate(scene_paths[:15]):
            norm_path = tempfile.mktemp(suffix='.mp4')
            try:
                subprocess.run(["ffmpeg", "-y", "-loglevel", "error", "-i", path, 
                              "-vf", "scale=1920:1080:force_original_aspect_ratio=decrease,pad=1920:1080:(ow-iw)/2:(oh-ih)/2,fps=30", 
                              "-c:v", "libx264", "-preset", "ultrafast", "-crf", "28", "-an", norm_path], 
                             timeout=90, check=True, capture_output=True)
                if os.path.exists(norm_path) and os.path.getsize(norm_path) > 50000:
                    normalized_clips.append(norm_path)
            except:
                try: os.unlink(norm_path)
                except: pass
        
        if not normalized_clips:
            raise RuntimeError("Nessuna clip valida")
        
        logger.info(f"[{job_id}] Normalized: {len(normalized_clips)}")
        
        # Concat list (loop se necessario)
        concat_list = tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.txt').name
        with open(concat_list, 'w') as f:
            total_dur = sum(subprocess.run(["ffprobe", "-v", "error", "-show_entries", "format=duration", 
                                          "-of", "csv=p=0", p], stdout=subprocess.PIPE, text=True, 
                                         timeout=10).stdout.strip() for p in normalized_clips)
            loops = max(1, math.ceil(real_duration / total_dur))
            for _ in range(min(loops, 3)):  # Max 3 loop
                for p in normalized_clips:
                    f.write(f"file '{p}'\n")
        
        # Video looped
        video_looped_path = tempfile.mktemp(suffix='.mp4')
        subprocess.run(["ffmpeg", "-y", "-f", "concat", "-safe", "0", "-i", concat_list, 
                      "-c", "copy", "-t", str(real_duration), video_looped_path], 
                     timeout=300, check=True, capture_output=True)
        os.unlink(concat_list)
        
        # Final merge audio+video
        final_video_path = tempfile.mktemp(suffix='.mp4')
        subprocess.run(["ffmpeg", "-y", "-i", video_looped_path, "-i", audiopath, 
                      "-c:v", "libx264", "-preset", "veryfast", "-crf", "25", 
                      "-c:a", "aac", "-shortest", final_video_path], 
                     timeout=300, check=True, capture_output=True)
        
        # R2 upload
        s3 = get_s3_client()
        key = f"videos/{dt.datetime.utcnow().strftime('%Y%m%d_%H%M')}_{job_id}.mp4"
        s3.upload_file(final_video_path, R2_BUCKET_NAME, key, ExtraArgs={'ContentType': 'video/mp4'})
        public_url = f"{R2_PUBLIC_BASE_URL.rstrip('/')}/{key}"
        
        # Cleanup
        for path in [audiopath, video_looped_path, final_video_path] + normalized_clips + scene_paths:
            try: os.unlink(path)
            except: pass
        
        # Job success + webhook
        jobs[job_id].update({
            'status': 'completed', 'video_url': public_url, 
            'duration': real_duration, 'clips_used': len(scene_paths)
        })
        logger.info(f"[{job_id}] SUCCESS: {public_url}")
        
        if webhook_url:
            try:
                requests.post(webhook_url, json={
                    'jobid': job_id, 'status': 'completed', 'videourl': public_url,
                    'duration': real_duration, 'clipsused': len(scene_paths),
                    'row_number': row_number, 'originaldata': data
                }, timeout=10)
            except: logger.error(f"Webhook fail: {webhook_url}")
            
    except Exception as e:
        logger.error(f"[{job_id}] FAIL: {str(e)}")
        jobs[job_id]['status'] = 'failed'
        jobs[job_id]['error'] = str(e)
        
        if webhook_url:
            try:
                requests.post(webhook_url, json={'jobid': job_id, 'status': 'failed', 'error': str(e)}, timeout=10)
            except: pass
        
        # Cleanup fail
        for path in [audiopath, video_looped_path, final_video_path, *([p[0] for p in getattr(scene_paths, 'values', lambda: [])()])]:
            try: os.unlink(path)
            except: pass

@app.route("/generate", methods=["POST"])
def generate():
    data = request.get_json() or {}
    job_id = str(uuid.uuid4())
    
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
        resp.update({k: job[k] for k in ['video_url', 'duration', 'clips_used', 'row_number']})
    elif job['status'] == 'failed':
        resp['error'] = job.get('error')
    return jsonify(resp)

@app.route("/health", methods=["GET"])
def health():
    return jsonify({'status': 'ok', 'jobs': len(jobs)})

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port, debug=False)

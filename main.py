import requests
import time
from multiprocessing import Pool, cpu_count
from collections import Counter

# Configuration
BASE_URL = "http://72.60.221.150:8080"
STUDENT_ID = "MCS202509"  # Replace with your actual student ID

def login():
    """Get a fresh secret key from the server."""
    response = requests.post(
        f"{BASE_URL}/login",
        json={"student_id": STUDENT_ID}
    )
    response.raise_for_status()
    return response.json()["secret_key"]

def get_title(secret_key, filename, retries=5):
    """Fetch title for a single file with retry on 429."""
    for attempt in range(retries):
        try:
            response = requests.post(
                f"{BASE_URL}/lookup",
                json={"secret_key": secret_key, "filename": filename}
            )
            if response.status_code == 429:
                wait = 2 ** attempt
                time.sleep(wait)
                continue
            response.raise_for_status()
            return response.json().get("title", "")
        except requests.RequestException as e:
            if attempt == retries - 1:
                print(f"Failed to get {filename}: {e}")
                return ""
            time.sleep(1)
    return ""

def mapper(filename_chunk):
    """
    Map phase: fetch titles and count first words.
    NO normalization - use raw first word as-is.
    """
    secret_key = login()
    word_counts = Counter()
    
    for filename in filename_chunk:
        title = get_title(secret_key, filename)
        if title and title.strip():
            parts = title.strip().split()
            if parts:
                first_word = parts[0]  # Raw, no modification
                word_counts[first_word] += 1

    return word_counts

def reducer(counters):
    """Reduce phase: merge all Counters."""
    total = Counter()
    for counter in counters:
        total.update(counter)
    return total

def verify_top_10(student_id, top_10_list):
    """Submit top_10_list to /verify endpoint."""
    secret_key = login()
    response = requests.post(
        f"{BASE_URL}/verify",
        json={"secret_key": secret_key, "top_10": top_10_list}
    )
    response.raise_for_status()
    result = response.json()
    print("=== Verification Result ===")
    print(f"Score  : {result.get('score')} / {result.get('total')}")
    print(f"Correct: {result.get('correct')}")
    print(f"Message: {result.get('message')}")
    return result

if __name__ == "__main__":
    all_filenames = [f"pub_{i}.txt" for i in range(1000)]

    num_workers = min(cpu_count(), 8)
    chunk_size = len(all_filenames) // num_workers
    chunks = [
        all_filenames[i * chunk_size:(i + 1) * chunk_size]
        for i in range(num_workers)
    ]
    remainder = all_filenames[num_workers * chunk_size:]
    if remainder:
        chunks[-1].extend(remainder)

    print(f"Using {num_workers} workers, chunk size ~{chunk_size}")

    with Pool(processes=num_workers) as pool:
        partial_counts = pool.map(mapper, chunks)

    total_counts = reducer(partial_counts)

    top_10 = [word for word, _ in total_counts.most_common(10)]
    print(f"\nSubmitting: {top_10}")

    if top_10:
        verify_top_10(STUDENT_ID, top_10)
    else:
        print("No words found!")
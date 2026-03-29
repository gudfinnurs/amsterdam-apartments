#!/usr/bin/env python3
"""
Fetch apartment listings from Google Sheet, geocode, scrape photos.
Outputs data/listings.json for the GitHub Pages site.

Env vars:
  GOOGLE_SHEET_ID            - spreadsheet ID
  GOOGLE_SERVICE_ACCOUNT_JSON - full JSON string of service account credentials
"""
import json, os, re, sys, time, urllib.request, urllib.parse
from datetime import datetime, timezone

def fetch_sheet():
    try:
        import gspread
        from google.oauth2.service_account import Credentials
    except ImportError:
        print("Installing deps...")
        os.system("pip install gspread google-auth -q")
        import gspread
        from google.oauth2.service_account import Credentials

    sa_raw = os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON")
    sheet_id = os.environ.get("GOOGLE_SHEET_ID")

    if not sa_raw or not sheet_id:
        print("ERROR: Missing GOOGLE_SERVICE_ACCOUNT_JSON or GOOGLE_SHEET_ID", file=sys.stderr)
        sys.exit(1)

    creds_data = json.loads(sa_raw)
    creds = Credentials.from_service_account_info(creds_data, scopes=[
        "https://www.googleapis.com/auth/spreadsheets.readonly"
    ])
    client = gspread.authorize(creds)
    sheet = client.open_by_key(sheet_id).sheet1
    return sheet.get_all_records()

def geocode(address):
    """Geocode using Nominatim. Returns (lat, lng) or (None, None)."""
    try:
        q = urllib.parse.quote(f"{address}, Amsterdam, Netherlands")
        url = f"https://nominatim.openstreetmap.org/search?q={q}&format=json&limit=1&countrycodes=nl"
        req = urllib.request.Request(url, headers={"User-Agent": "ApartmentFinder/1.0 (gudfinnurs@github.com)"})
        with urllib.request.urlopen(req, timeout=10) as resp:
            data = json.loads(resp.read())
        if data:
            return float(data[0]["lat"]), float(data[0]["lon"])
    except Exception as e:
        print(f"  Geocode error: {e}")
    return None, None

def fetch_photo(url):
    """Extract the main listing photo from Pararius detail page."""
    if not url or "pararius" not in url:
        return None
    try:
        req = urllib.request.Request(url, headers={
            "User-Agent": "Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Mobile/15E148 Safari/604.1",
            "Accept": "text/html",
        })
        with urllib.request.urlopen(req, timeout=20) as resp:
            html = resp.read().decode("utf-8", errors="ignore")

        # Try og:image first (most reliable)
        og = re.search(r'<meta\s+property="og:image"\s+content="([^"]+)"', html)
        if og:
            return og.group(1)

        # Try pararius image CDN patterns
        patterns = [
            r'(https://images\.pararius\.com/[^\s"\'<>]+?\.(?:jpg|jpeg|webp))',
            r'(https://cdn\.pararius\.com/[^\s"\'<>]+?\.(?:jpg|jpeg|webp))',
        ]
        for pat in patterns:
            matches = re.findall(pat, html, re.I)
            good = [m for m in matches if not any(x in m for x in ['thumb','100x','200x','icon','logo'])]
            if good:
                return good[0]
    except Exception as e:
        print(f"  Photo error ({url[:60]}…): {e}")
    return None

def main():
    print("📊 Fetching sheet data...")
    rows = fetch_sheet()
    print(f"   Got {len(rows)} rows")

    # Load geo cache from existing file if present
    out_path = os.path.join(os.path.dirname(__file__), "..", "data", "listings.json")
    geo_cache = {}
    if os.path.exists(out_path):
        try:
            with open(out_path) as f:
                existing = json.load(f)
            if isinstance(existing, list):
                for ex in existing:
                    if ex.get("address") and ex.get("lat"):
                        geo_cache[ex["address"].lower()] = (ex["lat"], ex["lng"])
            print(f"   Loaded {len(geo_cache)} cached geocodes")
        except Exception:
            pass

    listings = []
    for i, row in enumerate(rows):
        address = str(row.get("Address", "")).strip()
        url = str(row.get("Link", "")).strip()
        available_raw = str(row.get("Available ~May 1?", "")).strip()

        listing = {
            "date_found": str(row.get("Date Found", "")).strip(),
            "address": address,
            "price": str(row.get("Price (€/mo)", "")).strip(),
            "size": str(row.get("Size (m²)", "")).strip(),
            "rooms": str(row.get("Rooms", "")).strip(),
            "furnished": str(row.get("Furnished", "")).strip(),
            "available": available_raw,
            "summary": str(row.get("Summary", "")).strip(),
            "url": url,
            "lat": None, "lng": None,
            "photo_url": None,
        }

        # Geocode (use cache if available)
        if address:
            key = address.lower()
            if key in geo_cache:
                lat, lng = geo_cache[key]
                listing["lat"] = lat
                listing["lng"] = lng
                print(f"  [{i+1}/{len(rows)}] 📍 cached: {address[:50]}")
            else:
                print(f"  [{i+1}/{len(rows)}] 🌐 geocoding: {address[:50]}…")
                lat, lng = geocode(address)
                listing["lat"] = lat
                listing["lng"] = lng
                if lat:
                    geo_cache[key] = (lat, lng)
                time.sleep(1.2)  # Nominatim rate limit

        # Fetch photo
        if url:
            print(f"         📸 fetching photo…")
            listing["photo_url"] = fetch_photo(url)
            if listing["photo_url"]:
                print(f"         ✅ {listing['photo_url'][:60]}…")
            else:
                print(f"         ❌ no photo found")
            time.sleep(0.5)

        listings.append(listing)

    # Write output
    os.makedirs(os.path.join(os.path.dirname(__file__), "..", "data"), exist_ok=True)
    output = listings  # plain list; add metadata separately
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(output, f, indent=2, ensure_ascii=False)

    # Write updated timestamp as a sidecar
    meta_path = out_path.replace("listings.json", "meta.json")
    with open(meta_path, "w") as f:
        json.dump({"updated": datetime.now(timezone.utc).isoformat(), "count": len(listings)}, f)

    print(f"\n✅ Done! Wrote {len(listings)} listings → data/listings.json")
    print(f"   Photos found: {sum(1 for l in listings if l.get('photo_url'))}/{len(listings)}")
    print(f"   Geocoded:     {sum(1 for l in listings if l.get('lat'))}/{len(listings)}")

if __name__ == "__main__":
    main()

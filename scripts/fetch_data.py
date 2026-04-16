#!/usr/bin/env python3
"""
Fetch apartment listings from Google Sheet, geocode, compute distances, scrape photos.
Outputs data/listings.json and data/meta.json.

Env vars:
  GOOGLE_SHEET_ID              - spreadsheet ID
  GOOGLE_SERVICE_ACCOUNT_JSON  - full JSON of service account credentials
"""
import json, math, os, re, sys, time, urllib.request, urllib.parse
from datetime import datetime, timezone

# ── Constants ─────────────────────────────────────────────────────────────────
JAVAPLEIN = (52.3631, 4.9450)
OUT_DIR   = os.path.join(os.path.dirname(__file__), '..', 'data')
OUT_PATH  = os.path.join(OUT_DIR, 'listings.json')
META_PATH = os.path.join(OUT_DIR, 'meta.json')

# Amsterdam bounding box for coordinate validation
AMS_BOUNDS = dict(lat_min=52.25, lat_max=52.50, lng_min=4.70, lng_max=5.10)

# ── Haversine distance ────────────────────────────────────────────────────────
def haversine(lat1, lng1, lat2, lng2):
    R = 6371
    dlat = math.radians(lat2 - lat1)
    dlng = math.radians(lng2 - lng1)
    a = math.sin(dlat/2)**2 + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlng/2)**2
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))

# ── Fetch sheet ───────────────────────────────────────────────────────────────
def fetch_sheet():
    try:
        import gspread
        from google.oauth2.service_account import Credentials
    except ImportError:
        os.system('pip install gspread google-auth -q')
        import gspread
        from google.oauth2.service_account import Credentials

    sa_raw   = os.environ.get('GOOGLE_SERVICE_ACCOUNT_JSON')
    sheet_id = os.environ.get('GOOGLE_SHEET_ID')
    if not sa_raw or not sheet_id:
        raise ValueError('Missing GOOGLE_SERVICE_ACCOUNT_JSON or GOOGLE_SHEET_ID')

    creds  = Credentials.from_service_account_info(json.loads(sa_raw),
               scopes=['https://www.googleapis.com/auth/spreadsheets.readonly'])
    client = gspread.authorize(creds)
    return client.open_by_key(sheet_id).sheet1.get_all_records()

# ── Geocode (Nominatim) ───────────────────────────────────────────────────────
def geocode(address):
    """Return (lat, lng) or (None, None). Strips neighborhood suffixes first."""
    clean = re.sub(r'\s*\([^)]+\)', '', address).strip()
    try:
        q   = urllib.parse.quote(f'{clean}, Amsterdam, Netherlands')
        url = f'https://nominatim.openstreetmap.org/search?q={q}&format=json&limit=1&countrycodes=nl'
        req = urllib.request.Request(url, headers={'User-Agent': 'ApartmentFinder/1.0 (gudfinnurs@github.com)'})
        with urllib.request.urlopen(req, timeout=10) as r:
            data = json.loads(r.read())
        if data:
            lat, lng = float(data[0]['lat']), float(data[0]['lon'])
            # Validate Amsterdam bounds
            if (AMS_BOUNDS['lat_min'] <= lat <= AMS_BOUNDS['lat_max'] and
                AMS_BOUNDS['lng_min'] <= lng <= AMS_BOUNDS['lng_max']):
                return lat, lng
            else:
                print(f'    ⚠️  Geocode out of Amsterdam bounds: {lat},{lng}')
    except Exception as e:
        print(f'    Geocode error: {e}')
    return None, None

# ── Photo fetch ───────────────────────────────────────────────────────────────
def fetch_photo(url):
    """Try to get og:image from a Pararius listing page."""
    if not url or 'pararius' not in url:
        return None
    try:
        req = urllib.request.Request(url, headers={
            'User-Agent': 'Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) AppleWebKit/605.1.15',
            'Accept': 'text/html,application/xhtml+xml',
            'Accept-Language': 'nl-NL,nl;q=0.9,en;q=0.8',
        })
        with urllib.request.urlopen(req, timeout=20) as r:
            html = r.read().decode('utf-8', errors='ignore')

        # og:image first (most reliable)
        og = re.search(r'<meta\s+property=["\']og:image["\']\s+content=["\']([^"\']+)["\']', html)
        if og: return og.group(1)

        og2 = re.search(r'<meta\s+content=["\']([^"\']+)["\']\s+property=["\']og:image["\']', html)
        if og2: return og2.group(1)

        # Pararius CDN fallback
        for pat in [r'(https://images\.pararius\.com/[^\s"\'<>]+?\.(?:jpg|jpeg|webp))',
                    r'(https://cdn\.pararius\.com/[^\s"\'<>]+?\.(?:jpg|jpeg|webp))']:
            m = re.findall(pat, html, re.I)
            good = [x for x in m if not any(t in x for t in ['thumb','100x','200x','icon'])]
            if good: return good[0]
    except Exception as e:
        print(f'    Photo error: {e}')
    return None

# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    print('📊 Fetching sheet data…')
    rows = fetch_sheet()
    print(f'   {len(rows)} rows')

    # Load existing listings for geocode cache
    geo_cache = {}
    photo_cache = {}
    if os.path.exists(OUT_PATH):
        try:
            existing = json.load(open(OUT_PATH))
            for ex in (existing if isinstance(existing, list) else []):
                key = (ex.get('address') or '').lower()
                if key and ex.get('lat'):
                    geo_cache[key] = (ex['lat'], ex['lng'])
                if key and ex.get('photo_url'):
                    photo_cache[key] = ex['photo_url']
            print(f'   Geo cache: {len(geo_cache)} | Photo cache: {len(photo_cache)}')
        except Exception as e:
            print(f'   Cache load error: {e}')

    listings = []
    errors   = []

    for i, row in enumerate(rows):
        address   = str(row.get('Address', '')).strip()
        url       = str(row.get('Link', '')).strip()
        available = str(row.get('Available ~May 1?', '')).strip()
        furnished = str(row.get('Furnished', '')).strip()
        price_raw = str(row.get('Price (€/mo)', '')).strip()
        size_raw  = str(row.get('Size (m²)', '')).strip()

        listing = {
            'date_found':  str(row.get('Date Found', '')).strip(),
            'address':     address,
            'price':       price_raw,
            'size':        size_raw,
            'rooms':       str(row.get('Rooms', '')).strip(),
            'furnished':   furnished,
            'available':   available,
            'summary':     str(row.get('Summary', '')).strip(),
            'url':         url,
            # Enriched fields
            'lat':         None,
            'lng':         None,
            'distance_km': None,
            'photo_url':   row.get('Photo URL') or None,  # From sheet if cron stored it
        }

        # ── Validate basic criteria ──
        price_int = int(re.sub(r'\D', '', price_raw)) if re.search(r'\d', price_raw) else 0
        if price_int > 0 and not (1000 <= price_int <= 5000):
            errors.append(f'Row {i+1}: price {price_int} outside 1000–5000')

        furnished_lower = furnished.lower()
        if any(w in furnished_lower for w in ['gemeubileerd','gestoffeerd','furnished']) and \
           not any(w in furnished_lower for w in ['unfurnished','kaal','ongestoffeerd']):
            errors.append(f'Row {i+1}: possibly furnished listing slipped through: {address[:60]}')

        # ── Geocode ──
        key = address.lower()
        if key in geo_cache:
            lat, lng = geo_cache[key]
            listing['lat'], listing['lng'] = lat, lng
        elif address:
            print(f'  [{i+1}/{len(rows)}] 🌐 geocoding: {address[:55]}…')
            lat, lng = geocode(address)
            listing['lat'], listing['lng'] = lat, lng
            if lat: geo_cache[key] = (lat, lng)
            time.sleep(1.2)

        # ── Distance from Javaplein ──
        if listing['lat'] and listing['lng']:
            listing['distance_km'] = round(haversine(JAVAPLEIN[0], JAVAPLEIN[1], listing['lat'], listing['lng']), 2)

        # ── Photo ──
        # Only fetch if not already in sheet or cache; Pararius often 403s
        if not listing['photo_url']:
            if key in photo_cache:
                listing['photo_url'] = photo_cache[key]
            elif url:
                print(f'  [{i+1}/{len(rows)}] 📸 fetching photo…')
                listing['photo_url'] = fetch_photo(url)
                if listing['photo_url']:
                    photo_cache[key] = listing['photo_url']
                    print(f'       ✅ {listing["photo_url"][:60]}')
                else:
                    print(f'       ❌ no photo')
                time.sleep(0.5)

        listings.append(listing)

    # ── Write output ──────────────────────────────────────────────────────────
    os.makedirs(OUT_DIR, exist_ok=True)
    with open(OUT_PATH, 'w', encoding='utf-8') as f:
        json.dump(listings, f, indent=2, ensure_ascii=False)

    updated_ts = datetime.now(timezone.utc).isoformat()
    meta = {
        'updated': updated_ts,
        'count':   len(listings),
        'geocoded': sum(1 for l in listings if l.get('lat')),
        'photos':   sum(1 for l in listings if l.get('photo_url')),
        'errors':   errors,
    }
    with open(META_PATH, 'w') as f:
        json.dump(meta, f, indent=2)

    # ── Summary ───────────────────────────────────────────────────────────────
    print(f'\n✅ Done — {len(listings)} listings')
    print(f'   Geocoded:     {meta["geocoded"]}/{len(listings)}')
    print(f'   Photos:       {meta["photos"]}/{len(listings)}')
    print(f'   Errors:       {len(errors)}')
    if errors:
        print('\n⚠️  Validation errors:')
        for e in errors: print(f'   • {e}')

if __name__ == '__main__':
    main()

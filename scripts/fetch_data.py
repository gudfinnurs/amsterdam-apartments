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
def _extract_og(html):
    """Legacy single-photo extraction — returns first photo URL."""
    d = _extract_listing_details(html)
    return d['photo_urls'][0] if d['photo_urls'] else None

def _extract_listing_details(html):
    """Extract photos array, available_from, min_contract from Pararius HTML."""
    seen, photos = set(), []
    def add(u):
        if u and u.startswith('http') and u not in seen:
            seen.add(u); photos.append(u)

    # og:image (canonical main photo)
    for pat in [r'property=["\'']og:image["\''][^>]+content=["\'']([^"\']+)["\'']',
                r'content=["\'']([^"\']+)["\''][^>]+property=["\'']og:image["\'']']:
        m = re.search(pat, html)
        if m: add(m.group(1)); break

    # JSON-LD image arrays (often the full gallery)
    for block in re.findall(r'<script[^>]+type=["\'']application/ld\+json["\''][^>]*>([\s\S]*?)</script>', html, re.I):
        try:
            import json as _json
            obj = _json.loads(block)
            imgs = obj.get('image', [])
            if isinstance(imgs, str): imgs = [imgs]
            for u in imgs:
                if isinstance(u, str): add(u)
        except Exception:
            pass

    # Pararius CDN direct
    for u in re.findall(r'https://images\.pararius\.com/[^\s"\'<>]+?\.(?:jpg|jpeg|webp)', html, re.I):
        if not any(t in u for t in ['thumb','100x','200x','icon','logo']): add(u)

    # Available from — Dutch/English patterns
    available_from = None
    avail_pats = [
        r'(?:Beschikbaar\s*per|Aanvaarding)\s*[:\s]*([^<\n]{3,35})',
        r'(?:Available\s+from)\s*[:\s]*([^<\n]{3,35})',
        r'"availableFrom"\s*:\s*"([^"]{3,35})"',
        r'(Per\s+direct)',
        r'(In\s+overleg)',
    ]
    for pat in avail_pats:
        m = re.search(pat, html, re.I)
        if m:
            txt = re.sub(r'<[^>]+>', '', m.group(1) if m.lastindex else m.group(0)).strip()
            if 2 < len(txt) < 40:
                available_from = txt; break

    # Min contract — Dutch patterns
    min_contract = None
    contract_pats = [
        r'(?:Minimum\s*huur(?:periode|duur)|Minimumduur\s*huurovereenkomst|Contractduur)\s*[:\s]*([^<\n]{2,30})',
        r'"minimumRentalPeriod"\s*:\s*"?([^",}\n]{2,20})"?',
        r'(\d+\s*(?:maanden|jaar|months|years))',
    ]
    for pat in contract_pats:
        m = re.search(pat, html, re.I)
        if m:
            txt = re.sub(r'<[^>]+>', '', m.group(1)).strip()
            if 1 < len(txt) < 30:
                min_contract = txt; break

    return {'photo_urls': photos[:10], 'available_from': available_from, 'min_contract': min_contract}


def fetch_listing_details(url):
    """Fetch photos, available_from, min_contract for a Pararius listing. Returns dict or None."""
    if not url or 'pararius' not in url:
        return None
    html = None
    try:
        import cloudscraper  # type: ignore
        scraper = cloudscraper.create_scraper(browser={'browser':'chrome','platform':'darwin','mobile':False})
        r = scraper.get(url, timeout=25, headers={'Accept-Language':'nl-NL,nl;q=0.9'})
        if r.status_code == 200: html = r.text
    except ImportError: pass
    except Exception as e: print(f'    cloudscraper: {e}')
    if not html:
        try:
            req = urllib.request.Request(url, headers={
                'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 Chrome/124.0.0.0 Safari/537.36',
                'Accept': 'text/html,application/xhtml+xml,*/*;q=0.8',
                'Accept-Language': 'nl-NL,nl;q=0.9',
            })
            with urllib.request.urlopen(req, timeout=20) as r:
                html = r.read().decode('utf-8', errors='ignore')
        except Exception as e: print(f'    urllib: {e}')
    if not html: return None
    return _extract_listing_details(html)

def fetch_photo(url):
    """Fetch the main photo for a Pararius listing. Tries cloudscraper first, then urllib."""
    if not url or 'pararius' not in url:
        return None

    # Strategy 1 - cloudscraper (handles Cloudflare JS challenges)
    try:
        import cloudscraper  # type: ignore
        scraper = cloudscraper.create_scraper(
            browser={'browser': 'chrome', 'platform': 'darwin', 'mobile': False}
        )
        r = scraper.get(url, timeout=25, headers={'Accept-Language': 'nl-NL,nl;q=0.9'})
        if r.status_code == 200:
            result = _extract_og(r.text)
            if result:
                return result
    except ImportError:
        pass  # cloudscraper not installed, fall through
    except Exception as e:
        print(f'    cloudscraper: {e}')

    # Strategy 2 - urllib with realistic Chrome desktop headers
    try:
        req = urllib.request.Request(url, headers={
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'nl-NL,nl;q=0.9,en-US;q=0.8,en;q=0.7',
            'DNT': '1',
            'Upgrade-Insecure-Requests': '1',
        })
        with urllib.request.urlopen(req, timeout=20) as r:
            html = r.read().decode('utf-8', errors='ignore')
        result = _extract_og(html)
        if result:
            return result
    except Exception as e:
        print(f'    urllib: {e}')

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
            'photo_url':   row.get('Photo URL') or None,
            'photo_urls':  None,
            'available_from': row.get('Available From') or None,
            'min_contract':   row.get('Min Contract') or None,
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

        # ── Photos + rental details (available_from, min_contract) ──
        needs_fetch = url and (not listing['photo_url'] or not listing['available_from'] or not listing['min_contract'])
        if needs_fetch:
            if key in photo_cache and listing['photo_url']:
                pass  # already have photo from cache/sheet
            elif url:
                print(f'  [{i+1}/{len(rows)}] 📸 fetching details & photos…')
                details = fetch_listing_details(url)
                if details:
                    if not listing['photo_url'] and details['photo_urls']:
                        listing['photo_url']  = details['photo_urls'][0]
                        listing['photo_urls'] = details['photo_urls']
                        photo_cache[key] = details['photo_urls'][0]
                        print(f'       ✅ {len(details["photo_urls"])} photos')
                    if not listing['available_from'] and details['available_from']:
                        listing['available_from'] = details['available_from']
                    if not listing['min_contract'] and details['min_contract']:
                        listing['min_contract'] = details['min_contract']
                else:
                    print(f'       ❌ no details fetched')
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

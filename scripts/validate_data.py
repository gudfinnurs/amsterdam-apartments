#!/usr/bin/env python3
"""
Validate listings.json before it gets committed to the repo.
Exit 0 = all checks passed (or only warnings).
Exit 1 = critical failures that should block the commit.

Usage: python scripts/validate_data.py [--strict]
"""
import json, math, os, re, sys
from datetime import datetime, timezone

STRICT    = '--strict' in sys.argv
DATA_PATH = os.path.join(os.path.dirname(__file__), '..', 'data', 'listings.json')
META_PATH = os.path.join(os.path.dirname(__file__), '..', 'data', 'meta.json')

# Amsterdam bounding box
AMS = dict(lat_min=52.25, lat_max=52.50, lng_min=4.70, lng_max=5.10)
JAVAPLEIN = (52.3631, 4.9450)

WARN = []; FAIL = []

def warn(msg): WARN.append(msg); print(f'  ⚠️  WARN  {msg}')
def fail(msg): FAIL.append(msg); print(f'  ❌ FAIL  {msg}')
def ok(msg):   print(f'  ✅ OK    {msg}')

def haversine(lat1, lng1, lat2, lng2):
    R = 6371
    dlat = math.radians(lat2 - lat1)
    dlng = math.radians(lng2 - lng1)
    a = math.sin(dlat/2)**2 + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlng/2)**2
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))

def main():
    print('\n🧪 Amsterdam Apartments — Data Validation\n' + '─'*50)

    # ── 1. File existence ─────────────────────────────────────────────────────
    print('\n[1] File checks')
    if not os.path.exists(DATA_PATH):
        fail('listings.json not found')
        sys.exit(1)
    ok('listings.json exists')

    if os.path.exists(META_PATH):
        ok('meta.json exists')
    else:
        warn('meta.json missing — run fetch_data.py')

    # ── 2. Parse JSON ──────────────────────────────────────────────────────────
    print('\n[2] JSON parsing')
    try:
        with open(DATA_PATH) as f:
            listings = json.load(f)
        ok(f'Valid JSON — {len(listings)} listings')
    except Exception as e:
        fail(f'JSON parse error: {e}')
        sys.exit(1)

    if len(listings) == 0:
        fail('Zero listings — something is wrong')
        sys.exit(1)

    if len(listings) < 5:
        warn(f'Only {len(listings)} listings — suspiciously low')
    else:
        ok(f'Listing count {len(listings)} ≥ 5')

    # ── 3. Freshness ──────────────────────────────────────────────────────────
    print('\n[3] Data freshness')
    if os.path.exists(META_PATH):
        try:
            meta = json.load(open(META_PATH))
            updated = datetime.fromisoformat(meta['updated'].replace('Z', '+00:00'))
            age_h = (datetime.now(timezone.utc) - updated).total_seconds() / 3600
            if age_h > 26:
                warn(f'Data is {age_h:.1f}h old (> 26h) — daily update may have failed')
            elif age_h > 6:
                ok(f'Data is {age_h:.1f}h old (within daily window)')
            else:
                ok(f'Data is fresh ({age_h:.1f}h old)')
        except Exception as e:
            warn(f'Could not check freshness: {e}')
    else:
        warn('No meta.json — skipping freshness check')

    # ── 4. Required fields ────────────────────────────────────────────────────
    print('\n[4] Schema validation')
    required = ['date_found', 'address', 'price', 'url']
    missing_fields = []
    for i, l in enumerate(listings):
        for field in required:
            if not l.get(field):
                missing_fields.append(f'Row {i+1} missing "{field}"')
    if missing_fields:
        for m in missing_fields[:5]: warn(m)
        if len(missing_fields) > 5: warn(f'…and {len(missing_fields)-5} more')
    else:
        ok('All required fields present')

    # ── 5. Price sanity ───────────────────────────────────────────────────────
    print('\n[5] Price range check (€1,000–€5,000)')
    bad_price = []
    for l in listings:
        p_str = str(l.get('price', '') or '').strip()
        p = int(re.sub(r'\D', '', p_str)) if re.search(r'\d', p_str) else 0
        if p > 0 and not (1000 <= p <= 5000):
            bad_price.append(f'{l.get("address","?")} — €{p}')
    if bad_price:
        for b in bad_price: fail(f'Price out of range: {b}')
    else:
        ok('All prices within €1,000–€5,000')

    # ── 6. Furnished check ────────────────────────────────────────────────────
    print('\n[6] Furnishing check (no furnished listings allowed)')
    furnished_kw = ['gemeubileerd', 'gestoffeerd', 'furnished', 'semi-furnished']
    unfurnished_kw = ['unfurnished', 'kaal', 'ongestoffeerd']
    suspicious = []
    for l in listings:
        f_str = (l.get('furnished') or '').lower()
        is_furnished = any(k in f_str for k in furnished_kw)
        is_unfurnished = any(k in f_str for k in unfurnished_kw) or not f_str
        if is_furnished and not is_unfurnished:
            suspicious.append(l.get('address', '?'))
        # Also check summary for furnishing mentions
        summary = (l.get('summary') or '').lower()
        if any(k in summary for k in furnished_kw) and not any(k in summary for k in unfurnished_kw):
            if l.get('address') not in suspicious:
                suspicious.append(f'{l.get("address","?")} (summary flag)')
    if suspicious:
        for s in suspicious: fail(f'Possibly furnished: {s[:80]}')
    else:
        ok('No furnished listings detected')

    # ── 7. Coordinate validation ──────────────────────────────────────────────
    print('\n[7] Coordinate validation (Amsterdam bounding box)')
    geocoded = [l for l in listings if l.get('lat') and l.get('lng')]
    ok(f'{len(geocoded)}/{len(listings)} listings geocoded')

    bad_coords = []
    far_listings = []
    for l in geocoded:
        lat, lng = l['lat'], l['lng']
        if not (AMS['lat_min'] <= lat <= AMS['lat_max'] and AMS['lng_min'] <= lng <= AMS['lng_max']):
            bad_coords.append(f'{l.get("address","?")} → {lat:.4f},{lng:.4f}')
        else:
            dist = haversine(JAVAPLEIN[0], JAVAPLEIN[1], lat, lng)
            if dist > 15:
                far_listings.append(f'{l.get("address","?")} → {dist:.1f}km from Javaplein')

    if bad_coords:
        for b in bad_coords: fail(f'Coords outside Amsterdam: {b}')
    else:
        ok('All coordinates within Amsterdam bounding box')

    if far_listings:
        for b in far_listings: warn(f'Listing very far from Javaplein (geocoding error?): {b}')
    else:
        ok('All geocoded listings within 15km of Javaplein')

    # ── 8. URL check ──────────────────────────────────────────────────────────
    print('\n[8] URL check')
    bad_urls = [l.get('address','?') for l in listings if not str(l.get('url') or '').startswith('http')]
    if bad_urls:
        warn(f'{len(bad_urls)} listings with missing/invalid URLs')
    else:
        ok('All listings have valid URLs')

    # ── 9. Date sanity ────────────────────────────────────────────────────────
    print('\n[9] Date sanity')
    future_dates = []
    for l in listings:
        d_str = l.get('date_found', '')
        if d_str:
            try:
                d = datetime.fromisoformat(d_str)
                if d.year > 2030 or d.year < 2024:
                    future_dates.append(f'{l.get("address","?")} — {d_str}')
            except Exception:
                pass
    if future_dates:
        for f in future_dates: warn(f'Suspicious date: {f}')
    else:
        ok('All dates look reasonable')

    # ── Summary ───────────────────────────────────────────────────────────────
    print('\n' + '─'*50)
    print(f'Results: {len(FAIL)} failures, {len(WARN)} warnings\n')

    if FAIL:
        print(f'❌ VALIDATION FAILED — {len(FAIL)} critical issue(s)')
        if STRICT:
            sys.exit(1)
        else:
            print('   (non-strict mode — continuing despite failures)')
            sys.exit(0)
    elif WARN:
        print(f'⚠️  Passed with {len(WARN)} warning(s)')
        sys.exit(0)
    else:
        print('✅ All checks passed!')
        sys.exit(0)

if __name__ == '__main__':
    main()

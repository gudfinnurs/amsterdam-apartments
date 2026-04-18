#!/usr/bin/env python3
"""
Daily listing freshness checker.
Fetches each listing URL and marks it as Archived/Rented if no longer available.

Env vars: GOOGLE_SHEET_ID, GOOGLE_SERVICE_ACCOUNT_JSON
"""
import json, os, re, sys, time, urllib.request, urllib.error
from datetime import datetime, timezone

# Signals that a listing is no longer available
DEAD_SIGNALS = [
    'gearchiveerd', 'archived',
    'verhuurd', 'rented out', 'rented',
    'niet meer beschikbaar', 'no longer available',
    'this listing is no longer', 'woning is verhuurd',
]

def get_sheet_client():
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
        print('ERROR: Missing env vars', file=sys.stderr)
        sys.exit(1)

    creds  = Credentials.from_service_account_info(json.loads(sa_raw),
               scopes=['https://www.googleapis.com/auth/spreadsheets'])
    client = gspread.authorize(creds)
    return client.open_by_key(sheet_id).sheet1

def check_url(url):
    """
    Returns: 'active', 'archived', 'rented', 'error'
    Tries: Playwright (full render) -> cloudscraper -> urllib
    """
    if not url or 'pararius' not in url:
        return 'active'

    html = None
    final_url = url

    # Strategy 1: Playwright (best — renders JS, follows redirects)
    try:
        from playwright.sync_api import sync_playwright
        with sync_playwright() as pw:
            browser = pw.chromium.launch(headless=True)
            page = browser.new_context(
                user_agent='Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 Chrome/124.0.0.0 Safari/537.36',
                locale='nl-NL'
            ).new_page()
            resp = page.goto(url, wait_until='domcontentloaded', timeout=25000)
            if resp and resp.status == 404:
                browser.close()
                return 'archived'
            page.wait_for_timeout(1500)
            html = page.content().lower()
            final_url = page.url
            browser.close()
    except ImportError:
        pass
    except Exception as e:
        print(f'    playwright: {e}')

    # Strategy 2: cloudscraper
    if not html:
        try:
            import cloudscraper
            scraper = cloudscraper.create_scraper(
                browser={'browser': 'chrome', 'platform': 'darwin', 'mobile': False}
            )
            r = scraper.get(url, timeout=20)
            if r.status_code == 404: return 'archived'
            if r.status_code == 200: html = r.text.lower()
        except ImportError: pass
        except Exception as e: print(f'    cloudscraper: {e}')

    # Strategy 3: urllib
    if not html:
        try:
            req = urllib.request.Request(url, headers={
                'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 Chrome/124.0.0.0 Safari/537.36',
                'Accept-Language': 'nl-NL,nl;q=0.9',
            })
            with urllib.request.urlopen(req, timeout=15) as resp:
                html = resp.read().decode('utf-8', errors='ignore').lower()
        except urllib.error.HTTPError as e:
            if e.code == 404: return 'archived'
            return 'error'
        except Exception: return 'error'

    if not html:
        return 'error'

    # Check final URL — redirected away from listing = archived
    if final_url and final_url != url:
        if '/zoeken/' in final_url or '/huurwoningen/' in final_url or final_url.endswith('/huur'):
            return 'archived'

    # Check page content for dead signals
    for signal in DEAD_SIGNALS:
        if signal in html:
            return 'rented' if 'verhuurd' in signal or 'rented' in signal else 'archived'

    return 'active'

def main():
    print('🔍 Checking listing statuses...')
    ws = get_sheet_client()

    all_rows = ws.get_all_values()
    if not all_rows:
        print('Sheet is empty')
        return

    headers = all_rows[0]

    # Find column indices (create if missing)
    def col_idx(name):
        for i, h in enumerate(headers):
            if h.strip() == name:
                return i
        return None

    link_col     = col_idx('Link')
    status_col   = col_idx('Status')
    checked_col  = col_idx('Last Checked')
    address_col  = col_idx('Address')

    if link_col is None:
        print('ERROR: No "Link" column found')
        sys.exit(1)

    # Add Status and Last Checked columns if missing
    updates = []
    col_count = len(headers)

    if status_col is None:
        status_col = col_count
        col_count += 1
        ws.update_cell(1, status_col + 1, 'Status')
        headers.append('Status')
        print('   Added "Status" column')

    if checked_col is None:
        checked_col = col_count
        col_count += 1
        ws.update_cell(1, checked_col + 1, 'Last Checked')
        headers.append('Last Checked')
        print('   Added "Last Checked" column')
        time.sleep(0.5)

    now_str = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')

    archived_count = 0
    active_count   = 0
    error_count    = 0

    for row_idx, row in enumerate(all_rows[1:], start=2):
        if not any(c.strip() for c in row):
            continue

        url     = row[link_col].strip() if link_col < len(row) else ''
        address = row[address_col].strip() if address_col is not None and address_col < len(row) else url[:50]
        current_status = row[status_col].strip() if status_col < len(row) else ''

        # Skip already-archived rows
        if current_status.lower() in ('archived', 'rented'):
            print(f'  Row {row_idx}: ⏭  {address[:50]} (already {current_status})')
            continue

        if not url:
            continue

        print(f'  Row {row_idx}: checking {address[:50]}...', end=' ', flush=True)
        status = check_url(url)
        print(status)

        # Update sheet
        ws.update_cell(row_idx, status_col + 1, status.capitalize())
        ws.update_cell(row_idx, checked_col + 1, now_str)

        if status in ('archived', 'rented'):
            archived_count += 1
        elif status == 'active':
            active_count += 1
        else:
            error_count += 1

        time.sleep(1.2)  # rate limit

    print(f'\n✅ Done — Active: {active_count}, Archived/Rented: {archived_count}, Errors: {error_count}')

if __name__ == '__main__':
    main()

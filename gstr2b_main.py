"""
GSTR-2B Downloader + Consolidator
- Final automated version controlled by an external 'input.json' file.
- The original script structure is preserved with only minimal, necessary changes.
"""

# ===============================================================
# CHANGE 1 of 3: ADD THE REQUIRED IMPORTS AT THE TOP
# ===============================================================
import os
# (Your existing imports, including json and sys, remain below)
# ===============================================================

import time
import logging
import re
from pathlib import Path
from datetime import date
import pandas as pd  # consolidation
import json
import sys
from flask import Flask, request, jsonify
import threading
from flask_cors import CORS
import zipfile
import uuid
import shutil
import os


def zip_folder(source_folder: Path):
    zip_name = f"{source_folder.name}_{uuid.uuid4().hex}.zip"
    zip_path = source_folder.parent / zip_name

    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zipf:
        for root, dirs, files in os.walk(source_folder):
            for file in files:
                full_path = os.path.join(root, file)
                arcname = os.path.relpath(full_path, source_folder)
                zipf.write(full_path, arcname)

    return zip_path

JOB_STATUS = {}
JOB_DRIVERS = {}
JOB_CAPTCHA_READY = {}
MONTH_PENDING = "PENDING"
MONTH_RUNNING = "RUNNING"
MONTH_COMPLETED = "COMPLETED"
MONTH_FAILED = "FAILED"
MONTH_RETRYING = "RETRYING"
MONTH_FAILED_AGAIN = "FAILED_AGAIN"

# This is your original function, now removed as it is included in the main script body.
# def get_input_from_config_file():
#     ...



from selenium import webdriver
from selenium.webdriver.common.by import By
# ===============================================================
# CHANGE 2 of 3: ADD webdriver-manager FOR DRIVER STABILITY
# ===============================================================
from selenium.webdriver.chrome.service import Service as ChromeService
from webdriver_manager.chrome import ChromeDriverManager
# (Your existing imports remain below)
# ===============================================================
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.support.ui import WebDriverWait, Select
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import StaleElementReferenceException, NoSuchElementException, TimeoutException


def capture_captcha_image(driver, job_id, fy_folder: Path):
    try:
        captcha_el = WebDriverWait(driver, 30).until(
            EC.visibility_of_element_located((
                By.XPATH,
                "/html/body/div[2]/div[2]/div/div[2]/div/div/div/div/div/form/div[5]/div/div/div/table/tbody/tr[1]/th[1]/img"
            ))
        )

        path = fy_folder / "captcha.png"
        captcha_el.screenshot(str(path))

        JOB_CAPTCHA_READY[job_id] = True
        logger.info("CAPTCHA image captured at %s", path)

        return path

    except Exception as e:
        logger.error(f"CAPTCHA capture failed: {e}")
        return None



logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("gstr2b_full_integrated")


PORTAL_URL = "https://www.gst.gov.in"


MONTHS_APR_TO_MAR = ["April","May","June","July","August","September","October","November","December","January","February","March"]


def record_bug(job_id, message):
    """
    Lightweight bug-report helper.
    Stores a human-readable log of what went wrong in JOB_STATUS[job_id]['bug_log']
    without changing the main behaviour.
    """
    if not job_id:
        return
    job = JOB_STATUS.setdefault(job_id, {})
    log = job.setdefault("bug_log", [])
    log.append(str(message))
    logger.info("BUG[%s] %s", job_id, message)


def current_fy_for(dt: date) -> str:
    y = dt.year
    return f"{y}-{str((y+1)%100).zfill(2)}" if dt.month >= 4 else f"{y-1}-{str(y%100).zfill(2)}"


def fy_list_from_2017_to_today(today: date) -> list:
    start = 2017
    curr = current_fy_for(today)
    curr_start = int(curr.split('-')[0])
    return [f"{s}-{str((s+1)%100).zfill(2)}" for s in range(start, curr_start+1)]


def months_allowed_for_fy(selected_fy: str, today: date) -> list:
    fy_start_year = int(selected_fy.split('-')[0])
    this_fy = current_fy_for(today)
    this_fy_start = int(this_fy.split('-')[0])
    if fy_start_year < this_fy_start:
        return MONTHS_APR_TO_MAR[:]
    if fy_start_year > this_fy_start:
        return []
    m = today.month
    idx = m - 4 if m >= 4 else 8 + m
    return MONTHS_APR_TO_MAR[:idx+1]




# ---------- Selenium ----------
def setup_chrome(download_dir: Path):
    download_dir.mkdir(parents=True, exist_ok=True)
    opts = Options()
    prefs = {
        "download.default_directory": str(download_dir),
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "safebrowsing.enabled": True,
    }
    opts.add_experimental_option("prefs", prefs)
    opts.add_argument("--start-maximized")
    opts.add_argument("--disable-background-networking")
    opts.add_argument("--disable-features=Translate,NetworkService,OptimizationHints")
    opts.add_argument("--disable-sync")
    opts.add_argument("--metrics-recording-only")
    opts.add_argument("--no-first-run")
    opts.add_argument("--disable-notifications")
    
    # This line replaces your original webdriver.Chrome() call for stability
    driver = webdriver.Chrome(service=ChromeService(ChromeDriverManager().install()), options=opts)
    
    try:
        driver.execute_cdp_cmd("Security.setIgnoreCertificateErrors", {"ignore": True})
    except Exception:
        pass
    return driver


def wait_for_downloads_complete(folder: Path, timeout: int = 240):
    end = time.time() + timeout
    while time.time() < end:
        cr = list(folder.glob("*.crdownload"))
        pt = list(folder.glob("*.part"))
        any_files = list(folder.glob("*"))
        if not cr and not pt and any(f.is_file() for f in any_files):
            return True
        time.sleep(0.5)
    return False


def click_header_login(driver):
    driver.get(PORTAL_URL)
    for loc in [(By.LINK_TEXT, "LOGIN"),(By.PARTIAL_LINK_TEXT, "Login"),(By.XPATH, "//a[normalize-space()='LOGIN' or normalize-space()='Login']"),(By.CSS_SELECTOR, "a[href*='login']"),(By.XPATH, "//a[contains(@class,'btn') and (contains(.,'LOGIN') or contains(.,'Login'))]"),]:
        try:
            WebDriverWait(driver, 15).until(EC.element_to_be_clickable(loc)).click()
            return True
        except Exception:
            continue
    return False


def type_creds(driver, user, pwd):
    def first(locators, clickable=False, t=6):
        for how, what in locators:
            try:
                if clickable:
                    return WebDriverWait(driver, t).until(EC.element_to_be_clickable((how, what)))
                else:
                    return WebDriverWait(driver, t).until(EC.presence_of_element_located((how, what)))
            except Exception:
                continue
        return None
    u = first([(By.ID, "username"), (By.NAME, "username"),(By.ID, "userid"), (By.NAME, "userid"),(By.CSS_SELECTOR, "input[aria-label*='User' i]"),(By.CSS_SELECTOR, "input[placeholder*='User' i]"),(By.XPATH, "//label[contains(.,'User') or contains(.,'GSTIN')]/following::input[1]"),(By.XPATH, "//input[@type='text' and (contains(@placeholder,'GSTIN') or contains(@placeholder,'User'))]"),])
    p = first([(By.ID, "user_pass"), (By.NAME, "password"),(By.CSS_SELECTOR, "input[type='password']"),(By.CSS_SELECTOR, "input[aria-label*='Password' i]"),(By.CSS_SELECTOR, "input[placeholder*='Password' i]"),(By.XPATH, "//label[contains(.,'Password')]/following::input[@type='password'][1]"),])
    if u:
        try: driver.execute_script("arguments[0].scrollIntoView({block:'center'});", u)
        except Exception: pass
        try: u.clear()
        except Exception: pass
        try: u.send_keys(user)
        except Exception:
            try: u.click(); u.send_keys(user)
            except Exception: pass
    if p:
        try: driver.execute_script("arguments[0].scrollIntoView({block:'center'});", p)
        except Exception: pass
        try: p.clear()
        except Exception: pass
        try: p.send_keys(pwd)
        except Exception:
            try: p.click(); p.send_keys(pwd)
            except Exception: pass
        # ⛔ DO NOT AUTO-CLICK LOGIN
        # Login will be triggered ONLY after captcha is submitted by user
        return



def wait_until_logged_in(driver, timeout=180):
    WebDriverWait(driver, timeout).until(EC.any_of(EC.presence_of_element_located((By.XPATH, "//a[normalize-space()='Services']")),EC.presence_of_element_located((By.XPATH, "//a[normalize-space()='Returns']")),EC.url_contains("/dashboard"),EC.url_contains("/returns")))


def hover_returns_and_click_dashboard(driver):
    try:
        WebDriverWait(driver, 10).until(EC.invisibility_of_element_located((By.CSS_SELECTOR, ".dimmer-holder, .modal-backdrop, .blockUI")))
    except Exception:
        pass
    actions = ActionChains(driver)
    services = WebDriverWait(driver, 20).until(EC.element_to_be_clickable((By.XPATH, "//a[normalize-space()='Services' or @title='Services']")))
    try: services.click()
    except Exception: driver.execute_script("arguments[0].click();", services)
    time.sleep(0.2)
    returns_tab = WebDriverWait(driver, 20).until(EC.visibility_of_element_located((By.XPATH, "//a[normalize-space()='Returns' and ancestor::*[contains(@class,'menu') or contains(@class,'navbar')]]")))
    try:
        actions.move_to_element(returns_tab).pause(0.2).perform()
        actions.move_by_offset(2, 1).pause(0.1).move_by_offset(-2, -1).pause(0.1).perform()
    except Exception:
        pass
    for xp in ["//a[normalize-space()='Returns Dashboard']","//a[contains(@href,'returns/dashboard') and contains(.,'Dashboard')]",]:
        try:
            rd = WebDriverWait(driver, 5).until(EC.element_to_be_clickable((By.XPATH, xp)))
            try: rd.click()
            except Exception: driver.execute_script("arguments[0].click();", rd)
            break
        except Exception:
            continue
    WebDriverWait(driver, 20).until(EC.presence_of_element_located((By.XPATH, "//label[contains(.,'Financial Year')]")))


def re_anchor_to_returns_form(driver):
    try:
        WebDriverWait(driver, 3).until(EC.presence_of_element_located((By.XPATH, "//label[contains(.,'Financial Year')]")))
        return
    except Exception:
        pass
    actions = ActionChains(driver)
    services = WebDriverWait(driver, 20).until(EC.element_to_be_clickable((By.XPATH, "//a[normalize-space()='Services' or @title='Services']")))
    try: services.click()
    except Exception: driver.execute_script("arguments[0].click();", services)
    time.sleep(0.2)
    returns_tab = WebDriverWait(driver, 20).until(EC.visibility_of_element_located((By.XPATH, "//a[normalize-space()='Returns' and ancestor::*[contains(@class,'menu') or contains(@class,'navbar')]]")))
    try:
        actions.move_to_element(returns_tab).pause(0.2).perform()
        actions.move_by_offset(2, 1).pause(0.1).move_by_offset(-2, -1).pause(0.1).perform()
    except Exception:
        pass
    rd = WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.XPATH, "//a[normalize-space()='Returns Dashboard']")))
    try: rd.click()
    except Exception: driver.execute_script("arguments[0].click();", rd)
    WebDriverWait(driver, 12).until(EC.presence_of_element_located((By.XPATH, "//label[contains(.,'Financial Year')]")))

def normalize_txt(s: str) -> str:
    return " ".join((s or "").split()).strip()


def select_under_label_with_refresh(driver, label_text: str, option_text: str, timeout: int = 10, attempts: int = 2):
    option_text_norm = normalize_txt(option_text)
    for _ in range(attempts):
        try:
            sel_el = WebDriverWait(driver, timeout).until(EC.presence_of_element_located((By.XPATH, f"(//label[contains(., '{label_text}')]/following::select)[1]")))
            try: driver.execute_script("arguments[0].scrollIntoView({block:'center'});", sel_el)
            except Exception: pass
            sel = Select(sel_el)
            try:
                sel.select_by_visible_text(option_text); return
            except Exception:
                pass
            for opt in sel.options:
                txt = normalize_txt(opt.text)
                if option_text_norm == txt or option_text_norm in txt:
                    sel.select_by_visible_text(opt.text); return
            for opt in sel.options:
                val = (opt.get_attribute("value") or "").strip()
                if option_text_norm == val or option_text_norm in val:
                    sel.select_by_value(val); return
            raise NoSuchElementException()
        except (TimeoutException, NoSuchElementException, StaleElementReferenceException):
            driver.refresh(); time.sleep(2)
            re_anchor_to_returns_form(driver)
            continue
    raise NoSuchElementException(f"Failed to select '{option_text}' under '{label_text}' after retries")


def wait_for_dependent_dropdown(label_text: str, driver, timeout=2):
    try:
        WebDriverWait(driver, timeout).until(EC.presence_of_element_located((By.XPATH, f"(//label[contains(., '{label_text}')]/following::select)[1]")))
    except Exception:
        pass


def month_to_quarter(month_name: str) -> str:
    m = month_name.strip().capitalize()
    if m in ("April","May","June"): return "Quarter 1"
    if m in ("July","August","September"): return "Quarter 2"
    if m in ("October","November","December"): return "Quarter 3"
    return "Quarter 4"


def select_fy_quarter_month_and_search_with_refresh(driver, fin_year: str, month_name: str):
    try:
        select_under_label_with_refresh(driver, "Financial Year", fin_year, timeout=10, attempts=2)
    except Exception:
        driver.refresh(); time.sleep(2)
        re_anchor_to_returns_form(driver)
        select_under_label_with_refresh(driver, "Financial Year", fin_year, timeout=8, attempts=2)


    wait_for_dependent_dropdown("Quarter", driver, timeout=2)
    qtext = month_to_quarter(month_name)
    try:
        select_under_label_with_refresh(driver, "Quarter", qtext, timeout=10, attempts=2)
    except Exception:
        driver.refresh(); time.sleep(2)
        re_anchor_to_returns_form(driver)
        select_under_label_with_refresh(driver, "Financial Year", fin_year, timeout=6, attempts=1)
        wait_for_dependent_dropdown("Quarter", driver, timeout=2)
        select_under_label_with_refresh(driver, "Quarter", qtext, timeout=6, attempts=1)


    wait_for_dependent_dropdown("Period", driver, timeout=2)
    try:
        select_under_label_with_refresh(driver, "Period", month_name, timeout=10, attempts=2)
    except Exception:
        driver.refresh(); time.sleep(2)
        re_anchor_to_returns_form(driver)
        select_under_label_with_refresh(driver, "Financial Year", fin_year, timeout=6, attempts=1)
        wait_for_dependent_dropdown("Quarter", driver, timeout=2)
        select_under_label_with_refresh(driver, "Quarter", qtext, timeout=6, attempts=1)
        wait_for_dependent_dropdown("Period", driver, timeout=2)
        select_under_label_with_refresh(driver, "Period", month_name, timeout=6, attempts=1)


    for _ in range(2):
        for loc in [(By.ID, "search"),(By.XPATH, "//button[normalize-space()='SEARCH' or contains(.,'Search')]"),(By.XPATH, "//input[@type='submit' and (contains(@value,'SEARCH') or contains(@value,'Search'))]"),]:
            try:
                WebDriverWait(driver, 2).until(EC.element_to_be_clickable(loc)).click()
                return
            except Exception:
                continue
        driver.refresh(); time.sleep(2)
        re_anchor_to_returns_form(driver)
        select_under_label_with_refresh(driver, "Financial Year", fin_year, timeout=6, attempts=1)
        wait_for_dependent_dropdown("Quarter", driver, timeout=2)
        select_under_label_with_refresh(driver, "Quarter", qtext, timeout=6, attempts=1)
        wait_for_dependent_dropdown("Period", driver, timeout=2)
        select_under_label_with_refresh(driver, "Period", month_name, timeout=6, attempts=1)

def click_gstr2b_tile_heading_hardened(driver):
    # Specific XPaths provided as priority
    FIRST_XPATH = "/html/body/div[2]/div[2]/div/div[2]/div[4]/div[4]/div[1]/div[2]/div/div/div/div/div[1]/button"
    SECOND_XPATH = "/html/body/div[2]/div[2]/div/div[2]/div[4]/div[3]/div[1]/div[3]/div/div/div/div/div[1]/button"

    def try_click_once():
        # 1) Try FIRST_XPATH and SECOND_XPATH in order
        for xpath in [FIRST_XPATH, SECOND_XPATH]:
            try:
                el = driver.find_element(By.XPATH, xpath)
                driver.execute_script("arguments[0].scrollIntoView({block:'center'});", el)
                time.sleep(0.1)
                try:
                    el.click()
                except Exception:
                    driver.execute_script("arguments[0].click();", el)
                return True
            except Exception:
                continue

        # 2) LAST RESORT: Look for ANY element containing 'GSTR-2B' text
        containers = driver.find_elements(By.XPATH, "//div[.//text()[contains(translate(., 'abcdefghijklmnopqrstuvwxyz','ABCDEFGHIJKLMNOPQRSTUVWXYZ'), 'GSTR-2B') or contains(translate(., 'abcdefghijklmnopqrstuvwxyz','ABCDEFGHIJKLMNOPQRSTUVWXYZ'), 'GSTR2B')]]")
        for div in containers:
            try:
                driver.execute_script("arguments[0].scrollIntoView({block:'center'});", div)
            except Exception:
                pass
            
            # Sub-selectors for the dynamic search
            for xp in [".//div[contains(translate(.,'abcdefghijklmnopqrstuvwxyz','ABCDEFGHIJKLMNOPQRSTUVWXYZ'),'GSTR')]",
                       ".//h3[contains(translate(.,'abcdefghijklmnopqrstuvwxyz','ABCDEFGHIJKLMNOPQRSTUVWXYZ'),'GSTR')]",
                       ".//a[contains(translate(.,'abcdefghijklmnopqrstuvwxyz','ABCDEFGHIJKLMNOPQRSTUVWXYZ'),'GSTR')]",
                       ".//button[contains(translate(.,'abcdefghijklmnopqrstuvwxyz','ABCDEFGHIJKLMNOPQRSTUVWXYZ'),'GSTR')]",
                       ".//a|.//button"]:
                try:
                    el = div.find_element(By.XPATH, xp)
                    try: 
                        el.click()
                    except Exception: 
                        driver.execute_script("arguments[0].click();", el)
                    return True
                except Exception:
                    continue
        return False

    # Standard "Hardened" scroll-and-search routine
    try: 
        driver.execute_script("window.scrollTo(0,0);")
    except Exception: 
        pass

    for y in range(0, 4000, 350):
        try: 
            driver.execute_script(f"window.scrollTo(0,{y});")
        except Exception: 
            pass
        time.sleep(0.12)
        if try_click_once(): return True

    # Refresh and full re-scroll if still not found
    driver.refresh()
    time.sleep(2)
    for y in range(0, 4000, 350):
        try: 
            driver.execute_script(f"window.scrollTo(0,{y});")
        except Exception: 
            pass
        time.sleep(0.12)
        if try_click_once(): return True

    return False


def ensure_on_gstr2b_page(driver, max_wait=12):
    start = time.time()
    def is_gstr2b_page():
        try:
            WebDriverWait(driver, 2).until(EC.presence_of_element_located((By.XPATH,"//*[contains(translate(.,'abcdefghijklmnopqrstuvwxyz','ABCDEFGHIJKLMNOPQRSTUVWXYZ'),'GSTR-2B') and " "(contains(.,'SUMMARY') or contains(.,'ALL TABLES') or contains(.,'DOWNLOAD GSTR-2B'))]")))
            return True
        except Exception:
            return False
    while time.time() - start < max_wait:
        if is_gstr2b_page(): return True
        if click_gstr2b_tile_heading_hardened(driver):
            return True
        time.sleep(0.2)
    return False


def click_gstr2b_details_excel_with_refresh(driver):
    for _ in range(2):
        if ensure_on_gstr2b_page(driver, max_wait=10):
            for xp in ["//button[normalize-space()='DOWNLOAD GSTR-2B DETAILS (EXCEL)']","//a[normalize-space()='DOWNLOAD GSTR-2B DETAILS (EXCEL)']","//button[contains(translate(.,'abcdefghijklmnopqrstuvwxyz','ABCDEFGHIJKLMNOPQRSTUVWXYZ'),'DOWNLOAD GSTR-2B DETAILS (EXCEL)')]",]:
                try:
                    btn = WebDriverWait(driver, 6).until(EC.element_to_be_clickable((By.XPATH, xp)))
                    try: driver.execute_script("arguments[0].scrollIntoView({block:'center'});", btn)
                    except Exception: pass
                    try: btn.click()
                    except Exception: driver.execute_script("arguments[0].click();", btn)
                    return True
                except Exception:
                    continue
        driver.refresh(); time.sleep(2)
    return False


def click_back_to_dashboard(driver):
    for xp in [
        "//button[normalize-space()='BACK TO DASHBOARD']",
        "//a[normalize-space()='BACK TO DASHBOARD']",
        "//button[contains(.,'BACK') and contains(.,'DASHBOARD')]",
    ]:
        try:
            btn = WebDriverWait(driver, 4).until(
                EC.element_to_be_clickable((By.XPATH, xp))
            )
            driver.execute_script(
                "arguments[0].scrollIntoView({block:'center'});", btn
            )
            driver.execute_script("arguments[0].click();", btn)
            WebDriverWait(driver, 10).until(
                EC.presence_of_element_located(
                    (By.XPATH, "//label[contains(.,'Financial Year')]")
                )
            )
            return True
        except Exception:
            continue

    try:
        driver.back()
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located(
                (By.XPATH, "//label[contains(.,'Financial Year')]")
            )
        )
        return True
    except Exception:
        return False



# ---------- FY-wide loop ----------
def download_all_months_for_fy_from_form(driver, fin_year, today, download_dir, job_id=None):
    months = months_allowed_for_fy(fin_year, today)
    if not months:
        pass
        print(f"No months available for FY {fin_year}."); return
    downloaded = {m: False for m in months}
    failed_months = []
    pass

    WebDriverWait(driver, 12).until(EC.presence_of_element_located((By.XPATH, "//label[contains(.,'Financial Year')]")))

    for m in months:
        if job_id:
            JOB_STATUS[job_id]["months"][m] = MONTH_RUNNING

        select_fy_quarter_month_and_search_with_refresh(driver, fin_year, m)
        ok = click_gstr2b_details_excel_with_refresh(driver)

        if ok and wait_for_downloads_complete(download_dir, timeout=240):
            downloaded[m] = True
            if job_id:
                JOB_STATUS[job_id]["months"][m] = MONTH_COMPLETED
        else:
            downloaded[m] = False
            if job_id:
                JOB_STATUS[job_id]["months"][m] = MONTH_FAILED
                failed_months.append(m)

        click_back_to_dashboard(driver)
        time.sleep(0.3)
    # ==============================
    # 🔁 RETRY FAILED MONTHS
    # ==============================
    if failed_months:
        logger.info(f"Retrying failed months for FY {fin_year}: {failed_months}")

        for m in failed_months:
            try:
                if job_id:
                    JOB_STATUS[job_id]["months"][m] = MONTH_RETRYING

                select_fy_quarter_month_and_search_with_refresh(driver, fin_year, m)

                ok = click_gstr2b_details_excel_with_refresh(driver)

                if ok and wait_for_downloads_complete(download_dir, timeout=240):
                    if job_id:
                        JOB_STATUS[job_id]["months"][m] = MONTH_COMPLETED
                else:
                    if job_id:
                        JOB_STATUS[job_id]["months"][m] = MONTH_FAILED_AGAIN

            except Exception as e:
                logger.error(f"Retry failed for {m}: {e}")
                if job_id:
                    JOB_STATUS[job_id]["months"][m] = MONTH_FAILED_AGAIN

            click_back_to_dashboard(driver)
            time.sleep(0.3)




# ---------- Consolidation ----------
def _infer_month_from_filename(name: str) -> str:
    months = MONTHS_APR_TO_MAR
    for m in months:
        if re.search(rf"\b{m}\b", name, flags=re.IGNORECASE):
            return m
    return ""


def consolidate_gstr2b_monthlies(fy_folder: Path, fin_year: str):
    excel_files = sorted([p for p in fy_folder.glob("*.xlsx") if p.is_file() and not p.name.startswith(f"GSTR2B_Combined_{fin_year}")])
    if not excel_files:
        return None

    target_sheets = ["B2B", "B2BA", "B2B-CDNR", "B2B-CDNRA", "ISD", "ISDA", "IMPG", "IMPGSEZ", "Ecomm", "EcommA"]
    stacks = {s: [] for s in target_sheets}

    for f in excel_files:
        month_label = _infer_month_from_filename(f.name)
        try:
            all_sheets = pd.read_excel(f, sheet_name=None, engine="openpyxl")
        except Exception:
            continue
        for s in target_sheets:
            if s in all_sheets:
                df = all_sheets[s]
                if df is None or df.empty:
                    continue
                df.columns = [str(c).strip() for c in df.columns]
                df.insert(0, "Month", month_label or "")
                df.insert(1, "SourceFile", f.name)
                stacks[s].append(df)

    out_path = fy_folder / f"GSTR2B_Combined_{fin_year}.xlsx"
    with pd.ExcelWriter(out_path, engine="openpyxl", mode="w") as writer:
        for s, parts in stacks.items():
            if parts:
                df_all = pd.concat(parts, ignore_index=True)
                df_all.to_excel(writer, sheet_name=s, index=False)
            else:
                pd.DataFrame(columns=["Month","SourceFile"]).to_excel(writer, sheet_name=s, index=False)
    return out_path

# ===============================================================
# CHANGE 3 of 3: REPLACE THE `main` FUNCTION AND THE SCRIPT ENTRY POINT
# ===============================================================
# The original main() function is replaced by this new logic.




def main():
    raise RuntimeError(
        "Standalone execution is disabled. "
        "Start the Flask server and use the web UI."
    )



app = Flask(__name__)

CORS(app, resources={r"/*": {"origins": "*"}}, supports_credentials=True)

@app.route("/run-gstr2b", methods=["POST"])
def run_gstr2b():
    data = request.json

    required = ["gstin", "password", "fy", "only_fy", "path", "client"]

    for k in required:
        if k not in data:
            return jsonify({"error": f"Missing field: {k}"}), 400

    if not data.get("only_fy") and not data.get("month"):
        return jsonify({"error": "Month is required when only_fy is false"}), 400


    job_id = str(uuid.uuid4())

    # Prepare month list
    if data["only_fy"]:
        months = months_allowed_for_fy(data["fy"], date.today())
    else:
        months = [data["month"].capitalize()]

    JOB_STATUS[job_id] = {
        "status": "RUNNING",
        "stage": "DOWNLOADING",
        "months": {m: MONTH_PENDING for m in months},
        "client": data["client"],
        "fy": data["fy"],
        "base_path": data["path"]
    }



    vals = {
        "GSTIN": data["gstin"],
        "PASSWORD": data["password"],
        "FY": data["fy"],
        "ONLY_FY": data["only_fy"],
        "MONTH": data.get("month", ""),
        "DL_PATH": data["path"],
        "CLIENT": data["client"]
    }


    def background_job():
        try:
            zip_path = run_automation(vals, job_id)

            failed = [
                m for m, s in JOB_STATUS[job_id]["months"].items()
                if s != MONTH_COMPLETED
            ]

            JOB_STATUS[job_id]["failed_months"] = failed

            if not zip_path or not os.path.exists(zip_path):
                raise Exception("ZIP file was not created")

            JOB_STATUS[job_id]["zip_path"] = zip_path
            JOB_STATUS[job_id]["status"] = "COMPLETED"
            JOB_STATUS[job_id]["download_url"] = f"/download/{job_id}"
            JOB_STATUS[job_id]["client"] = data["client"]
            JOB_STATUS[job_id]["fy"] = data["fy"]
            JOB_STATUS[job_id]["base_path"] = data["path"]

            # ✅ ADD THIS LOGIC
            if failed:
                JOB_STATUS[job_id]["stage"] = "COMPLETED_WITH_ERRORS"
            else:
                JOB_STATUS[job_id]["stage"] = "DONE"


        except Exception as e:
            existing = JOB_STATUS.get(job_id, {})
            bug_log = existing.get("bug_log", [])
            bug_log = bug_log + [f"Job failed with exception: {e}"]
            JOB_STATUS[job_id] = {
                "status": "FAILED",
                "stage": "FAILED",
                "error": str(e),
                "months": existing.get("months", {}),
                "bug_log": bug_log,
            }


    threading.Thread(target=background_job, daemon=True).start()

    return jsonify({
        "job_id": job_id,
        "status": "RUNNING"
    }), 200

@app.route("/job-status/<job_id>", methods=["GET"])
def job_status(job_id):
    if job_id not in JOB_STATUS:
        return jsonify({"error": "Invalid job ID"}), 404
    
    logger.info("JOB STATUS: %s", JOB_STATUS[job_id])  #DEBUG
    return jsonify(JOB_STATUS[job_id]), 200

from flask import send_file

@app.route("/download/<job_id>", methods=["GET"])
def download(job_id):
    job = JOB_STATUS.get(job_id)
    if not job or not job.get("zip_path"):
        return "File not ready", 404

    zip_path = Path(job["zip_path"])
    if not zip_path.exists():
        return "File not found", 404

    response = send_file(
        zip_path,
        mimetype="application/zip",
        as_attachment=True,
        download_name=zip_path.name,
        max_age=0,
        conditional=False
    )

    # 🧹 CLEANUP AFTER DOWNLOAD
    try:
        fy_folder = Path(job["base_path"]) / job["client"] / job["fy"]
        if fy_folder.exists():
            shutil.rmtree(fy_folder, ignore_errors=True)
    except Exception:
        pass

    return response


def run_automation(vals, job_id=None):

    today = date.today()

    fin_year = vals.get('FY')
    month_name = (vals.get('MONTH') or "").strip().capitalize()
    only_fy = bool(vals.get('ONLY_FY'))
    user = vals.get('GSTIN')
    pwd = vals.get('PASSWORD')
    base_path = Path(vals.get('DL_PATH')).expanduser().resolve()
    client_name = vals.get('CLIENT')

    fy_folder = base_path / client_name / fin_year
    fy_folder.mkdir(parents=True, exist_ok=True)

    driver = setup_chrome(fy_folder)
    try:
        click_header_login(driver)
        type_creds(driver, user, pwd)

        # ---------------- CAPTCHA FLOW START ----------------
        JOB_DRIVERS[job_id] = driver

        cap_path = capture_captcha_image(driver, job_id, fy_folder)
        if cap_path:
            JOB_STATUS.setdefault(job_id, {})
            JOB_STATUS[job_id].update({
                "status": "WAITING_FOR_CAPTCHA",
                "captcha": True
            })

            JOB_STATUS[job_id]["captcha"] = True

            logger.info("Waiting for captcha submission by user...")

            captcha_start = time.time()
            CAPTCHA_TIMEOUT = 180  # seconds

            while True:
                status = JOB_STATUS.get(job_id, {}).get("status")

                if status == "RUNNING":  # user submitted captcha
                    break

                if status == "FAILED":
                    return

                if time.time() - captcha_start > CAPTCHA_TIMEOUT:
                    JOB_STATUS[job_id]["status"] = "FAILED"
                    JOB_STATUS[job_id]["error"] = "Captcha timeout"
                    return

                time.sleep(0.5)


            
            try:
                wait_until_logged_in(driver, timeout=180)
            except Exception:
                JOB_STATUS[job_id]["status"] = "FAILED"
                JOB_STATUS[job_id]["error"] = "Invalid captcha or login failed"
                return



        # ---------------- CAPTCHA FLOW END ----------------

        hover_returns_and_click_dashboard(driver)


        if only_fy:
            download_all_months_for_fy_from_form(driver, fin_year, today, fy_folder, job_id=job_id)
        else:
            JOB_STATUS[job_id]["months"][month_name] = MONTH_RUNNING
            select_fy_quarter_month_and_search_with_refresh(driver, fin_year, month_name)

            if click_gstr2b_details_excel_with_refresh(driver) and wait_for_downloads_complete(fy_folder):
                JOB_STATUS[job_id]["months"][month_name] = MONTH_COMPLETED
            else:
                JOB_STATUS[job_id]["months"][month_name] = MONTH_FAILED


    finally:
        try:
            time.sleep(5)  # allow Chrome to flush downloads
            driver.quit()
        except Exception as e:
            print("Driver quit error:", e)





    combined_file = consolidate_gstr2b_monthlies(fy_folder, fin_year)
    time.sleep(1)  # allow file handles to release before zipping

    zip_path = zip_folder(fy_folder)

    return str(zip_path)


@app.route("/captcha/<job_id>", methods=["GET"])
def get_captcha(job_id):
    job = JOB_STATUS.get(job_id)
    if not job:
        return "Invalid job", 404

    # reconstruct folder
    client = job.get("client")
    fy = job.get("fy")
    base = job.get("base_path")

    fy_folder = Path(base) / client / fy
    path = fy_folder / "captcha.png"

    if not path.exists():
        return "CAPTCHA not ready", 404

    return send_file(
        path,
        mimetype="image/png",
        as_attachment=False,
        conditional=False,
        max_age=0
    )



@app.route("/submit-captcha/<job_id>", methods=["POST"])
def submit_captcha(job_id):
    from pathlib import Path

    data = request.json
    captcha_text = data.get("captcha", "").strip()

    if not captcha_text:
        return jsonify({"error": "Empty captcha"}), 400

    driver = JOB_DRIVERS.get(job_id)
    if not driver:
        return jsonify({"error": "Driver not found"}), 404

    try:
        # 1️⃣ Locate captcha input
        input_box = WebDriverWait(driver, 20).until(
            EC.element_to_be_clickable((
                By.XPATH,
                "//input[contains(translate(@placeholder,'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'captcha') "
                "or contains(translate(@aria-label,'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'captcha') "
                "or contains(@id,'captcha') or contains(@name,'captcha')]"
            ))
        )

        input_box.clear()
        input_box.send_keys(captcha_text)

        # 2️⃣ Click Login / Verify
        for xp in [
            "//button[contains(.,'Login') or contains(.,'Submit') or contains(.,'Verify')]",
            "//input[@type='submit']"
        ]:
            try:
                driver.find_element(By.XPATH, xp).click()
                time.sleep(2)
                break
            except Exception:
                pass

        # 3️⃣ Mark job as running again
        JOB_STATUS[job_id]["status"] = "RUNNING"

        # 4️⃣ 🧹 DELETE CAPTCHA IMAGE (THIS WAS MISSING)
        try:
            job = JOB_STATUS[job_id]
            fy_folder = Path(job["base_path"]) / job["client"] / job["fy"]
            (fy_folder / "captcha.png").unlink(missing_ok=True)
        except Exception:
            pass

        # 5️⃣ Cleanup flag
        JOB_CAPTCHA_READY.pop(job_id, None)

        return jsonify({"status": "CAPTCHA_ACCEPTED"})

    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/")
def home():
    return "GST Automation Server is running"


if __name__ == "__main__":
    import sys
    if "--standalone" in sys.argv:
        main()
    else:
        app.run(
            host="0.0.0.0",
            port=5000,
        )
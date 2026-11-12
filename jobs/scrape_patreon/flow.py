import datetime as dt
import os
import re
import shutil
import subprocess
import time
from logging import Logger, LoggerAdapter
from pathlib import Path

import gdown
import undetected_chromedriver as uc
from prefect import flow, get_run_logger, task
from prefect.cache_policies import NO_CACHE
from pyvirtualdisplay import Display
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

from hooks import discord_failure_hook

_CHROME_CANDIDATE_PATHS = [
    "/usr/bin/google-chrome-stable",
    "/usr/bin/google-chrome",
    "/usr/bin/chromium-browser",
    "/usr/bin/chromium",
    "/snap/bin/chromium",
]

CHROME_PROFILE_DIR = Path(
    os.environ.get("PATREON_CHROME_PROFILE", "/opt/c3po/state/patreon-chrome-profile")
)
# Cookie that indicates a logged-in Patreon session. Verify against the
# profile's Default/Cookies db if the "already logged in" check never passes.
SESSION_COOKIE = "session_id"


class PatreonVerificationRequired(RuntimeError):
    """Cloudflare challenge appeared; the saved session needs a manual refresh."""


def _find_chrome_binary() -> str | None:
    for name in ["google-chrome-stable", "google-chrome", "chromium-browser", "chromium"]:
        binary = shutil.which(name)
        if binary:
            return binary
    for path in _CHROME_CANDIDATE_PATHS:
        if Path(path).exists():
            return path
    return None


def _get_chrome_version(binary: str) -> int | None:
    try:
        output = subprocess.check_output([binary, "--version"], stderr=subprocess.DEVNULL).decode()
        match = re.search(r"(\d+)\.", output)
        if match:
            return int(match.group(1))
    except Exception:
        pass
    return None


def _download_path(name: str) -> Path:
    path = Path(os.environ["PATREON_DOWNLOAD_PATH"]) / name
    path.mkdir(parents=True, exist_ok=True)
    return path


def _remove_part_files(folder: Path) -> None:
    for part_file in folder.glob("*.part"):
        part_file.unlink()


def _page_state(driver: uc.Chrome) -> str | None:
    """Return 'challenge', 'login', 'logged_in', or None if still loading."""
    try:
        if (
            "just a moment" in driver.title.lower()
            or "performing security verification" in driver.page_source.lower()
        ):
            return "challenge"
        if driver.find_elements(By.NAME, "email"):
            return "login"
        url = driver.current_url
        if "/login" not in url and driver.get_cookie(SESSION_COOKIE):
            return "logged_in"
    except Exception:
        pass
    return None


def _ensure_logged_in(driver: uc.Chrome, logger: LoggerAdapter[Logger]) -> None:
    """Reuse the saved session if valid; otherwise log in. Fail on Cloudflare challenge."""
    wait = WebDriverWait(driver, 30)
    driver.get("https://www.patreon.com/home")
    time.sleep(10)

    state = wait.until(lambda d: _page_state(d))
    if state == "challenge":
        raise PatreonVerificationRequired(
            "Cloudflare verification appeared. Refresh the Chrome profile manually."
        )
    if state == "logged_in":
        logger.info("Existing Patreon session is valid, skipping login")
        return

    logger.info("Not logged in, logging in")
    email_element = wait.until(EC.element_to_be_clickable((By.NAME, "email")))
    email_element.send_keys(os.environ["PATREON_USERNAME"])
    email_element.submit()
    # Patreon's login is a two-step form: enter email, submit, then the
    # password field renders in. Wait for each field to be interactable
    # rather than relying on fixed sleeps.
    password_element = wait.until(EC.element_to_be_clickable((By.NAME, "current-password")))
    password_element.send_keys(os.environ["PATREON_PASSWORD"])
    password_element.submit()

    try:
        state = WebDriverWait(driver, 30).until(
            lambda d: (s := _page_state(d)) in ("challenge", "logged_in") and s
        )
    except Exception:
        # Login may land on a page without the session cookie visible yet; re-check /home.
        driver.get("https://www.patreon.com/home")
        state = _page_state(driver)

    if state == "challenge":
        raise PatreonVerificationRequired("Cloudflare verification appeared after login.")
    if state != "logged_in":
        raise RuntimeError("Login did not complete (check credentials or 2FA)")
    logger.info("Logged in successfully")


@task(cache_policy=NO_CACHE)
def fetch_collection_links(
    driver: uc.Chrome, count: int, name: str, url: str
) -> list[tuple[Path, str]]:
    logger = get_run_logger()
    download_folder = _download_path(name)
    _remove_part_files(download_folder)
    logger.info(f"Getting posts for {name}")

    existing = list(download_folder.glob("*"))
    if len(existing) == count:
        logger.info("Number of files equal the post count. Skipping")
        return []

    driver.get(url)
    time.sleep(3)
    while load_more := [
        e for e in driver.find_elements(By.TAG_NAME, "button") if e.text == "Load more"
    ]:
        load_more[0].click()
        time.sleep(5)

    all_links = driver.find_elements(By.TAG_NAME, "a")
    col_id = url.split("/")[-1]
    seen: dict[str, tuple[str, str]] = {}
    for a in all_links:
        href = a.get_property("href")
        if not href or not href.endswith(f"collection={col_id}") or href in seen:
            continue
        parts = a.text.split("\n")
        if parts[0] == "Google Docs":
            seen[href] = (parts[1], href)
    post_links = list(seen.values())[::-1]
    logger.info(f"Found {len(post_links)} posts")

    col_posts = []
    gdrive_url = "https://drive.google.com"
    for i, (file_name, post_link) in enumerate(post_links):
        file_name = file_name.replace("/", "_").replace("\\", "_")
        file_name = f"{i + 1:03} {file_name}.mp4"
        if file_name == "005 Reacting to One Punch Man S3X4.mp4":
            file_name = "004 Reacting to One Punch Man S3X4.mp4"
        if file_name == "004 Reacting to One Punch Man S3X5.mp4":
            file_name = "005 Reacting to One Punch Man S3X5.mp4"
        file_path = download_folder / file_name
        if file_path.exists():
            logger.info(f"{file_name} exists")
            continue
        driver.get(post_link)
        time.sleep(2)
        all_links = driver.find_elements(By.TAG_NAME, "a")
        download_link = [
            link.get_property("href")
            for link in all_links
            if link.get_property("href").startswith(gdrive_url)
        ][0]
        col_posts.append((file_path, download_link))

    return col_posts


@task(cache_policy=NO_CACHE)
def download_posts(col_name: str, posts: list[tuple[Path, str]]) -> None:
    logger = get_run_logger()
    if not posts:
        return
    logger.info(f"Downloading {len(posts)} missing posts for {col_name}")
    for post_path, drive_url in posts[::-1]:
        gdrive_id = drive_url.split("/")[-2]
        down_url = f"https://drive.google.com/uc?id={gdrive_id}"
        retry = True
        retries = 0
        while retry and retries < 5:
            try:
                logger.info(f"Downloading {post_path.name}")
                gdown.download(down_url, str(post_path), quiet=True)
            except gdown.exceptions.FileURLRetrievalError:
                logger.warning(f"Couldn't download {post_path.name}")
                retry = False
            except (BlockingIOError, OSError):
                retries += 1
                logger.warning(f"Retrying attempt {retries} for {post_path.name}")
                time.sleep(30)
            else:
                retry = False


@flow(name="scrape-patreon", on_failure=[discord_failure_hook])
def scrape_patreon() -> None:
    logger = get_run_logger()
    download_cols = ["Avatar The Last Airbender"]
    error_path = Path(os.environ["PATREON_DOWNLOAD_PATH"]) / "error_screenshots"
    error_path.mkdir(exist_ok=True)
    CHROME_PROFILE_DIR.mkdir(parents=True, exist_ok=True)

    chrome_binary = _find_chrome_binary()
    chrome_version = _get_chrome_version(chrome_binary) if chrome_binary else None
    logger.info(f"Chrome binary: {chrome_binary}")
    logger.info(f"Detected Chrome version: {chrome_version}")
    options = uc.ChromeOptions()
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    options.add_argument("--disable-extensions")
    options.add_argument("--disable-background-networking")
    options.add_argument("--disable-sync")
    options.add_argument("--no-first-run")
    with Display(visible=False):
        # NOTE: do not use uc.Chrome as a context manager. undetected-chromedriver
        # 3.5.5's __exit__ restarts the driver service and opens a *new* session
        # instead of quitting, which fails with SessionNotCreatedException
        # ("chrome not reachable") once the browser is torn down. Manage teardown
        # explicitly via driver.quit() in a finally block instead.
        driver = uc.Chrome(
            subprocess=True,
            version_main=chrome_version,
            browser_executable_path=chrome_binary,
            user_data_dir=str(CHROME_PROFILE_DIR),
            options=options,
        )
        try:
            _ensure_logged_in(driver, logger)

            driver.get("https://www.patreon.com/c/omariorpg/collections")
            time.sleep(10)
            for _ in range(25):
                last_height = driver.execute_script("return document.body.scrollHeight")
                driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                time.sleep(3)
                new_height = driver.execute_script("return document.body.scrollHeight")
                if new_height == last_height:
                    break

            links = driver.find_elements(By.TAG_NAME, "a")
            collection_a_tags = [
                a
                for a in links
                if str(a.get_property("href")).startswith("https://www.patreon.com/collection/")
            ]
            collection_links: dict[str, dict[str, str | int]] = {}
            for col_link in collection_a_tags:
                try:
                    wrapper = col_link.find_element(
                        By.XPATH,
                        "./ancestor::div[.//p[@data-tag='box-collection-num-post']][1]",
                    )
                    try:
                        col_count_str = wrapper.find_element(
                            By.CSS_SELECTOR, "p[data-tag='box-collection-num-post']"
                        ).text.strip()
                        col_name = col_link.text.strip()
                        if not col_name:
                            col_name = wrapper.find_element(
                                By.CSS_SELECTOR, "[data-tag='box-collection-title']"
                            ).text.strip()
                        collection_links[col_name] = {
                            "name": col_name,
                            "count": int(col_count_str),
                            "url": col_link.get_property("href"),
                        }
                    except Exception:
                        logger.warning(f"Could not parse {wrapper.text}")
                        raise
                except Exception as e:
                    logger.warning(f"Failed to parse collection link: {e}")

            logger.info(f"Collections found: {list(collection_links.keys())}")

            for col in download_cols:
                if col not in collection_links:
                    logger.info(f"Couldn't find the collection: {col}")
                    continue
                col_data = collection_links[col]
                posts = fetch_collection_links(
                    driver,
                    col_data["count"],
                    col_data["name"],
                    col_data["url"],
                )
                download_posts(col_data["name"], posts)
        except Exception:
            driver.save_screenshot(error_path / f"{dt.datetime.now():%Y%m%d_%H%M%S}.png")
            raise
        finally:
            driver.quit()


if __name__ == "__main__":
    scrape_patreon()

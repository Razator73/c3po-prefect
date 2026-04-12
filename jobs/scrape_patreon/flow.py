import os
import time
from pathlib import Path

import gdown
import razator_utils
import undetected_chromedriver as uc
from prefect import flow, get_run_logger, task
from prefect.cache_policies import NO_CACHE
from pyvirtualdisplay import Display
from selenium.webdriver.common.by import By

from hooks import discord_failure_hook


def _download_path(name: str) -> Path:
    path = Path(os.environ["PATREON_DOWNLOAD_PATH"]) / name
    path.mkdir(parents=True, exist_ok=True)
    return path


def _remove_part_files(folder: Path) -> None:
    for part_file in folder.glob("*.part"):
        part_file.unlink()


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
    post_links = [
        (a.text.split("\n")[-1], a.get_property("href"))
        for a in all_links
        if a.get_property("href").endswith(f"collection={url.split('/')[-1]}")
    ][::-1]
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
    download_cols = ["Naruto Shippuden", "JJK", "Frieren"]

    with Display(visible=False):
        chrome_version = razator_utils.get_chrome_major_version()
        logger.info(f"Detected Chrome version: {chrome_version}")
        options = uc.ChromeOptions()
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        with uc.Chrome(subprocess=True, version_main=chrome_version, options=options) as driver:
            driver.get("https://www.patreon.com/login")
            time.sleep(3)
            email_element = driver.find_element(By.NAME, "email")
            email_element.send_keys(os.environ["PATREON_USERNAME"])
            email_element.submit()
            time.sleep(1)
            driver.find_element(By.NAME, "current-password").send_keys(
                os.environ["PATREON_PASSWORD"]
            )
            email_element.submit()
            time.sleep(2)

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


if __name__ == "__main__":
    scrape_patreon()

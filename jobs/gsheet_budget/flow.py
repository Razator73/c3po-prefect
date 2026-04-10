import datetime as dt
import json
import os
import re
from pathlib import Path

import pygsheets
import razator_utils
from dateutil.relativedelta import relativedelta
from prefect import flow, get_run_logger, task
from prefect.cache_policies import NO_CACHE
from sqlalchemy import text
from tabulate import tabulate

from hooks import discord_failure_hook
from jobs.gsheet_budget.model import Deposit, Withdrawal, init_db

_CRED_FILE = Path.home() / ".creds" / "gdrive.json"
_MAPPING_DIR = Path(__file__).parent
_IS_PROD = os.getenv("C3PO_ENV") == "prod"

# Maps sheet account name aliases to DB account IDs
_ACCOUNT_IDS = {
    "Checking": 1,
    "Savings 1": 2,
    "Savings 2": 3,
    "Credit Card": 4,
    "Amazon Card": 5,
    "HSA": 6,
    "Zions Checking": 1,
    "Zions Savings 1": 2,
    "Zions Savings 2": 3,
    "MACU Checking": 7,
    "MACU Savings": 8,
}

_ALL_MONTHS = ["Dec", "Nov", "Oct", "Sep", "Aug", "Jul", "Jun", "May", "Apr", "Mar", "Feb", "Jan"]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _get_sheet_data(sheets, sheet_names, year, w_range, d_range, w_data, d_data):
    for sheet in [x for x in sheets if x.title in sheet_names]:
        month_cell = [f"{year} - {sheet.title}"]
        w_data += [x + month_cell for x in sheet.range(f"{w_range}{sheet.rows}") if x[1].value]
        d_data += [x + month_cell for x in sheet.range(f"{d_range}{sheet.rows}") if x[1].value]
    return w_data, d_data


def _get_notes(note, add_note, mapping):
    if mapping.get(note) or note == "":
        is_missing = False
        note = mapping.get(note)
        if isinstance(note, dict):
            add_note = (
                f"{note['additional_notes']}\n{add_note}" if add_note else note["additional_notes"]
            )
            note = note["notes"]
    else:
        is_missing = True
    add_note = add_note.strip() if add_note else add_note
    return note, add_note, is_missing


# ---------------------------------------------------------------------------
# update_budget_db
# ---------------------------------------------------------------------------


@task(cache_policy=NO_CACHE)
def fetch_budget_data(full_refresh: bool) -> tuple[list, list]:
    logger = get_run_logger()
    gc = pygsheets.authorize(service_file=_CRED_FILE)
    cur_date = dt.datetime.now()

    if full_refresh:
        good_sheets = [f"Budget {x}" for x in range(2017, cur_date.year + 1)]
    else:
        good_sheets = [f"Budget {cur_date.year}"]

    withdrawal_data: list = []
    deposit_data: list = []

    if full_refresh:
        logger.info("Getting data for Budget 2015...")
        budget_sheet = gc.open("Budget 2015")
        all_sheets = budget_sheet.worksheets()
        pre_deposit_shift = ["April 2015", "March 2015", "Feb 2015"]
        withdrawal_data, deposit_data = _get_sheet_data(
            all_sheets,
            pre_deposit_shift,
            "Budget 2015",
            "G3:K",
            "L3:P",
            withdrawal_data,
            deposit_data,
        )
        post_deposit_shift = [
            "Dec 2015",
            "Nov 2015",
            "Oct 2015",
            "Sept 2015",
            "Aug 2015",
            "July 2015",
            "June 2015",
            "May 2015",
        ]
        withdrawal_data, deposit_data = _get_sheet_data(
            all_sheets,
            post_deposit_shift,
            "Budget 2015",
            "G3:K",
            "M3:Q",
            withdrawal_data,
            deposit_data,
        )

        logger.info("Getting data for Budget 2016...")
        budget_sheet = gc.open("Budget 2016")
        all_sheets = budget_sheet.worksheets()
        jan_sheet = [x for x in all_sheets if x.title == "Jan2016"][0]
        withdrawal_data += [
            x + ["Budget 2016 - Jan2016"]
            for x in jan_sheet.range(f"G3:K{jan_sheet.rows}")
            if x[1].value
        ]
        deposit_data += [
            x + ["Budget 2016 - Jan2016"]
            for x in jan_sheet.range(f"M3:Q{jan_sheet.rows}")
            if x[1].value
        ]
        months_2016 = [f"{month}2016" for month in _ALL_MONTHS if month != "Jan"]
        withdrawal_data, deposit_data = _get_sheet_data(
            all_sheets,
            months_2016,
            "Budget 2016",
            "H3:L",
            "N3:R",
            withdrawal_data,
            deposit_data,
        )

    for sheet_title in good_sheets:
        budget_sheet = gc.open(sheet_title)
        all_sheets = budget_sheet.worksheets()
        logger.info(f"Getting data for {sheet_title}...")
        withdrawal_data, deposit_data = _get_sheet_data(
            all_sheets,
            _ALL_MONTHS,
            sheet_title,
            "H3:L",
            "N3:R",
            withdrawal_data,
            deposit_data,
        )

    logger.info(f"Fetched {len(withdrawal_data)} withdrawals, {len(deposit_data)} deposits")
    return withdrawal_data, deposit_data


@task(cache_policy=NO_CACHE)
def save_budget_data(withdrawal_data: list, deposit_data: list, full_refresh: bool) -> None:
    logger = get_run_logger()
    session = init_db()

    with open(_MAPPING_DIR / "withdrawal_mapping.json") as f:
        withdrawal_mapping = json.load(f)
    with open(_MAPPING_DIR / "deposit_mapping.json") as f:
        deposit_mapping = json.load(f)

    if full_refresh:
        session.query(Withdrawal).delete()
        session.query(Deposit).delete()
    else:
        cur_date = dt.datetime.now()
        session.query(Withdrawal).filter(
            Withdrawal.withdrawal_date >= dt.date(cur_date.year, 1, 1)
        ).delete()
        session.query(Deposit).filter(Deposit.deposit_date >= dt.date(cur_date.year, 1, 1)).delete()

    missing_withdrawals = []
    bad_withdrawal_rows = []
    for row in withdrawal_data:
        notes, add_notes, is_missing = _get_notes(row[4].value, row[4].note, withdrawal_mapping)
        if is_missing:
            missing_withdrawals.append(notes)
        try:
            session.add(
                Withdrawal(
                    withdrawal_date=dt.datetime.strptime(row[0].value, "%m/%d/%Y"),
                    amount=round(float(re.sub("[$,]", "", row[1].value)), 2),
                    account_id=_ACCOUNT_IDS[row[2].value],
                    category=row[3].value,
                    notes=notes,
                    additional_notes=add_notes,
                )
            )
        except Exception as e:
            bad_withdrawal_rows.append((row, str(e)))

    for note in set(missing_withdrawals):
        logger.info(f"Missing {note} from the withdrawal mapping json")

    missing_deposits = []
    bad_deposit_rows = []
    for row in deposit_data:
        notes, add_notes, is_missing = _get_notes(row[4].value, row[4].note, deposit_mapping)
        if is_missing:
            missing_deposits.append(notes)
        try:
            session.add(
                Deposit(
                    deposit_date=dt.datetime.strptime(row[0].value, "%m/%d/%Y"),
                    amount=round(float(re.sub("[$,]", "", row[1].value)), 2),
                    account_id=_ACCOUNT_IDS[row[2].value],
                    category=row[3].value,
                    notes=notes,
                    additional_notes=add_notes,
                )
            )
        except Exception as e:
            bad_deposit_rows.append((row, str(e)))

    for note in set(missing_deposits):
        logger.info(f"Missing {note} from the deposit mapping json")

    session.commit()
    session.close()
    if bad_withdrawal_rows:
        for row, err in bad_withdrawal_rows:
            logger.warning(f"Failed to save withdrawal row: {err}")
    if bad_deposit_rows:
        for row, err in bad_deposit_rows:
            logger.warning(f"Failed to save deposit row: {err}")
    logger.info(
        f"Saved {len(withdrawal_data) - len(bad_withdrawal_rows)} withdrawals, "
        f"{len(deposit_data) - len(bad_deposit_rows)} deposits to DB"
    )


@flow(name="update-budget-db", on_failure=[discord_failure_hook])
def update_budget_db(full_refresh: bool = False) -> None:
    today = dt.date.today()
    if today.month == 1 and today.day == 1:
        get_run_logger().info("Skipping Jan 1 — new year sheet not yet created.")
        return
    withdrawal_data, deposit_data = fetch_budget_data(full_refresh)
    save_budget_data(withdrawal_data, deposit_data, full_refresh)


# ---------------------------------------------------------------------------
# fun_funds_email
# ---------------------------------------------------------------------------


@task(cache_policy=NO_CACHE)
def send_fun_funds_emails() -> None:
    logger = get_run_logger()
    session = init_db()
    try:
        now = dt.date.today()
        if now.month == 1:
            start_previous = now.replace(year=now.year - 1, month=12, day=1)
        else:
            start_previous = now.replace(month=now.month - 1, day=1)

        query = text("""
            select * from personal_fun_funds
            where transaction_date between :start_date and :end_date
            order by person, transaction_date, transaction_type;
        """)
        result = session.execute(query, {"start_date": start_previous, "end_date": now}).fetchall()

        funds_map: dict[str, list] = {"Ryan": [], "Emerald": []}
        headers = ["date", "description", "withdrawal", "deposit", "balance"]
        for row in result:
            person = row.person
            if person not in funds_map:
                continue
            amount = row.amount
            balance = row.balance
            description = row.additional_notes if row.additional_notes else ""
            if amount > 0:
                line = [row.transaction_date, description, "", f"${amount:.2f}", f"${balance:.2f}"]
            else:
                line = [
                    row.transaction_date,
                    description,
                    f"${abs(amount):.2f}",
                    "",
                    f"${balance:.2f}",
                ]
            funds_map[person].append(line)

        email_map = {
            "Ryan": "ryan.t.scott73@gmail.com" if _IS_PROD else "test@razator.com",
            "Emerald": "emerald.scott11715@gmail.com" if _IS_PROD else "test@razator.com",
        }
        for person, items in funds_map.items():
            if not items:
                continue
            recipient = email_map.get(person)
            if not recipient:
                continue
            final_data = [headers] + items
            table = tabulate(final_data, headers="firstrow", tablefmt="html").replace(
                "<table>", "<table border='1'>"
            )
            html_content = f"""
                <html><body><p>Hey {person},</p>
                <p>Here is a summary of your fun funds data for the last month or so.
                With a current balance around {items[-1][4]}.</p>
                {table}
                <p>Happy shopping,</p>
                <p>Ryan</p>
                </body></html>
            """
            razator_utils.send_email(
                recipient, f"{person} Fun Funds", html_content, logger, is_html=True
            )
            logger.info(f"Sent fun funds email to {person}")
    finally:
        session.close()


@flow(name="fun-funds-email", on_failure=[discord_failure_hook])
def fun_funds_email() -> None:
    send_fun_funds_emails()


# ---------------------------------------------------------------------------
# budget_new_month
# ---------------------------------------------------------------------------


@task(cache_policy=NO_CACHE)
def create_new_month_sheet() -> None:
    logger = get_run_logger()
    gc = pygsheets.authorize(service_file=_CRED_FILE)
    now = dt.datetime.now()
    this_month_name = now.strftime("%b")
    next_month_date = now + relativedelta(months=1)
    next_month_name = next_month_date.strftime("%b")
    next_month_index = next_month_date.month + 4

    sheet_title = f"Budget {now.year}"
    logger.info(f"Opening Google Sheet: {sheet_title}")
    ss = gc.open(sheet_title)

    tm = ss.worksheet_by_title(this_month_name)

    try:
        nm = ss.worksheet_by_title(next_month_name)
        logger.info(f"Worksheet {next_month_name} already exists.")
    except pygsheets.exceptions.WorksheetNotFound:
        logger.info(f"Creating worksheet {next_month_name}")
        nm = ss.add_worksheet(next_month_name, src_worksheet=tm)
        nm.index = next_month_date.month

    date_str = next_month_date.replace(day=1).strftime("%m/%d/%Y")
    nm.clear("H4", "L200", fields="userEnteredValue,note")
    nm.clear("N5", "R200", fields="userEnteredValue,note")
    nm.update_value("H3", date_str)
    nm.update_value("N3", date_str)
    nm.update_value("N4", date_str)
    nm.update_value("E2", f"={this_month_name}!D2")
    nm.clear("B18", "B19")
    nm.clear("B21", "B23")
    this_month_index = next_month_index - 1
    nm.update_value(
        "E10", f"=AVERAGE(Overview!E2:{pygsheets.Address((2, this_month_index)).label})"
    )
    for i in range(14, 25):
        nm.update_value(
            f"E{i}",
            f"=AVERAGE(Overview!E{i - 11}:{pygsheets.Address((i - 11, this_month_index)).label})",
        )

    nm.update_value("B2", f"={this_month_name}!B2 - sumif(J:J,A2,I:I) + sumif(P:P,A2,O:O)")
    nm.update_value("B3", f"={this_month_name}!B3 - sumif(J:J,A3,I:I) + sumif(P:P,A3,O:O)")
    nm.update_value("B4", f"={this_month_name}!B4 - sumif(J:J,A4,I:I) + sumif(P:P,A4,O:O)")
    nm.update_value("B6", f"={this_month_name}!B6 - sumif(J:J,A6,I:I) + sumif(P:P,A6,O:O)")
    nm.update_value("B7", f"={this_month_name}!B7 - sumif(J:J,A7,I:I) + sumif(P:P,A7,O:O)")
    nm.update_value(
        "B30",
        f'={this_month_name}!B30+sumifs(O:O,R:R,CONCATENATE(A30," ",$A$29))'
        f'-sumifs(I:I,L:L,CONCATENATE(A30," ",$A$29))',
    )
    nm.update_value(
        "B31",
        f'={this_month_name}!B31+sumifs(O:O,R:R,CONCATENATE(A31," ",$A$29))'
        f'-sumifs(I:I,L:L,CONCATENATE(A31," ",$A$29))',
    )
    nm.update_value("B33", f'={this_month_name}!B33-sumifs(O:O,R:R,"Kareena loan payment")')
    nm.update_value("B34", f'={this_month_name}!B34-sumifs(O:O,R:R,"Jenn and AJ loan payment")')
    nm.update_value("B35", f'={this_month_name}!B35-sumifs(O:O,R:R,"NAS payment")')

    overview = ss.worksheet_by_title("Overview")
    for i in range(2, 13):
        overview.update_value(
            f"{pygsheets.Address((i, next_month_index)).label}",
            f"=iferror(vlookup(B{i},{next_month_name}!A:C,3,False))",
        )
    overview.update_value(
        f"{pygsheets.Address((13, next_month_index)).label}",
        f"=sum({pygsheets.Address((3, next_month_index)).label}"
        f":{pygsheets.Address((12, next_month_index)).label})",
    )
    overview.update_value(
        f"{pygsheets.Address((1, next_month_index)).label}", next_month_date.strftime("%B")
    )
    logger.info("Done creating new month.")


@flow(name="budget-new-month", on_failure=[discord_failure_hook])
def budget_new_month() -> None:
    create_new_month_sheet()


# ---------------------------------------------------------------------------
# budget_new_year
# ---------------------------------------------------------------------------


@task(cache_policy=NO_CACHE)
def create_new_year_sheet() -> None:
    logger = get_run_logger()
    gc = pygsheets.authorize(service_file=_CRED_FILE)
    now = dt.datetime.now()
    next_year = now.year + 1
    current_year = next_year - 1

    logger.info(f"Opening current Google Sheet: Budget {current_year}")
    ss_current = gc.open(f"Budget {current_year}")

    logger.info(f"Creating new spreadsheet: Budget {next_year}")
    ss_new = gc.create(f"Budget {next_year}")

    folder_id = gc.drive.get_folder_id("Finances")
    file_metadata = [
        f for f in gc.drive.spreadsheet_metadata() if f["name"] == f"Budget {next_year}"
    ][0]
    gc.drive.move_file(ss_new.id, old_folder=file_metadata["parents"][0], new_folder=folder_id)
    logger.info("Moved new sheet to Finances folder.")

    for tab in ["DataValidations", "Jan", "Overview"]:
        ws = ss_current.worksheet_by_title(tab)
        ss_new.add_worksheet(tab, src_worksheet=ws)
    ss_new.del_worksheet(ss_new.worksheet_by_title("Sheet1"))

    overview = ss_new.worksheet_by_title("Overview")
    overview.clear("F1", "P13")
    overview.index = 1

    jan = ss_new.worksheet_by_title("Jan")
    jan.index = 0
    jan.clear("H4", "L200", fields="userEnteredValue,note")
    jan.clear("N5", "R200", fields="userEnteredValue,note")
    year_start = dt.date(next_year, 1, 1)
    jan.update_value("H3", year_start.strftime("%m/%d/%Y"))
    jan.update_value("N3", year_start.strftime("%m/%d/%Y"))
    jan.update_value("N4", year_start.strftime("%m/%d/%Y"))

    dec = ss_current.worksheet_by_title("Dec")
    jan.update_value("E2", dec.get_value("D2"))

    def _strip(val: str) -> str:
        return val.replace("$", "").replace(",", "")

    acct_formula = " - sumif(J:J,A{n},I:I) + sumif(P:P,A{n},O:O)"
    for cell, col in [("B2", "B2"), ("B3", "B3"), ("B4", "B4"), ("B6", "B6"), ("B7", "B7")]:
        n = cell[1:]
        jan.update_value(cell, f"={_strip(dec.get_value(col))}{acct_formula.format(n=n)}")
    jan.update_value(
        "B30",
        f'={_strip(dec.get_value("B30"))} + sumifs(O:O,R:R,CONCATENATE(A30," ",$A$29))'
        f'-sumifs(I:I,L:L,CONCATENATE(A30," ",$A$29))',
    )
    jan.update_value(
        "B31",
        f'={_strip(dec.get_value("B31"))} + sumifs(O:O,R:R,CONCATENATE(A31," ",$A$29))'
        f'-sumifs(I:I,L:L,CONCATENATE(A31," ",$A$29))',
    )
    jan.update_value(
        "B33",
        f'={_strip(dec.get_value("B33"))} - sumifs(O:O,R:R,"Kareena loan payment")',
    )
    jan.update_value(
        "B34",
        f"={_strip(dec.get_value('B34'))} + sumifs(I:I,L:L,A34)"
        f' - sumifs(O:O,R:R,"Jenn and AJ loan payment")',
    )
    jan.update_value("F10", f"={_strip(dec.get_value('F10'))}")
    for i in range(14, 25):
        jan.update_value(f"F{i}", f"={_strip(dec.get_value(f'F{i}'))}")

    try:
        logger.info(
            f"Initiating ownership transfer to ryan.t.scott73@gmail.com for Budget {next_year}..."
        )
        # The writer role is inherited from the parent folder — Google requires a direct
        # file-level permission to set pendingOwner. Create one explicitly first.
        new_perm = (
            gc.drive.service.permissions()
            .create(
                fileId=ss_new.id,
                body={"type": "user", "role": "writer", "emailAddress": "ryan.t.scott73@gmail.com"},
                sendNotificationEmail=True,
            )
            .execute()
        )

        gc.drive.service.permissions().update(
            fileId=ss_new.id,
            permissionId=new_perm["id"],
            body={"role": "writer", "pendingOwner": True},
        ).execute()

        logger.info("Ownership transfer initiated. Accept the invitation in Google Drive.")
    except Exception as e:
        logger.warning(f"Could not initiate ownership transfer automatically: {e}")
        logger.warning("Manually transfer ownership of the new sheet in Google Drive.")

    logger.info(f"Done creating budget sheet for {next_year}.")


@flow(name="budget-new-year", on_failure=[discord_failure_hook])
def budget_new_year() -> None:
    create_new_year_sheet()


# ---------------------------------------------------------------------------
# budget_reminder / budget_summary
# ---------------------------------------------------------------------------


@task(cache_policy=NO_CACHE)
def send_budget_reminder() -> None:
    logger = get_run_logger()
    gc = pygsheets.authorize(service_file=_CRED_FILE)
    now = dt.datetime.now()
    last_month_name = (now - relativedelta(months=1)).strftime("%B")

    reminder_addr = "ryan.t.scott73@gmail.com" if _IS_PROD else "test@razator.com"
    razator_utils.send_email(
        reminder_addr,
        "Update Budget Sheet",
        (
            f"Hey\n\nThis is your friendly reminder to update the budget sheet "
            f"for {last_month_name}, as it is now a new month. Also don't forget "
            f"to come up with a plan for this month if you haven't yet. "
            f"Also check the email for fun funds has a refreshed token.\n\nThanks"
        ),
        logger,
    )
    logger.info("Sent budget reminder email.")

    if now.month == 1:
        return

    sheet_title = f"Budget {now.year}"
    try:
        ss = gc.open(sheet_title)
        this_month = now.strftime("%b")
        month_sheet = ss.worksheet_by_title(this_month)
        month_sheet.index = 0
        logger.info(f"Moved {this_month} to index 0.")
    except Exception as e:
        logger.error(f"Could not move sheet: {e}")


@task(cache_policy=NO_CACHE)
def send_budget_summary() -> None:
    logger = get_run_logger()
    gc = pygsheets.authorize(service_file=_CRED_FILE)
    now = dt.datetime.now()
    last_month_date = now - relativedelta(months=1)
    last_month = last_month_date.strftime("%b")
    last_month_full = last_month_date.strftime("%B")

    sheet_title = f"Budget {now.year}" if now.month != 1 else f"Budget {now.year - 1}"

    try:
        ss = gc.open(sheet_title)
        month_sheet = ss.worksheet_by_title(last_month)
    except Exception as e:
        logger.error(f"Could not open sheet {last_month} in {sheet_title}: {e}")
        return

    def _val(row: int, col: int) -> str:
        return data[row][col] if len(data) > row and len(data[row]) > col else "0"

    data = month_sheet.get_values("A10", "E24", returnas="matrix")
    spent = round(float(_val(14, 2).replace("$", "").replace(",", "")), 2)
    earned = round(float(_val(0, 2).replace("$", "").replace(",", "")), 2)
    cash = round(float(month_sheet.get_value("D2").replace("$", "").replace(",", "") or 0), 2)

    categories = []
    for i in range(4, 15):
        if len(data) > i and len(data[i]) > 2:
            cat_name = data[i][0]
            if not cat_name:
                continue
            budgeted = float(data[i][1].replace("$", "").replace(",", "") or 0)
            actual_spent = float(data[i][2].replace("$", "").replace(",", "") or 0)
            if budgeted - actual_spent < 0 and cat_name != "Tithing":
                categories.append(cat_name)

    over_budget = (
        "We didn't go over budget on anything. Go us."
        if not categories
        else "The following categories are where we went over budget:\n\n\t"
        + "\n\t".join(categories)
    )
    net_gain = round(earned - spent, 2)
    message = (
        f"Hey,\n\nHere is a summary of the finances for the month of {last_month_full}.\n\n"
        f"\tTotal Earned:\t\t${earned}\n"
        f"\tTotal Spent: \t\t${spent}\n"
        f"\tNet Gain:    \t\t${net_gain}\n"
        f"\tCash Value:  \t\t${cash}\n\n"
        f"{over_budget}"
    )
    prod_addrs = ["ryan.t.scott73@gmail.com", "emerald.scott11715@gmail.com"]
    email_addrs = prod_addrs if _IS_PROD else ["test@razator.com"]
    razator_utils.send_email(
        email_addrs,
        "Month End Budget Summary",
        message,
        logger,
    )
    logger.info("Sent budget summary email.")


@flow(name="budget-reminder", on_failure=[discord_failure_hook])
def budget_reminder() -> None:
    send_budget_reminder()


@flow(name="budget-summary", on_failure=[discord_failure_hook])
def budget_summary() -> None:
    send_budget_summary()

from process import process_players
from etl_scripts.api import get_fpl_data, get_league_data, get_gameweek
from services.db import Subcribers
from services.build_charts import build_charts, update_status
from services.send_email import send_email
import logging
import os
from datetime import datetime
import pytz

logger = logging.getLogger("fpl_newsletter")
logger.setLevel(logging.INFO)

BASE_LOG_PATH = os.environ.get('LOG_DIR', '/tmp')
LOG_FILENAME = 'fpl_newsletter.log'
LOG_PATH = os.path.join(BASE_LOG_PATH, "fpl_newsletter", datetime.now(
    pytz.timezone('US/Pacific')).strftime('%Y-%m-%d_%H-%M-%S'), LOG_FILENAME)
os.makedirs(os.path.dirname(LOG_PATH), exist_ok=True)

file_handler = logging.FileHandler(LOG_PATH)
formatter = logging.Formatter(
    '%(levelname)s - %(asctime)s - %(name)s - %(message)s')
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)


gameweek, _ = get_gameweek()


def main():
    """
    Entry point to program
    """
    subs = Subcribers()
    logger.info(f"current subscribers: {subs.get_df_as_list()}")
    get_fpl_data()
    for sub in subs.get_df_as_list():
        league_number, subscriber_id = sub["league_number"], sub["subscriber_id"]
        email, first_name, last_name = sub["email"], sub["first_name"], sub["last_name"]
        full_name = f"{first_name} {last_name}"
        get_league_data(league_number)
        report_path, message_body = process_players(league_number)
        chart_paths = build_charts(league_number, subscriber_id)
        send_email(chart_paths + report_path, message_body, email, full_name)
        if chart_paths:
            update_status(gameweek, subscriber_id)
    return


if __name__ == "__main__":
    logger.info(
        f"Starting execution at {datetime.now(pytz.timezone('US/Pacific')).strftime('%Y-%m-%d %H:%M:%S')}")
    main()
    logger.info(
        f'Finished execution at {datetime.now(pytz.timezone("US/Pacific")).strftime("%Y-%m-%d %H:%M:%S")}')

from process import process_players
from etl_scripts.api import get_fpl_data, get_league_data
from services.db import Subcribers
from services.build_charts import build_charts
from services.send_email import send_email


def main():
    """
    Entry point to program
    """
    subs = Subcribers()
    get_fpl_data()
    for sub in subs.get_df_as_list():
        league_number, subscriber_id = sub["league_number"], sub["subscriber_id"]
        email, first_name, last_name = sub["email"], sub["first_name"], sub["last_name"]
        full_name = f"{first_name} {last_name}"
        get_league_data(league_number)
        report_path, message_body = process_players(league_number)
        paths = build_charts(league_number, subscriber_id)
        rc = send_email(paths + report_path, message_body, email, full_name)
    return rc


if __name__ == "__main__":
    print(main())

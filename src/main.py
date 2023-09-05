import os
os.chdir(os.path.dirname(os.path.abspath(__file__)))
from process import process_players
from etl_scripts.api import get_data
from services.build_charts import build_charts
from services.send_email import send_email

# entry point to program
def main():
    get_data()
    report_path, message_body = process_players()
    paths = build_charts()
    rc = send_email(paths + report_path, message_body)
    return rc
    
if __name__ == "__main__":
    print(main())
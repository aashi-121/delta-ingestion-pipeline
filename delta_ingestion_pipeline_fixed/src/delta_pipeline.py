from apscheduler.schedulers.blocking import BlockingScheduler
from config.config import DELTA_PATH, INTERVAL_MINUTES, TIMEZONE, NUM_ROWS
from delta_handler import DeltaTableHandler
from utils import generate_fake_data
from email_notifier import send_email_notification

def run_pipeline():
    print("🔁 Running data ingestion pipeline...")
    handler = DeltaTableHandler(DELTA_PATH)
    df = generate_fake_data(NUM_ROWS)
    handler.append_data(df)
    latest_df = handler.get_latest_version()
    print("\n✅ Latest data in Delta Table:")
    latest_df.show(truncate=False)
    send_email_notification(latest_df)

if __name__ == "__main__":
    print("🚀 Starting scheduler...")
    scheduler = BlockingScheduler(timezone=TIMEZONE)
    scheduler.add_job(run_pipeline, 'interval', minutes=INTERVAL_MINUTES)
    run_pipeline()
    scheduler.start()
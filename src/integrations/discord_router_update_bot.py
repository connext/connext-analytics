import os
import asyncio
import logging
import requests
from pandas_gbq import read_gbq
from datetime import datetime, timedelta
from dotenv import load_dotenv

# from discord import Intents
import interactions


load_dotenv()
logging.basicConfig(level=logging.INFO)
WEBHOOK_URL = os.getenv("discord_router_webhook_url")
DISCORD_ROUTER_TOKEN = os.getenv("discord_router_update_bot_token")
CHANNEL_ID = int(os.getenv("discord_router_update_bot_channel_id"))
PROJECT_ID = os.getenv("gcp_project_id")
QUERY = """
WITH raw AS (

SELECT *, locked + fees_earned AS current_balance
FROM `mainnet-bigq.y42_connext_main.routers_assets_balance_hist`
WHERE snapshot_time = (
    SELECT MAX(snapshot_time)
    FROM `mainnet-bigq.y42_connext_main.routers_assets_balance_hist`
)
)

SELECT
    router_address AS router,
    SUM(current_balance) AS balance
FROM raw
WHERE current_balance > 0
GROUP BY 1
"""

# Define the intents
# intents = Intents.default()
# intents.messages = True  # Enable intents for messages, adjust as needed

# Initiate discord bot
bot = interactions.Client(token=DISCORD_ROUTER_TOKEN)


def fetch_data():
    logging.info("Fetching data from BigQuery...")
    df = read_gbq(QUERY, project_id=PROJECT_ID)
    return df


async def send_data_to_webhook():
    logging.info(f"Starting data fetch at {datetime.now()}")
    try:
        df = fetch_data()
        logging.info("Data fetched from BigQuery")
        if df is not None and not df.empty:
            message = df.to_string(index=False)
            payload = {"content": f"Hourly Update:\n```\n{message}\n```"}
            response = requests.post(WEBHOOK_URL, json=payload)
            logging.info("Data posted to Discord")
            if response.status_code != 204:
                logging.error(
                    f"Failed to send message to Discord: {response.status_code} {response.text}"
                )
        else:
            logging.info("No data fetched from BigQuery.")
    except Exception as e:
        logging.error(f"Error fetching data: {e}")


async def scheduled_task():
    while True:
        now = datetime.now()
        next_hour = (now + timedelta(hours=1)).replace(
            minute=0, second=0, microsecond=0
        )
        delay = (next_hour - now).total_seconds()
        await send_data_to_webhook()
        await asyncio.sleep(delay)


@interactions.slash_command(name="info", description="Get info about Router Metrics")
async def getinfo(ctx: interactions.SlashContext):
    await ctx.send(
        "Router Metrics is a bot that sends hourly updates on the most active routers on Mainnet."
    )


@interactions.slash_command(
    name="getrouter", description="Fetch the latest data from BigQuery"
)
async def getrouterbalance(ctx: interactions.SlashContext):
    logging.info("Manual data fetch triggered via command")
    try:
        df = fetch_data()
        if df is not None and not df.empty:
            message = df.to_string(index=False)
            await ctx.send(f"Requested Data:\n```\n{message}\n```")
            logging.info("Data fetched from BigQuery and posted to Discord")
        else:
            await ctx.send("No data fetched from BigQuery.")
    except Exception as e:
        logging.error(f"Error fetching data: {e}")
        await ctx.send("Error fetching data.")


@interactions.slash_command(
    name="getrouterbyaddress", 
    description="Fetch the latest data for a specific router from BigQuery"
)
@interactions.option(
    name="router_address",
    description="The address of the router",
    required=True,
    type=str
)
async def getrouterinfobyaddress(ctx: interactions.SlashContext, router_address: str):
    logging.info(f"Manual data fetch triggered for router: {router_address}")
    try:
        df = fetch_data()
        if df is not None and not df.empty:
            router_data = df[df['router'] == router_address]
            if not router_data.empty:
                message = router_data.to_string(index=False)
                await ctx.send(f"Requested Data for {router_address}:\n```\n{message}\n```")
                logging.info(f"Data fetched for router {router_address} and posted to Discord")
            else:
                await ctx.send(f"No data found for router {router_address}.")
        else:
            await ctx.send("No data fetched from BigQuery.")
    except Exception as e:
        logging.error(f"Error fetching data: {e}")
        await ctx.send("Error fetching data.")

@interactions.listen()
async def on_startup():
    logging.info("Bot is ready!")

    # Ensure commands are synced
    bot.sync_interactions
    asyncio.create_task(scheduled_task())


if __name__ == "__main__":
    # asyncio.run(bot.start(), reload=True)

    # router active/inactive metrics
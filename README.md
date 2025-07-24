NPA Depot Manager Monitor Bot
Overview
The NPA Depot Manager Monitor Bot is a Telegram bot designed to monitor new Depot Manager records from the National Petroleum Authority (NPA) API and notify subscribed Telegram group chats. The bot fetches data every 5 minutes, processes it, stores it in a PostgreSQL database, and sends notifications about new records to subscribed groups. It supports group chat subscriptions, admin-only commands, and detailed record tracking.
This project consists of four main files:

bot.py: Handles Telegram bot functionality and group notifications.
main.py: Manages data fetching from the NPA API and database population.
config.py: Centralizes environment variable handling.
utils.py: Provides shared logging configuration.

Features

Automated Monitoring: Checks the NPA API every 5 minutes for new Depot Manager records.
Group Notifications: Sends updates to subscribed Telegram groups.
Admin Controls: Group admins can subscribe/unsubscribe their groups.
Superadmin Features: Superadmins can list all subscribed groups.
Database Integration: Stores records in PostgreSQL with history and new record tracking.
Commands:
/start: Displays welcome message.
/help: Lists available commands.
/status: Shows bot status.
/subscribe: Subscribes a group to notifications (admin only).
/unsubscribe: Unsubscribes a group (admin only).
/check: Manually checks for new records.
/recent: Displays the last 10 Depot Manager records.
/stats: Shows detailed bot statistics.
/groups: Lists subscribed groups (superadmin only).



Prerequisites

Python: Version 3.8 or higher.
PostgreSQL: A running PostgreSQL database.
Telegram Bot Token: Obtain from BotFather on Telegram.
Dependencies:
python-telegram-bot
pandas
requests
sqlalchemy
psycopg2-binary
openpyxl



Setup Instructions

Clone the Repository (or create the project structure):

Create a directory and save the following files: bot.py, main.py, config.py, utils.py.
Ensure the directory has write permissions for log files and group_subscriptions.json.


Install Dependencies:
pip install python-telegram-bot pandas requests sqlalchemy psycopg2-binary openpyxl


Set Up Environment Variables:

Create a .env file or export variables:export TELEGRAM_BOT_TOKEN='your_bot_token_here'
export TELEGRAM_SUPERADMIN_IDS='admin_id1,admin_id2'
export DB_USER='your_db_user'
export DB_PASSWORD='your_db_password'
export DB_HOST='your_db_host'
export DB_NAME='your_db_name'


Alternatively, set a DATABASE_URL:export DATABASE_URL='postgresql://user:password@host:port/dbname'




Initialize the Database:

Run main.py to create necessary database tables:python main.py


This sets up tables for each status (e.g., Depot_Manager), their history tables, and a depot_manager_new_records table.


Run the Bot:

Start the bot:python bot.py


The bot will begin monitoring the database and responding to Telegram commands.


Add the Bot to a Telegram Group:

Add the bot to a Telegram group using its handle.
Promote the bot to an admin in the group to allow it to read messages and manage subscriptions.



Usage

Group Commands:

/subscribe: Subscribe the group to receive notifications (admin only).
/unsubscribe: Unsubscribe the group from notifications (admin only).
/check: Manually trigger a check for new records.
/recent: View the last 10 Depot Manager records.
/status: Check bot status (monitoring, subscriptions, etc.).
/stats: View detailed statistics.
/groups: List all subscribed groups (superadmin only).
/help: Display available commands.


Monitoring:

The bot automatically checks the NPA API every 5 minutes.
New Depot Manager records are saved to the database and notified to subscribed groups.


Logs:

Check telegram_bot.log for bot activity and errors.
Check npa_data.log for data fetching and processing logs.


Stopping the Bot:

Press Ctrl+C to stop the bot gracefully.



Project Structure

bot.py: Telegram bot logic, group management, and notification system.
main.py: API data fetching, processing, and database population.
config.py: Environment variable management for bot token and database credentials.
utils.py: Shared logging configuration.
group_subscriptions.json: Stores group subscription data (auto-generated).
telegram_bot.log: Logs bot activities.
npa_data.log: Logs data processing activities.

Troubleshooting

Bot Not Responding:
Verify the TELEGRAM_BOT_TOKEN is correct.
Ensure the bot is an admin in the group.
Check telegram_bot.log for errors.


Database Connection Issues:
Confirm database credentials in environment variables.
Ensure PostgreSQL is running and accessible.
Check npa_data.log for connection errors.


No Notifications:
Ensure the group is subscribed using /subscribe.
Verify the NPA API is accessible and returning data.
Check if new records are being detected in depot_manager_new_records.


Dependency Errors:
Reinstall dependencies using pip install -r requirements.txt (create one if needed).


Logs Not Generating:
Ensure the directory has write permissions.



Contributing
Contributions are welcome! Please submit pull requests or open issues for bugs, feature requests, or improvements.
License
This project is licensed under the MIT License.
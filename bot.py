#!/usr/bin/env python3
"""
Telegram Bot for NPA Depot Manager Record Notifications
Monitors the database every 5 minutes and reports new records
Enhanced for robust group chat functionality and executes main.py functionalities
Now using Supabase instead of PostgreSQL for better reliability
"""

import asyncio
import json
import os
from datetime import datetime
from typing import Set, Dict
import pandas as pd
from telegram import Bot, Update, ChatMember
from telegram.ext import Application, CommandHandler, ContextTypes, ChatMemberHandler
from telegram.constants import ChatType, ParseMode
from main import DataFetcher, PDFGenerator, main
from supabase_handler import SupabaseTableGenerator
from config import get_bot_token, get_superadmin_ids
from utils import setup_logging
from io import BytesIO

try:
    from fpdf import FPDF
except ImportError:
    FPDF = None

# Setup logging
logger = setup_logging('telegram_bot.log')

class GroupChatManager:
    """Manages group chat subscriptions and permissions"""
    
    def __init__(self, storage_file: str = "group_subscriptions.json"):
        self.storage_file = storage_file
        self.subscribed_groups: Set[str] = set()
        self.group_admins: Dict[str, Set[str]] = {}
        self.load_subscriptions()
    
    def load_subscriptions(self):
        """Load subscriptions from file with robust error handling"""
        try:
            if os.path.exists(self.storage_file):
                with open(self.storage_file, 'r') as f:
                    data = json.load(f)
                    self.subscribed_groups = set(data.get('groups', []))
                    self.group_admins = {k: set(v) for k, v in data.get('admins', {}).items()}
            else:
                logger.info("No subscriptions file found, starting with empty subscriptions")
                self.subscribed_groups = set()
                self.group_admins = {}
        except json.JSONDecodeError as e:
            logger.error(f"Corrupted subscriptions file: {e}. Initializing empty subscriptions")
            self.subscribed_groups = set()
            self.group_admins = {}
        except Exception as e:
            logger.error(f"Failed to load subscriptions: {e}")
            self.subscribed_groups = set()
            self.group_admins = {}
    
    def save_subscriptions(self):
        """Save subscriptions to file"""
        try:
            data = {
                'groups': list(self.subscribed_groups),
                'admins': {k: list(v) for k, v in self.group_admins.items()}
            }
            with open(self.storage_file, 'w') as f:
                json.dump(data, f, indent=2)
        except Exception as e:
            logger.error(f"Failed to save subscriptions: {e}")
    
    def subscribe_group(self, group_id: str) -> bool:
        """Subscribe a group to notifications"""
        self.subscribed_groups.add(group_id)
        self.save_subscriptions()
        return True
    
    def unsubscribe_group(self, group_id: str) -> bool:
        """Unsubscribe a group from notifications"""
        self.subscribed_groups.discard(group_id)
        if group_id in self.group_admins:
            del self.group_admins[group_id]
        self.save_subscriptions()
        return True
    
    def is_subscribed(self, group_id: str) -> bool:
        """Check if group is subscribed"""
        return group_id in self.subscribed_groups
    
    def get_subscribed_groups(self) -> Set[str]:
        """Get all subscribed groups"""
        return self.subscribed_groups.copy()
    
    def add_admin(self, group_id: str, user_id: str):
        """Add admin for a group"""
        if group_id not in self.group_admins:
            self.group_admins[group_id] = set()
        self.group_admins[group_id].add(user_id)
        self.save_subscriptions()
    
    def is_admin(self, group_id: str, user_id: str) -> bool:
        """Check if user is admin for a group"""
        return user_id in self.group_admins.get(group_id, set())

class NPAMonitorBot:
    """Telegram bot for monitoring NPA Depot Manager records using Supabase"""
    
    def __init__(self):
        # Bot configuration
        self.bot_token = get_bot_token()
        self.superadmin_ids = get_superadmin_ids()
        self.application = Application.builder().token(self.bot_token).build()
        self.bot = Bot(token=self.bot_token)
        
        # Group management
        self.group_manager = GroupChatManager()
        
        # Monitoring configuration
        self.monitoring_interval = 300  # 5 minutes
        self.monitoring_active = False
        self.last_check_time = None
        self.total_checks = 0
        self.last_notification_count = 0
        
        # Data processing components using Supabase
        self.table_generator = SupabaseTableGenerator()
        self.data_fetcher = DataFetcher()
        
        # Execute main.py during initialization
        try:
            success = main()
            if success:
                logger.info("main.py functionalities executed successfully during bot initialization")
            else:
                logger.error("Failed to execute main.py functionalities during bot initialization")
                asyncio.create_task(self._notify_superadmins("⚠️ Failed to execute main.py functionalities during initialization"))
        except Exception as e:
            logger.error(f"Error executing main.py functionalities: {str(e)}")
            asyncio.create_task(self._notify_superadmins(f"⚠️ Error executing main.py functionalities: {str(e)}"))
        
        # Setup handlers
        self._setup_handlers()
        logger.info("NPAMonitorBot initialized successfully with Supabase")
    
    def _setup_handlers(self):
        """Setup telegram command handlers"""
        self.application.add_handler(CommandHandler("start", self.start_command))
        self.application.add_handler(CommandHandler("help", self.help_command))
        self.application.add_handler(CommandHandler("status", self.status_command))
        self.application.add_handler(CommandHandler("subscribe", self.subscribe_command))
        self.application.add_handler(CommandHandler("unsubscribe", self.unsubscribe_command))
        self.application.add_handler(CommandHandler("check", self.manual_check_command))
        self.application.add_handler(CommandHandler("recent", self.recent_records_command))
        self.application.add_handler(CommandHandler("stats", self.stats_command))
        self.application.add_handler(CommandHandler("groups", self.list_groups_command))
        self.application.add_handler(CommandHandler("download_pdf", self.download_pdf_command))
        self.application.add_handler(ChatMemberHandler(self.track_chat_members, ChatMemberHandler.MY_CHAT_MEMBER))
    
    async def _notify_superadmins(self, message: str):
        """Notify superadmins of critical errors"""
        for admin_id in self.superadmin_ids:
            try:
                await self.bot.send_message(
                    chat_id=int(admin_id),
                    text=message,
                    parse_mode=ParseMode.MARKDOWN
                )
                logger.info(f"Notified superadmin {admin_id} with message: {message}")
            except Exception as e:
                logger.error(f"Failed to notify superadmin {admin_id}: {e}")
    
    async def track_chat_members(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Track when bot is added/removed from groups"""
        try:
            chat_member = update.my_chat_member
            chat = update.effective_chat
            user = update.effective_user
            if chat.type in [ChatType.GROUP, ChatType.SUPERGROUP]:
                if chat_member.new_chat_member.status == ChatMember.MEMBER:
                    logger.info(f"Bot added to group: {chat.title} (ID: {chat.id}) by user {user.id} ({user.first_name})")
                    await self.bot.send_message(
                        chat_id=chat.id,
                        text=f"🤖 Bot added to {chat.title}! Use /subscribe to enable notifications (admin only).\n\n✨ Now powered by Supabase for better reliability!",
                        parse_mode=ParseMode.MARKDOWN
                    )
                elif chat_member.new_chat_member.status in [ChatMember.LEFT, ChatMember.KICKED]:
                    logger.info(f"Bot removed from group: {chat.title} (ID: {chat.id}) by user {user.id} ({user.first_name})")
                    self.group_manager.unsubscribe_group(str(chat.id))
                    await self._notify_superadmins(
                        f"🚪 Bot removed from group: {chat.title} (ID: {chat.id}) by user {user.id} ({user.first_name})"
                    )
        except Exception as e:
            logger.error(f"Error tracking chat members for chat {update.effective_chat.id}: {e}")
    
    async def _is_user_admin(self, chat_id: int, user_id: int, retries: int = 3) -> bool:
        """Check if user is admin in the chat with retry logic"""
        for attempt in range(retries):
            try:
                chat_member = await self.bot.get_chat_member(chat_id, user_id)
                return chat_member.status in [ChatMember.ADMINISTRATOR, ChatMember.OWNER]
            except Exception as e:
                if attempt < retries - 1:
                    logger.warning(f"Retrying admin check for user {user_id} in chat {chat_id}: {e}")
                    await asyncio.sleep(1)
                else:
                    logger.error(f"Failed to check admin status for user {user_id} in chat {chat_id}: {e}")
                    return False
    
    def _is_superadmin(self, user_id: int) -> bool:
        """Check if user is a superadmin"""
        return str(user_id) in self.superadmin_ids
    
    async def start_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle /start command"""
        chat_type = update.effective_chat.type
        welcome_message = (
            """
🏭 **NPA Depot Manager Monitor Bot**

Hello! I'm here to monitor new Depot Manager records from the NPA system for your group.


**Group Commands:**
• `/help` - Show all commands
• `/status` - Check bot status
• `/subscribe` - Subscribe group to notifications (admin only)
• `/unsubscribe` - Unsubscribe group (admin only)
• `/check` - Manual check for new records
• `/recent` - Show recent records
• `/stats` - Show monitoring statistics
• `/download_pdf` - Download latest report in PDF format (subscribed groups only)

**Admin Note:** Only group administrators can subscribe/unsubscribe the group from notifications.
            """ if chat_type in [ChatType.GROUP, ChatType.SUPERGROUP] else
            """
🏭 **NPA Depot Manager Monitor Bot**

Welcome! This bot is designed to work in group chats to monitor NPA Depot Manager records.


Please add me to a group chat and use the commands there.
            """
        )
        await update.message.reply_text(welcome_message, parse_mode=ParseMode.MARKDOWN)
        logger.info(f"Start command used by user {update.effective_user.id} in chat {update.effective_chat.id}")
    
    async def help_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle /help command"""
        chat_type = update.effective_chat.type
        help_message = (
            """
🤖 **Group Bot Commands**

**Monitoring Commands:**
• `/status` - Check if monitoring is active
• `/check <BRV_number>` - Search for a specific BRV number
• `/recent` - Show last 10 records
• `/stats` - Show monitoring statistics
• `/subscribe` - Subscribe group to notifications (admin only)
• `/unsubscribe` - Unsubscribe group (admin only)
• `/download_pdf` - Download latest report in PDF format (subscribed groups only)
• `/help` - Show this help message

            """ if chat_type in [ChatType.GROUP, ChatType.SUPERGROUP] else
            """
🤖 **Bot Commands (Private Chat)**

• `/help` - Show this help
• `/status` - Check bot status
• `/recent` - Show recent records
• `/stats` - Show statistics
• `/groups` - List subscribed groups (superadmin only)

            """
        )
        await update.message.reply_text(help_message, parse_mode=ParseMode.MARKDOWN)
    
    async def status_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle /status command"""
        # Test Supabase connection
        db_status = "Connected" if self.table_generator.connect_to_database() else "Failed"
        
        status_message = f"""
📊 **Bot Status**

🔄 **Monitoring:** {'Active' if self.monitoring_active else 'Inactive'}
🕒 **Last Check:** {self.last_check_time.strftime("%Y-%m-%d %H:%M:%S") if self.last_check_time else "Never"}
⏱️ **Check Interval:** {self.monitoring_interval // 60} minutes
👥 **Subscribed Groups:** {len(self.group_manager.get_subscribed_groups())}
🔢 **Total Checks:** {self.total_checks}
📬 **Last Notification:** {self.last_notification_count} records
🗄️ **Supabase Database:** {db_status}
💾 **Current Chat:** {'Subscribed' if self.group_manager.is_subscribed(str(update.effective_chat.id)) else 'Not Subscribed'}

        """
        await update.message.reply_text(status_message, parse_mode=ParseMode.MARKDOWN)
        logger.info(f"Status command used by user {update.effective_user.id}")
    
    async def subscribe_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle /subscribe command"""
        chat = update.effective_chat
        user = update.effective_user
        if chat.type not in [ChatType.GROUP, ChatType.SUPERGROUP]:
            await update.message.reply_text("❌ Subscription is only available in group chats.")
            return
        if not (await self._is_user_admin(chat.id, user.id) or self._is_superadmin(user.id)):
            await update.message.reply_text("❌ Only group administrators can subscribe.")
            return
        if self.group_manager.is_subscribed(str(chat.id)):
            await update.message.reply_text("ℹ️ This group is already subscribed!")
            return
        
        # Test Supabase connection before subscribing
        if not self.table_generator.connect_to_database():
            await update.message.reply_text("❌ Cannot subscribe: Supabase database connection failed. Please try again later.")
            return
        
        self.group_manager.subscribe_group(str(chat.id))
        self.group_manager.add_admin(str(chat.id), str(user.id))
        await update.message.reply_text(
            f"✅ **Group Subscribed Successfully!**\n\n🏷️ **Group:** {chat.title}\n👤 **Subscribed by:** {user.first_name}\n✨ **Database:** Supabase Connected",
            parse_mode=ParseMode.MARKDOWN
        )
        logger.info(f"Group {chat.title} ({chat.id}) subscribed by user {user.id}")
    
    async def unsubscribe_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle /unsubscribe command"""
        chat = update.effective_chat
        user = update.effective_user
        if chat.type not in [ChatType.GROUP, ChatType.SUPERGROUP]:
            await update.message.reply_text("❌ Unsubscription is only available in group chats.")
            return
        if not (await self._is_user_admin(chat.id, user.id) or self._is_superadmin(user.id)):
            await update.message.reply_text("❌ Only group administrators can unsubscribe.")
            return
        if not self.group_manager.is_subscribed(str(chat.id)):
            await update.message.reply_text("ℹ️ This group is not subscribed.")
            return
        self.group_manager.unsubscribe_group(str(chat.id))
        await update.message.reply_text(
            f"✅ **Group Unsubscribed Successfully!**\n\n🏷️ **Group:** {chat.title}\n👤 **Unsubscribed by:** {user.first_name}",
            parse_mode=ParseMode.MARKDOWN
        )
        logger.info(f"Group {chat.title} ({chat.id}) unsubscribed by user {user.id}")
    
    def _split_message(self, message: str, max_length: int = 4000) -> list:
        """Split a message into parts while preserving markdown integrity"""
        messages = []
        current_message = ""
        lines = message.split("\n")
        for line in lines:
            if len(current_message) + len(line) + 1 > max_length:
                messages.append(current_message.strip())
                current_message = ""
            current_message += line + "\n"
        if current_message.strip():
            messages.append(current_message.strip())
        return messages
    
    async def manual_check_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle /check command to search for a specific BRV number using Supabase"""
        if not context.args:
            await update.message.reply_text("❌ Please provide a BRV number to search for (e.g., /check AS496820)")
            return
        
        brv_number = context.args[0].strip()
        await update.message.reply_text(f"🔍 Searching for BRV number: `{brv_number}` in Supabase...")
        
        try:
            # Use the new Supabase search method
            found_records = self.table_generator.search_brv_number(brv_number)
            
            if not found_records:
                await update.message.reply_text(f"❌ No records found for BRV number: {brv_number}")
                return
            
            message = f"✅ **Found {len(found_records)} record(s) for BRV number: {brv_number}**\n\n"
            
            for idx, record in enumerate(found_records, start=1):
                row = record['data']
                table_name = record['table'].replace('_', ' ').title()
                
                message += (
                    f"**Record {idx} (Status: {table_name})**\n"
                    f"📅 Date: {row.get('order_date', 'N/A')}\n"
                    f"🔢 Order: {row.get('order_number', 'N/A')}\n"
                    f"🛢️ Product: {row.get('products', 'N/A')}\n"
                    f"📊 Volume: {row.get('volume', 'N/A')}\n"
                    f"💰 Price: {row.get('ex_ref_price', 'N/A')}\n"
                    f"📋 BRV: {row.get('brv_number', 'N/A')}\n"
                    f"🏢 BDC: {row.get('bdc', 'N/A')}\n"
                    "─────────────────\n"
                )
            
            messages = self._split_message(message)
            for msg in messages:
                await update.message.reply_text(msg, parse_mode=ParseMode.MARKDOWN)
            
            logger.info(f"Manual check for BRV {brv_number} completed with {len(found_records)} records found")
            
        except Exception as e:
            logger.error(f"Manual check failed: {e}")
            await update.message.reply_text(f"❌ Check failed: {str(e)}")
    
    async def recent_records_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle /recent command using Supabase"""
        try:
            recent_df = self.table_generator.get_new_depot_manager_records()
            if recent_df.empty:
                await update.message.reply_text("📭 No recent records found.")
                return
            
            recent_df = recent_df.head(10)
            message = "📋 **Recent Depot Manager Records (Last 10):**\n\n"
            
            for idx, (_, row) in enumerate(recent_df.iterrows(), 1):
                message += (
                    f"**Record {idx}:**\n"
                    f"📅 Date: {row.get('order_date', 'N/A')}\n"
                    f"🔢 Order: {row.get('order_number', 'N/A')}\n"
                    f"🛢️ Product: {row.get('products', 'N/A')}\n"
                    f"📊 Volume: {row.get('volume', 'N/A')}\n"
                    f"💰 Price: {row.get('ex_ref_price', 'N/A')}\n"
                    f"📋 BRV: {row.get('brv_number', 'N/A')}\n"
                    f"🏢 BDC: {row.get('bdc', 'N/A')}\n"
                    f"🕒 Detected: {row.get('detected_at', 'N/A')}\n"
                    "─────────────────\n"
                )
            
            messages = self._split_message(message)
            for msg in messages:
                await update.message.reply_text(msg, parse_mode=ParseMode.MARKDOWN)
                
        except Exception as e:
            logger.error(f"Failed to get recent records: {e}")
            await update.message.reply_text(f"❌ Failed to retrieve records: {str(e)}")
    
    async def download_pdf_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle /download_pdf command to send processed data as PDF"""
        chat = update.effective_chat
        if chat.type not in [ChatType.GROUP, ChatType.SUPERGROUP]:
            await update.message.reply_text("❌ This command is only available in group chats.")
            return
        if not self.group_manager.is_subscribed(str(chat.id)):
            await update.message.reply_text("❌ This group is not subscribed. Use /subscribe to enable notifications.")
            return
        if not FPDF:
            await update.message.reply_text("❌ PDF generation is not available (FPDF library not installed).")
            return
        
        try:
            await update.message.reply_text("📄 Generating PDF report from Supabase data...")
            
            df, error = self.data_fetcher.fetch_data()
            if error:
                await update.message.reply_text(f"❌ Failed to fetch data: {error}")
                return
            
            logger.info(f"Fetched {len(df)} records for PDF generation")
            
            processed_df, error = self.data_fetcher.process_data(df)
            if error:
                await update.message.reply_text(f"❌ Failed to process data: {error}")
                return
            
            if processed_df.empty:
                await update.message.reply_text("📭 No processed data found to generate PDF.")
                return
            
            pdf_generator = PDFGenerator()
            title = f"BOST-KUMASI - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
            footnote = "Data sourced from NPA Enterprise API. Processed by I.T.S (Persol System Limited). Modified by Awuah. Powered by Supabase."
            
            pdf_data, error = pdf_generator.generate(processed_df, title, footnote)
            if error:
                await update.message.reply_text(f"❌ Failed to generate PDF: {error}")
                return
            
            pdf_file = BytesIO(pdf_data)
            pdf_file.name = f"BOST-KUMASI_{datetime.now().strftime('%Y%m%d_%H%M%S')}.pdf"
            
            await update.message.reply_document(
                document=pdf_file,
                filename=pdf_file.name,
                caption="📄 Latest Processed Records (Generated from Supabase)",
            )
            logger.info(f"PDF report sent to chat {chat.id}")
            
        except Exception as e:
            logger.error(f"Failed to send PDF to chat {chat.id}: {e}")
            await update.message.reply_text(f"❌ Failed to send PDF: {str(e)}")
    
    async def stats_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle /stats command with Supabase statistics"""
        try:
            uptime = str(datetime.now() - self.last_check_time).split('.')[0] if self.last_check_time else "N/A"
            db_connected = self.table_generator.connect_to_database()
            
            # Get table statistics from Supabase
            table_stats = {}
            if db_connected:
                try:
                    table_stats = self.table_generator.get_table_stats()
                except Exception as e:
                    logger.warning(f"Could not get table stats: {e}")
            
            stats_message = f"""
📊 **Bot Statistics**

**🔄 Monitoring**
• Status: {'Active' if self.monitoring_active else 'Inactive'}
• Uptime: {uptime}
• Check Interval: {self.monitoring_interval // 60} minutes
• Total Checks: {self.total_checks}
• Last Check: {self.last_check_time.strftime("%Y-%m-%d %H:%M:%S") if self.last_check_time else "Never"}

**👥 Groups**
• Subscribed Groups: {len(self.group_manager.get_subscribed_groups())}
• Current Chat: {'Subscribed' if self.group_manager.is_subscribed(str(update.effective_chat.id)) else 'Not Subscribed'}

**📊 Activity**
• Last Notification: {self.last_notification_count} records
• Supabase Database: {'Connected' if db_connected else 'Failed'}

**📈 Table Statistics**
"""
            
            if table_stats:
                for table, count in table_stats.items():
                    table_display = table.replace('_', ' ').title()
                    stats_message += f"• {table_display}: {count} records\n"
            else:
                stats_message += "• Unable to retrieve table statistics\n"
            
            stats_message += "\n✨ **Powered by Supabase**"
            
            await update.message.reply_text(stats_message, parse_mode=ParseMode.MARKDOWN)
            
        except Exception as e:
            logger.error(f"Failed to get stats: {e}")
            await update.message.reply_text(f"❌ Failed to retrieve statistics: {str(e)}")
    
    async def list_groups_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle /groups command"""
        if not self._is_superadmin(update.effective_user.id):
            await update.message.reply_text("❌ This command is for superadmins only.")
            return
        
        try:
            subscribed_groups = self.group_manager.get_subscribed_groups()
            if not subscribed_groups:
                await update.message.reply_text("📭 No groups are subscribed.")
                return
            
            message = f"👥 **Subscribed Groups ({len(subscribed_groups)}):**\n\n"
            
            for group_id in subscribed_groups:
                try:
                    chat = await self.bot.get_chat(int(group_id))
                    message += f"• **{chat.title}** (ID: `{group_id}`)\n"
                except Exception as e:
                    message += f"• **Unknown Group** (ID: `{group_id}`) - Error: {str(e)}\n"
            
            message += "\n✨ **All groups using Supabase backend**"
            await update.message.reply_text(message, parse_mode=ParseMode.MARKDOWN)
            
        except Exception as e:
            logger.error(f"Failed to list groups: {e}")
            await update.message.reply_text(f"❌ Failed to retrieve group list: {str(e)}")
    
    async def _check_for_new_records(self) -> int:
        """Check for new records and notify groups using Supabase"""
        try:
            logger.info("Starting new records check with Supabase...")
            
            # Fetch and process data
            df, error = self.data_fetcher.fetch_data()
            if error:
                logger.error(f"Failed to fetch data: {error}")
                return 0
            
            logger.info(f"Fetched {len(df)} records from API")
            
            processed_df, error = self.data_fetcher.process_data(df)
            if error:
                logger.error(f"Failed to process data: {error}")
                return 0
            
            logger.info(f"Processed {len(processed_df)} records")
            
            # Split and populate Supabase tables
            section_dataframes = self.table_generator.split_dataframe_by_sections(processed_df)
            logger.info(f"Split data into {len(section_dataframes)} sections")
            
            populate_results = self.table_generator.populate_tables(section_dataframes)
            successful_tables = [table for table, success in populate_results.items() if success]
            failed_tables = [table for table, success in populate_results.items() if not success]
            
            if failed_tables:
                logger.warning(f"Some tables failed to populate: {failed_tables}")
                await self._notify_superadmins(f"⚠️ Failed to populate Supabase tables: {failed_tables}")
            
            logger.info(f"Successfully populated tables: {successful_tables}")
            
            # Get new records
            new_records = self.table_generator.get_new_depot_manager_records()
            
            if not new_records.empty:
                await self._notify_subscribed_groups(new_records)
                self.last_notification_count = len(new_records)
                logger.info(f"Notified groups about {len(new_records)} new records")
                return len(new_records)
            
            logger.info("No new records found")
            return 0
            
        except Exception as e:
            logger.error(f"Error checking for new records: {e}")
            await self._notify_superadmins(f"⚠️ Error checking for new records: {str(e)}")
            return 0
    
    async def _notify_subscribed_groups(self, new_records: pd.DataFrame):
        """Send notifications to subscribed groups with rate limiting"""
        subscribed_groups = self.group_manager.get_subscribed_groups()
        if not subscribed_groups:
            logger.info("No subscribed groups to notify")
            return
        
        notification_message = self._format_notification_message(new_records)
        
        for group_id in subscribed_groups:
            try:
                await self.bot.send_message(
                    chat_id=int(group_id),
                    text=notification_message,
                    parse_mode=ParseMode.MARKDOWN
                )
                logger.info(f"Notification sent to group {group_id}")
                await asyncio.sleep(0.1)  # Rate limiting: 10 messages per second
                
            except Exception as e:
                logger.error(f"Failed to send notification to group {group_id}: {e}")
                if "chat not found" in str(e).lower() or "bot was blocked" in str(e).lower():
                    self.group_manager.unsubscribe_group(group_id)
                    logger.info(f"Auto-unsubscribed group {group_id} due to access error")
                    await self._notify_superadmins(
                        f"🚫 Auto-unsubscribed group {group_id} due to error: {str(e)}"
                    )
    
    def _format_notification_message(self, new_records: pd.DataFrame) -> str:
        """Format new records into a notification message"""
        count = len(new_records)
        message = f"""
🚨 **New Depot Manager Records Detected!**

📊 **Total New Records:** {count}
🕒 **Detection Time:** {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}

"""
        
        for idx, (_, row) in enumerate(new_records.head(3).iterrows()):
            message += f"""**Record {idx + 1}:**
📅 Date: {row.get('order_date', 'N/A')}
🔢 Order: {row.get('order_number', 'N/A')}
🛢️ Product: {row.get('products', 'N/A')}
📊 Volume: {row.get('volume', 'N/A')}
💰 Price: {row.get('ex_ref_price', 'N/A')}
📋 BRV: {row.get('brv_number', 'N/A')}
🏢 BDC: {row.get('bdc', 'N/A')}

"""
        
        if count > 3:
            message += f"... and {count - 3} more records.\n\n"
        
        message += "Use `/recent` to see all recent records.\nUse `/download_pdf` to get detailed report."
        return message
    
    async def _monitoring_loop(self):
        """Background monitoring loop with periodic main.py execution using Supabase"""
        logger.info("Started monitoring loop with Supabase backend")
        
        while self.monitoring_active:
            try:
                self.last_check_time = datetime.now()
                self.total_checks += 1
                
                logger.info(f"Starting monitoring check #{self.total_checks}")
                
                # Execute main.py functionalities
                success = main()
                if success:
                    logger.info("main.py functionalities executed successfully")
                else:
                    logger.error("Failed to execute main.py functionalities")
                    await self._notify_superadmins("⚠️ Failed to execute main.py functionalities in monitoring loop")
                
                # Check for new records
                new_records_count = await self._check_for_new_records()
                logger.info(f"Monitoring check completed: {new_records_count} new records")
                
            except Exception as e:
                logger.error(f"Error in monitoring loop: {e}")
                await self._notify_superadmins(f"⚠️ Error in monitoring loop: {str(e)}")
            
            # Wait for next check
            await asyncio.sleep(self.monitoring_interval)
        
        logger.info("Monitoring loop stopped")
    
    def start_monitoring(self):
        """Start monitoring task"""
        if self.monitoring_active:
            logger.info("Monitoring is already active")
            return
        
        self.monitoring_active = True
        asyncio.create_task(self._monitoring_loop())
        logger.info("Background monitoring started with Supabase backend")
    
    def stop_monitoring(self):
        """Stop monitoring task"""
        self.monitoring_active = False
        logger.info("Background monitoring stopped")
    
    async def run(self):
        """Run the bot with webhook"""
        try:
            # Test Supabase connection before starting
            if not self.table_generator.connect_to_database():
                logger.error("Failed to connect to Supabase database")
                await self._notify_superadmins("🚨 Critical Error: Failed to connect to Supabase database")
                return
            
            logger.info("Supabase connection verified successfully")
            
            self.start_monitoring()
            logger.info("Starting NPA Monitor Bot with webhook and Supabase backend...")
            
            await self.application.initialize()
            await self.application.start()
            
            webhook_url = f"https://report-bot-01yl.onrender.com/{self.bot_token}"
            await self.application.bot.set_webhook(
                url=webhook_url,
                allowed_updates=Update.ALL_TYPES
            )
            logger.info(f"Webhook set to {webhook_url}")
            
            await self.application.updater.start_webhook(
                listen="0.0.0.0",
                port=8443,
                url_path=self.bot_token,
                webhook_url=webhook_url
            )
            
            logger.info("Bot is running with Supabase backend.")
            
            # Send startup notification to superadmins
            await self._notify_superadmins("🚀 NPA Monitor Bot started successfully with Supabase backend!")
            
            try:
                while True:
                    await asyncio.sleep(1)
            except KeyboardInterrupt:
                logger.info("Received shutdown signal")
                
        except Exception as e:
            logger.error(f"Error running bot: {e}")
            await self._notify_superadmins(f"⚠️ Error running bot: {str(e)}")
        finally:
            self.stop_monitoring()
            await self.application.bot.delete_webhook()
            await self.application.updater.stop()
            await self.application.stop()
            await self.application.shutdown()
            logger.info("Bot shutdown complete")

async def main():
    """Main function to run the bot"""
    try:
        bot = NPAMonitorBot()
        await bot.run()
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        raise

if __name__ == "__main__":
    asyncio.run(main())
#!/usr/bin/env python3
"""
Enhanced Telegram Bot for NPA Depot Manager Record Notifications
Synchronized with Supabase handler and improved error handling
"""

import asyncio
import json
import os
import re
from datetime import datetime
from typing import Set, Dict, List
import pandas as pd
from telegram import Bot, Update, ChatMember
from telegram.ext import Application, CommandHandler, ContextTypes, ChatMemberHandler
from telegram.constants import ChatType, ParseMode
from main import DataFetcher, PDFGenerator, main
from supabase_handler import SupabaseTableGenerator
from config import CONFIG, Environment
from utils import setup_logging
from io import BytesIO
import functools
from functools import partial

try:
    from fpdf import FPDF
except ImportError:
    FPDF = None

# Setup enhanced logging
logger = setup_logging('telegram_bot.log')

def rate_limit(per_seconds: int = 5):
    """Decorator to rate limit command execution"""
    def decorator(func):
        last_called = {}

        @functools.wraps(func)
        async def wrapped(update: Update, context: ContextTypes.DEFAULT_TYPE, *args, **kwargs):
            chat_id = update.effective_chat.id
            user_id = update.effective_user.id
            key = (chat_id, user_id)
            
            now = datetime.now()
            if key in last_called:
                elapsed = (now - last_called[key]).total_seconds()
                if elapsed < per_seconds:
                    wait = per_seconds - elapsed
                    await update.message.reply_text(
                        f"⏳ Please wait {wait:.1f} seconds before using this command again."
                    )
                    return
            
            last_called[key] = now
            return await func(update, context, *args, **kwargs)
        return wrapped
    return decorator

class GroupChatManager:
    """Enhanced group chat manager with validation and cleanup"""
    
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
        if not re.match(r'^-?\d+$', group_id):
            logger.warning(f"Invalid group ID format: {group_id}")
            return False
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
        # Initialize components
        self.bot_token = CONFIG.telegram.bot_token
        self.superadmin_ids = {str(id) for id in CONFIG.telegram.superadmin_ids}
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
        
        # Batch notification configuration
        self._record_buffers: Dict[str, List[Dict]] = {}
        self._buffer_timeout = 5  # seconds to wait before flushing buffer
        
        # Data processing components
        self.table_generator = SupabaseTableGenerator()
        self.data_fetcher = DataFetcher()
        
        # Setup handlers
        self._setup_handlers()
        
        # Start background tasks
        self._start_background_tasks()
        
        logger.info("NPAMonitorBot initialized successfully with Supabase")
    
    def _start_background_tasks(self):
        """Start necessary background tasks"""
        asyncio.create_task(self._schedule_main_execution())
        self.start_monitoring()

    async def _schedule_main_execution(self):
        """Schedule main.py execution with enhanced error handling"""
        try:
            await asyncio.sleep(5)  # Initial delay
            while True:
                try:
                    success = await asyncio.wait_for(main(), timeout=300)
                    if success:
                        logger.info("main.py executed successfully")
                    else:
                        logger.error("main.py execution failed")
                        await self._notify_superadmins("⚠️ Failed to execute main.py")
                    await asyncio.sleep(self.monitoring_interval)
                except asyncio.TimeoutError:
                    logger.error("main.py execution timed out")
                    await self._notify_superadmins("⚠️ main.py execution timed out")
                except Exception as e:
                    logger.error(f"Error in main execution: {str(e)}")
                    await self._notify_superadmins(f"⚠️ Error in main execution: {str(e)}")
                    await asyncio.sleep(60)
        except Exception as e:
            logger.error(f"Failed to schedule main execution: {str(e)}")
            await self._notify_superadmins(f"🚨 Failed to schedule main execution: {str(e)}")
    
    def _setup_handlers(self):
        """Setup telegram command handlers with rate limiting"""
        self.application.add_handler(CommandHandler("start", self.start_command))
        self.application.add_handler(CommandHandler("help", self.help_command))
        self.application.add_handler(CommandHandler("status", self.status_command))
        self.application.add_handler(CommandHandler("subscribe", rate_limit()(self.subscribe_command)))
        self.application.add_handler(CommandHandler("unsubscribe", rate_limit()(self.unsubscribe_command)))
        self.application.add_handler(CommandHandler("check", rate_limit()(self.manual_check_command)))
        self.application.add_handler(CommandHandler("recent", rate_limit()(self.recent_records_command)))
        self.application.add_handler(CommandHandler("stats", rate_limit()(self.stats_command)))
        self.application.add_handler(CommandHandler("groups", rate_limit()(self.list_groups_command)))
        self.application.add_handler(CommandHandler("download_pdf", rate_limit(30)(self.download_pdf_command)))
        self.application.add_handler(ChatMemberHandler(self.track_chat_members, ChatMemberHandler.MY_CHAT_MEMBER))
    
    async def _notify_superadmins(self, message: str):
        """Notify superadmins of critical errors with retry logic"""
        for admin_id in self.superadmin_ids:
            try:
                await self.bot.send_message(
                    chat_id=int(admin_id),
                    text=message,
                    parse_mode=ParseMode.MARKDOWN
                )
            except Exception as e:
                logger.error(f"Failed to notify superadmin {admin_id}: {e}")
    
    async def track_chat_members(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Track when bot is added/removed from groups"""
        try:
            chat_member = update.my_chat_member
            chat = update.effective_chat
            user = update.effective_user
            if not chat_member or not chat_member.new_chat_member:
                logger.warning("Invalid chat_member update received")
                return
            if chat.type in [ChatType.GROUP, ChatType.SUPERGROUP]:
                if chat_member.new_chat_member.status == ChatMember.MEMBER:
                    logger.info(f"Bot added to group: {chat.title} (ID: {chat.id}) by user {user.id}")
                    await self.bot.send_message(
                        chat_id=chat.id,
                        text=f"🤖️ Bot added to {chat.title}! Use /subscribe to enable notifications (admin only).",
                        parse_mode=ParseMode.MARKDOWN
                    )
                elif chat_member.new_chat_member.status in [ChatMember.LEFT, ChatMember.KICKED]:
                    logger.info(f"Bot removed from group: {chat.title} (ID: {chat.id}) by user {user.id}")
                    self.group_manager.unsubscribe_group(str(chat.id))
                    await self._notify_superadmins(
                        f"🚪 Bot removed from group: {chat.title} (ID: {chat.id}) by user {user.id}"
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
        try:
            db_status = "Connected" if await self.table_generator.connect_to_database() else "Failed"
        except Exception as e:
            logger.error(f"Database connection test failed: {e}")
            db_status = "Error"
        
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
        try:
            if not await self.table_generator.connect_to_database():
                await update.message.reply_text("❌ Cannot subscribe: Supabase database connection failed.")
                return
        except Exception as e:
            logger.error(f"Database connection failed during subscription: {e}")
            await update.message.reply_text("❌ Cannot subscribe: Database connection error.")
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
    
    async def list_groups_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle /groups command for superadmins"""
        user = update.effective_user
        if not self._is_superadmin(user.id):
            await update.message.reply_text("❌ This command is only available to superadmins.")
            return
        subscribed_groups = self.group_manager.get_subscribed_groups()
        if not subscribed_groups:
            await update.message.reply_text("ℹ️ No groups are currently subscribed.")
            return
        message = f"👥 **Subscribed Groups ({len(subscribed_groups)}):**\n\n"
        for group_id in subscribed_groups:
            try:
                chat = await self.bot.get_chat(int(group_id))
                message += f"• {chat.title} (ID: {group_id})\n"
            except Exception as e:
                logger.warning(f"Failed to get chat info for group {group_id}: {e}")
                message += f"• Unknown Group (ID: {group_id})\n"
        await update.message.reply_text(message, parse_mode=ParseMode.MARKDOWN)
        logger.info(f"Groups command used by superadmin {user.id}")
    
    def _split_message(self, message: str, max_length: int = 4000) -> List[str]:
        """Split a message into parts while preserving markdown integrity"""
        messages = []
        current_message = ""
        lines = message.split("\n")
        for line in lines:
            if len(current_message) + len(line) + 1 > max_length:
                if current_message.strip():
                    messages.append(current_message.strip())
                current_message = line + "\n"
            else:
                current_message += line + "\n"
        if current_message.strip():
            messages.append(current_message.strip())
        return messages
    
    async def manual_check_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle /check command to search for a specific BRV number"""
        if not context.args:
            await update.message.reply_text("❌ Please provide a BRV number (e.g., /check AS496820)")
            return
        brv_number = context.args[0].strip()
        await update.message.reply_text(f"🔍 Searching for BRV number: `{brv_number}`...")
        try:
            found_records = await self.table_generator.search_brv_number(brv_number)
            if not found_records:
                await update.message.reply_text(f"❌ No records found for BRV number: {brv_number}")
                return
            message = f"✅ **Found {len(found_records)} record(s) for BRV number: {brv_number}**\n\n"
            for idx, record in enumerate(found_records, start=1):
                row = record['data']
                table_name = record['table'].replace('_', ' ').title()
                order_date = row.get('order_date', 'N/A')
                if order_date != 'N/A':
                    try:
                        order_date = pd.to_datetime(order_date).strftime('%d-%m-%Y')
                    except:
                        pass
                message += (
                    f"**Record {idx} (Status: {table_name})**\n"
                    f"📅 Date: {order_date}\n"
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
            logger.info(f"Manual check for BRV {brv_number} completed with {len(found_records)} records")
        except Exception as e:
            logger.error(f"Manual check failed: {e}")
            await update.message.reply_text(f"❌ Check failed: {str(e)}")
    
    async def recent_records_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle /recent command"""
        try:
            recent_df = await self.table_generator.get_new_records('depot_manager')
            if recent_df.empty:
                await update.message.reply_text("📭 No recent records found.")
                return
            recent_df = recent_df.head(10)
            message = "📋 **Recent Depot Manager Records (Last 10):**\n\n"
            for idx, (_, row) in enumerate(recent_df.iterrows(), 1):
                order_date = row.get('order_date', 'N/A')
                if order_date != 'N/A':
                    try:
                        order_date = pd.to_datetime(order_date).strftime('%d-%m-%Y')
                    except:
                        pass
                message += (
                    f"**Record {idx}:**\n"
                    f"📅 Date: {order_date}\n"
                    f"🔢 Order: {row.get('order_number', 'N/A')}\n"
                    f"🛢️ Product: {row.get('products', 'N/A')}\n"
                    f"📊 Volume: {row.get('volume', 'N/A')}\n"
                    f"💰 Price: {row.get('ex_ref_price', 'N/A')}\n"
                    f"📋 BRV: {row.get('brv_number', 'N/A')}\n"
                    f"🏢 BDC: {row.get('bdc', 'N/A')}\n"
                    f"🕒 Detected: {pd.to_datetime(row.get('created_at', 'N/A')).strftime('%d-%m-%Y %H:%M:%S') if row.get('created_at') != 'N/A' else 'N/A'}\n"
                    "─────────────────\n"
                )
            messages = self._split_message(message)
            for msg in messages:
                await update.message.reply_text(msg, parse_mode=ParseMode.MARKDOWN)
        except Exception as e:
            logger.error(f"Failed to get recent records: {e}")
            await update.message.reply_text(f"❌ Failed to retrieve recent records: {str(e)}")
    
    async def stats_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle /stats command"""
        try:
            stats = await self.table_generator.get_table_stats()
            message = "📊 **Table Statistics**\n\n"
            for table_name, count in stats.items():
                display_name = table_name.replace('_', ' ').title()
                message += f"• **{display_name}**: {count} records\n"
            message += f"\n👥 **Subscribed Groups**: {len(self.group_manager.get_subscribed_groups())}"
            messages = self._split_message(message)
            for msg in messages:
                await update.message.reply_text(msg, parse_mode=ParseMode.MARKDOWN)
        except Exception as e:
            logger.error(f"Failed to get table stats: {e}")
            await update.message.reply_text(f"❌ Failed to retrieve statistics: {str(e)}")
    
    async def download_pdf_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle /download_pdf command"""
        chat = update.effective_chat
        if chat.type not in [ChatType.GROUP, ChatType.SUPERGROUP]:
            await update.message.reply_text("❌ This command is only available in group chats.")
            return
        if not self.group_manager.is_subscribed(str(chat.id)):
            await update.message.reply_text("❌ This group is not subscribed. Use /subscribe to enable notifications.")
            return
        if not FPDF:
            await update.message.reply_text("❌ PDF generation is not available.")
            return
        try:
            await update.message.reply_text("📄 Generating PDF report...")
            df, error = await self.data_fetcher.fetch_data()
            if error:
                await update.message.reply_text(f"❌ Failed to fetch data: {error}")
                return
            processed_df, error = await self.data_fetcher.process_data(df)
            if error:
                await update.message.reply_text(f"❌ Failed to process data: {error}")
                return
            if processed_df.empty:
                await update.message.reply_text("📭 No processed data found to generate PDF.")
                return
            pdf_generator = PDFGenerator()
            title = f"BOST-KUMASI - {datetime.now().strftime('%d-%m-%Y %H:%M:%S')}"
            footnote = "Data sourced from NPA Enterprise API. Processed by I.T.S (Persol System Limited). Modified by Awuah. Powered by Supabase."
            pdf_data, error = await pdf_generator.generate(processed_df, title, footnote)
            if error:
                await update.message.reply_text(f"❌ Failed to generate PDF: {error}")
                return
            with BytesIO(pdf_data) as pdf_file:
                pdf_file.name = f"BOST-KUMASI_{datetime.now().strftime('%Y%m%d_%H%M%S')}.pdf"
                await update.message.reply_document(
                    document=pdf_file,
                    filename=pdf_file.name,
                    caption="📄 Latest Processed Records"
                )
            logger.info(f"PDF report sent to chat {chat.id}")
        except Exception as e:
            logger.error(f"Failed to generate/send PDF: {e}")
            await update.message.reply_text(f"❌ Failed to generate PDF: {str(e)}")
    
    async def _notify_subscribed_groups(self, table_name: str, new_records: pd.DataFrame):
        """Send notifications to subscribed groups with rate limiting"""
        subscribed_groups = self.group_manager.get_subscribed_groups()
        if not subscribed_groups:
            logger.info("No subscribed groups to notify")
            return
        notification_message = self._format_notification_message(table_name, new_records)
        messages = self._split_message(notification_message)
        for group_id in subscribed_groups:
            try:
                for msg in messages:
                    await self.bot.send_message(
                        chat_id=int(group_id),
                        text=msg,
                        parse_mode=ParseMode.MARKDOWN
                    )
                    await asyncio.sleep(0.1)  # Rate limiting
                logger.info(f"Notification sent to group {group_id} for {len(new_records)} records in {table_name}")
                self.last_notification_count = len(new_records)
            except Exception as e:
                logger.error(f"Failed to send notification to group {group_id}: {e}")
                error_str = str(e).lower()
                if any(phrase in error_str for phrase in ["chat not found", "bot was blocked", "kicked", "not found"]):
                    self.group_manager.unsubscribe_group(group_id)
                    logger.info(f"Auto-unsubscribed group {group_id} due to access error")
                    await self._notify_superadmins(
                        f"🚫 Auto-unsubscribed group {group_id} due to error: {str(e)}"
                    )
    
    def _format_notification_message(self, table_name: str, records: pd.DataFrame) -> str:
        """Format new records into a detailed, table-specific message"""
        if records.empty:
            return f"📭 No new records to notify for {table_name.replace('_', ' ').title()}."
        count = len(records)
        table_display = table_name.replace('_', ' ').title()
        message = (
            f"🚨 **New {table_display} Records Detected!**\n\n"
            f"📊 **Total New Records:** {count}\n"
            f"🕒 **Detection Time:** {datetime.now().strftime('%d-%m-%Y %H:%M:%S')}\n\n"
        )
        for idx, (_, row) in enumerate(records.head(5).iterrows(), 1):
            order_date = row.get('order_date', 'N/A')
            if order_date != 'N/A':
                try:
                    order_date = pd.to_datetime(order_date).strftime('%d-%m-%Y %H:%M')
                except:
                    pass
            message += (
                f"**Record {idx} ({table_display})**\n"
                f"📅 **Order Date:** {order_date}\n"
                f"🔢 **Order Number:** {row.get('order_number', 'N/A')}\n"
                f"🛢️ **Product:** {row.get('products', 'N/A')}\n"
                f"📊 **Volume:** {row.get('volume', 'N/A')}\n"
                f"💰 **Ex Ref Price:** {row.get('ex_ref_price', 'N/A')}\n"
                f"📋 **BRV Number:** {row.get('brv_number', 'N/A')}\n"
                f"🏢 **BDC:** {row.get('bdc', 'N/A')}\n"
                f"🕖 **Created At:** {pd.to_datetime(row.get('created_at', 'N/A')).strftime('%d-%m-%Y %H:%M:%S') if row.get('created_at') != 'N/A' else 'N/A'}\n"
                "─────────────────\n"
            )
        if count > 5:
            message += f"... and {count - 5} more records.\n\n"
        message += f"Use `/recent` to see recent {table_display} records.\nUse `/download_pdf` to get a detailed report."
        return message
    
    async def _on_new_record(self, table_name: str, record: Dict):
        """Callback for new record notifications via real-time subscription"""
        try:
            if table_name not in self._record_buffers:
                self._record_buffers[table_name] = []
            self._record_buffers[table_name].append(record)
            logger.info(f"Added record to buffer for {table_name}. Buffer size: {len(self._record_buffers[table_name])}")
            if not hasattr(self, f'_buffer_task_{table_name}'):
                setattr(self, f'_buffer_task_{table_name}', asyncio.create_task(self._flush_buffer(table_name)))
        except Exception as e:
            logger.error(f"Failed to process real-time record for {table_name}: {str(e)}")
            await self._notify_superadmins(f"⚠️ Error in real-time record for {table_name}: {str(e)}")
    
    async def _flush_buffer(self, table_name: str):
        """Flush buffered records and send a single notification"""
        try:
            await asyncio.sleep(self._buffer_timeout)
            if table_name in self._record_buffers and self._record_buffers[table_name]:
                new_records = pd.DataFrame(self._record_buffers[table_name])
                await self._notify_subscribed_groups(table_name, new_records)
                logger.info(f"Notified groups about {len(self._record_buffers[table_name])} buffered records for {table_name}")
                self._record_buffers[table_name] = []
            if hasattr(self, f'_buffer_task_{table_name}'):
                delattr(self, f'_buffer_task_{table_name}')
        except Exception as e:
            logger.error(f"Failed to flush buffer for {table_name}: {str(e)}")
            await self._notify_superadmins(f"⚠️ Error flushing buffer for {table_name}: {str(e)}")
    
    def start_monitoring(self):
        """Start real-time subscriptions for all tables"""
        if self.monitoring_active:
            logger.info("Monitoring is already active")
            return
        self.monitoring_active = True
        self._record_buffers = {table: [] for table in self.table_generator.table_names}
        for table in self.table_generator.table_names:
            callback = partial(self._on_new_record, table)
            asyncio.create_task(self.table_generator.handler.realtime.subscribe(table, callback))
        logger.info("Real-time monitoring started for all tables")
    
    def stop_monitoring(self):
        """Stop monitoring and clear buffers"""
        self.monitoring_active = False
        for table in self._record_buffers:
            if hasattr(self, f'_buffer_task_{table}'):
                task = getattr(self, f'_buffer_task_{table}')
                if not task.done():
                    task.cancel()
        self._record_buffers = {}
        logger.info("Monitoring and buffer tasks stopped")
    
    async def run(self):
        """Run the bot with enhanced startup sequence"""
        try:
            if not await self.table_generator.connect_to_database():
                logger.error("Failed to connect to Supabase")
                await self._notify_superadmins("🚨 Critical Error: Failed to connect to Supabase")
                return
            await self.application.initialize()
            await self.application.start()
            if CONFIG.telegram.webhook_url:
                try:
                    await self.application.bot.set_webhook(
                        url=CONFIG.telegram.webhook_url,
                        allowed_updates=Update.ALL_TYPES
                    )
                    logger.info(f"Webhook set to {CONFIG.telegram.webhook_url}")
                except Exception as e:
                    logger.error(f"Failed to set webhook: {e}")
                    await self._notify_superadmins(f"⚠️ Failed to set webhook: {str(e)}")
            if CONFIG.telegram.webhook_url and CONFIG.env == Environment.PRODUCTION:
                await self._run_webhook()
            else:
                await self._run_polling()
        except Exception as e:
            logger.error(f"Error running bot: {e}")
            await self._notify_superadmins(f"⚠️ Error running bot: {str(e)}")
            raise
        finally:
            await self._shutdown()
    
    async def _run_webhook(self):
        """Run in webhook mode"""
        port = CONFIG.telegram.webhook_port
        await self.application.updater.start_webhook(
            listen="0.0.0.0",
            port=port,
            url_path=self.bot_token,
            webhook_url=CONFIG.telegram.webhook_url
        )
        logger.info(f"Bot running with webhook on port {port}")
    
    async def _run_polling(self):
        """Run in polling mode"""
        logger.info("Starting in polling mode")
        await self.application.updater.start_polling()
    
    async def _shutdown(self):
        """Clean shutdown procedure"""
        logger.info("Starting shutdown process")
        self.stop_monitoring()
        try:
            if hasattr(self, 'application'):
                await self.application.updater.stop()
                await self.application.stop()
                await self.application.shutdown()
                await self.table_generator.close()
        except Exception as e:
            logger.error(f"Error during shutdown: {e}")
        logger.info("Shutdown completed")

async def main_bot():
    """Main function to run the bot"""
    try:
        bot = NPAMonitorBot()
        await bot.run()
    except KeyboardInterrupt:
        logger.info("Bot stopped by user")
    except Exception as e:
        logger.error(f"Fatal error in main: {e}")
        try:
            temp_bot = NPAMonitorBot()
            await temp_bot._notify_superadmins(f"🚨 Fatal error in bot: {str(e)}")
        except:
            pass
        raise

if __name__ == "__main__":
    try:
        asyncio.run(main_bot())
    except KeyboardInterrupt:
        logger.info("Bot stopped by user")
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        exit(1)
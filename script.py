import sys
import os
import logging
import re
import asyncio
from datetime import datetime, timedelta, timezone
from PyQt5.QtWidgets import QApplication, QMainWindow, QLabel, QVBoxLayout, QHBoxLayout, QWidget, QComboBox, QPushButton, QCalendarWidget, QMessageBox, QCheckBox
from PyQt5.QtGui import QIcon
from PyQt5.QtCore import QDate, QThread, pyqtSignal
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from telethon import TelegramClient, errors, functions

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Telegram API credentials
api_id = 26384924
api_hash = 'c3be3167493a936140929784852a75f3'

# Google Sheets setup
scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
credentials_path = 'credentials.json'
try:
    creds = ServiceAccountCredentials.from_json_keyfile_name(credentials_path, scope)
    client = gspread.authorize(creds)
except Exception as e:
    logger.error('Failed to set up Google Sheets API client: {}'.format(e))
    raise

# Initialize Telegram client
telegram_client = TelegramClient('session_name', api_id, api_hash)

# Time Zone
my_timezone = timezone(timedelta(hours=+8))

# Function to fetch and store messages within a date range
async def fetch_and_store_messages(spreadsheet_url, drive_folder_url, group_name, group_id, start_date, end_date, progress_callback):
    await telegram_client.start()
    logger.info('Fetching messages for group: {}'.format(group_name))

    messages_to_append = []
    total_fetched = 0

    try:
        # Resolve the chat entity
        chat = await telegram_client.get_entity(group_id)
        logger.info('Resolved chat entity: {}'.format(chat.id))

        if not start_date:
            # If no start_date is provided, fetch messages from the beginning
            start_date = datetime.min.replace(tzinfo=my_timezone)
        else:
            # Convert string dates to timezone-aware datetime objects
            start_date = datetime.strptime(start_date, '%Y-%m-%d').replace(tzinfo=my_timezone)
        
        end_date = datetime.strptime(end_date, '%Y-%m-%d').replace(tzinfo=my_timezone) + timedelta(days=1)  # Include end_date in range

        # Open the spreadsheet
        try:
            spreadsheet = client.open_by_url(spreadsheet_url)
        except gspread.exceptions.SpreadsheetNotFound:
            logger.error('Spreadsheet not found at {}. Creating a new one in the specified Google Drive folder.'.format(spreadsheet_url))
            drive_folder_id = re.search(r'folders/([a-zA-Z0-9-_]+)', drive_folder_url).group(1)
            spreadsheet = client.create(group_name, folder_id=drive_folder_id)
            spreadsheet.share('', perm_type='anyone', role='writer')

        # Get or create the worksheet for the group
        try:
            sheet = spreadsheet.worksheet(group_name)
            logger.info('Using existing sheet for group: {}'.format(group_name))
        except gspread.exceptions.WorksheetNotFound:
            logger.info('Creating new sheet for group: {}'.format(group_name))
            sheet = spreadsheet.add_worksheet(title=group_name, rows="1000", cols="20")
            sheet.append_row(["Date", "User ID", "Username", "Message"])

        # Fetch messages from the group or channel within the date range
        async for message in telegram_client.iter_messages(chat, offset_date=end_date, limit=None):
            if start_date <= message.date < end_date:
                user_id = message.sender_id
                date = message.date.astimezone(my_timezone).strftime('%Y-%m-%d %H:%M:%S')  # Format date as string with time zone
                if user_id:
                    try:
                        user = await telegram_client.get_entity(user_id)
                        username = user.username if user.username else "N/A"
                    except errors.FloodWaitError as e:
                        logger.info(f'Sleeping for {e.seconds}s due to FloodWaitError')
                        await asyncio.sleep(e.seconds)
                        user = await telegram_client.get_entity(user_id)
                        username = user.username if user.username else "N/A"
                else:
                    username = "N/A"
                content = message.message
                # Collect message data to append later
                messages_to_append.append([date, user_id, username, content])
                total_fetched += 1

                # If we have collected 1000 messages, append them to the sheet and clear the list
                if total_fetched % 1000 == 0:
                    sheet.append_rows(messages_to_append)
                    messages_to_append = []
                    progress_callback.emit(f'Fetched {total_fetched} messages. Waiting for 5 seconds...')
                    await asyncio.sleep(5)

        # Append any remaining messages
        if messages_to_append:
            sheet.append_rows(messages_to_append)

        progress_callback.emit('All messages saved to Google Sheets for group: {}!'.format(group_name))
        logger.info('All messages saved to Google Sheets for group: {}!'.format(group_name))

    except errors.ChatAdminRequiredError:
        logger.error("Bot needs to be an admin in the group/channel to fetch messages.")
    except errors.FloodWaitError as e:
        logger.info(f'Sleeping for {e.seconds}s due to FloodWaitError')
        await asyncio.sleep(e.seconds)
        await fetch_and_store_messages(spreadsheet_url, drive_folder_url, group_name, group_id, start_date, end_date, progress_callback)
    except Exception as e:
        logger.error('An error occurred while fetching messages for group {}: {}'.format(group_name, e))

# Function to load group data from Google Sheet
def load_group_data(sheet_url):
    try:
        sheet = client.open_by_url(sheet_url).sheet1
        data = sheet.get_all_records()
        groups = {
            row['Name']: {
                'id': row['ID'],
                'sheet_url': row['Sheet URL'],
                'drive_folder_url': row['Drive Folder URL'],
            }
            for row in data
        }
        return groups
    except Exception as e:
        logger.error('Failed to load group data from Google Sheet: {}'.format(e))
        raise

# URL of the Google Sheet containing group data
group_data_sheet_url = "https://docs.google.com/spreadsheets/d/1O_qJx2uDsKhLvBvsZbakDlrk-ms1RGX7Jj8oFb1Ca_M/edit#gid=0"

# Load group data
groups = load_group_data(group_data_sheet_url)

class FetchThread(QThread):
    progress = pyqtSignal(str)

    def __init__(self, spreadsheet_url, drive_folder_url, group_name, group_id, start_date, end_date):
        super().__init__()
        self.spreadsheet_url = spreadsheet_url
        self.drive_folder_url = drive_folder_url
        self.group_name = group_name
        self.group_id = group_id
        self.start_date = start_date
        self.end_date = end_date

    async def run_async(self):
        await fetch_and_store_messages(self.spreadsheet_url, self.drive_folder_url, self.group_name, self.group_id, self.start_date, self.end_date, self.progress)

    def run(self):
        asyncio.run(self.run_async())

class DateRangeSelector(QMainWindow):
    def __init__(self):
        super().__init__()
        
        self.setWindowTitle("Data Collector")
        self.setGeometry(100, 100, 400, 300)
        
        # Get the path to the script's directory
        script_dir = os.path.dirname(os.path.abspath(__file__))
        icon_path = os.path.join(script_dir, 'logo.ico')
        self.setWindowIcon(QIcon(icon_path))  # Set the icon for the window
        
        self.selected_group = None
        self.group_id = None
        self.start_date = None
        self.end_date = None

        self.initUI()

    def initUI(self):
        layout = QVBoxLayout()

        group_layout = QHBoxLayout()
        group_label = QLabel("Select Group:")
        self.group_combo = QComboBox()
        self.group_combo.addItems(groups.keys())
        group_layout.addWidget(group_label)
        group_layout.addWidget(self.group_combo)
        
        self.all_messages_checkbox = QCheckBox("Fetch all messages")
        self.all_messages_checkbox.stateChanged.connect(self.toggle_date_selection)

        self.today_checkbox = QCheckBox("Fetch Today's Messages")
        self.today_checkbox.stateChanged.connect(self.toggle_today_selection)

        start_date_layout = QHBoxLayout()
        start_date_label = QLabel("Start Date:")
        self.start_date_calendar = QCalendarWidget()
        start_date_layout.addWidget(start_date_label)
        start_date_layout.addWidget(self.start_date_calendar)
        
        end_date_layout = QHBoxLayout()
        end_date_label = QLabel("End Date:")
        self.end_date_calendar = QCalendarWidget()
        end_date_layout.addWidget(end_date_label)
        end_date_layout.addWidget(self.end_date_calendar)
        
        self.submit_button = QPushButton("Submit")
        self.submit_button.clicked.connect(self.submit)

        layout.addLayout(group_layout)
        layout.addWidget(self.all_messages_checkbox)
        layout.addWidget(self.today_checkbox)
        layout.addLayout(start_date_layout)
        layout.addLayout(end_date_layout)
        layout.addWidget(self.submit_button)
        
        container = QWidget()
        container.setLayout(layout)
        self.setCentralWidget(container)

    def toggle_date_selection(self):
        if self.all_messages_checkbox.isChecked():
            self.start_date_calendar.setEnabled(False)
            self.end_date_calendar.setEnabled(False)
            self.today_checkbox.setEnabled(False)
        else:
            self.start_date_calendar.setEnabled(True)
            self.end_date_calendar.setEnabled(True)
            self.today_checkbox.setEnabled(True)

    def toggle_today_selection(self):
        if self.today_checkbox.isChecked():
            self.start_date_calendar.setEnabled(False)
            self.end_date_calendar.setEnabled(False)
            self.all_messages_checkbox.setEnabled(False)
        else:
            self.start_date_calendar.setEnabled(True)
            self.end_date_calendar.setEnabled(True)
            self.all_messages_checkbox.setEnabled(True)

    def submit(self):
        self.selected_group = self.group_combo.currentText()
        self.group_id = groups[self.selected_group]['id']

        if self.all_messages_checkbox.isChecked():
            self.start_date = None
            self.end_date = datetime.now().strftime('%Y-%m-%d')
        elif self.today_checkbox.isChecked():
            today = datetime.now().strftime('%Y-%m-%d')
            self.start_date = today
            self.end_date = today
        else:
            self.start_date = self.start_date_calendar.selectedDate().toString('yyyy-MM-dd')
            self.end_date = self.end_date_calendar.selectedDate().toString('yyyy-MM-dd')
        
        # Show confirmation dialog
        msg = QMessageBox()
        msg.setIcon(QMessageBox.Information)
        msg.setText("Group: {}\nStart Date: {}\nEnd Date: {}".format(self.selected_group, self.start_date, self.end_date))
        msg.setWindowTitle("Confirmation")
        msg.setStandardButtons(QMessageBox.Ok | QMessageBox.Cancel)
        retval = msg.exec_()
        
        if retval == QMessageBox.Ok:
            self.run_fetch_and_store()

    def run_fetch_and_store(self):
        spreadsheet_url = groups[self.selected_group]['sheet_url']
        drive_folder_url = groups[self.selected_group]['drive_folder_url']
        
        self.fetch_thread = FetchThread(spreadsheet_url, drive_folder_url, self.selected_group, self.group_id, self.start_date, self.end_date)
        self.fetch_thread.progress.connect(self.update_status)
        self.fetch_thread.start()

    def update_status(self, status):
        QMessageBox.information(self, "Status", status)

def main():
    app = QApplication(sys.argv)
    window = DateRangeSelector()
    window.show()
    sys.exit(app.exec_())

if __name__ == '__main__':
    main()

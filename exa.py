from datetime import datetime
import pytz

IST = pytz.timezone('Asia/Kolkata')


datetime_ist = datetime.now(IST)
print("Date : ",
      datetime_ist.strftime('%Y-%m-%d'))
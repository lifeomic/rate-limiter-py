#!/usr/bin/env python
import random
import string
from datetime import datetime

def random_string(length=8):
    return ''.join(random.choice(string.lowercase) for i in range(length))

def now_utc_sec():
    return int(datetime.utcnow().strftime('%s'))

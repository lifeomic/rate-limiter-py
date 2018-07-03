#!/usr/bin/env python
import random
import string

def random_string(length=8):
    return ''.join(random.choice(string.lowercase) for i in range(length))

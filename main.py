import os
import subprocess
import collections
import csv
import asyncio

from email_validator import validate_email
from functools import lru_cache


LOOCKUP_DIR = 'loockup'

emails = []

def parsers_run():
    tasks = []

    for filename in os.listdir(os.getcwd() + '/' + LOOCKUP_DIR):
        tasks.append(parse_user(filename))
    
    loop = asyncio.get_event_loop()
    results = loop.run_until_complete(asyncio.gather(*tasks, return_exceptions=True))
    
    loop.close()
    
    for result in results:
        emails.extend(result)

async def parse_user(filename):
    with open(LOOCKUP_DIR + '/' + filename, 'r') as f:
        reader = csv.reader(f)
        
        return [field[1] for field in reader if field[1] != 'email']

@lru_cache(maxsize=None)
def email_duplicates():
    return [(count, item) for item, count in collections.Counter(emails).items() if count > 1]

def email_validator(email):
    try:
        validate_email(email)
        res = True
    except:
        return 'Unvalid'

    return 'Valid'

def grep_duplicates():
    for count, email in email_duplicates():
        print(f'<<<<<< We found {count} duplicates of {email} in these files:>>>>>>>\n')
        
        subprocess.call(['grep', '-r', email, './' + LOOCKUP_DIR])
        
        print("\n")
        print(f"Validation {email}: {email_validator(email)}")
        print("\n\n")



if __name__ == '__main__':
    parsers_run()
    grep_duplicates()
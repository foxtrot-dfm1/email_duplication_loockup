import os
import collections
import csv
import asyncio

from email_validator import validate_email


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

def email_duplicates():
    return [(count, item) for item, count in collections.Counter(emails).items() if count > 1]

def email_validator(email, counter):
    try:
        validate_email(email)
    except:
        counter += 1
        return 'Unvalid'

    return 'Valid'

def grep_duplicates():
    duplicates = email_duplicates()
    unvalid_counter = 0

    print(f"\nEmail duplicates total: {len(duplicates)}\n")

    for count, email in duplicates:
        print(f'{email}, entries: {count}, validation: {email_validator(email, unvalid_counter)}')

    print(f"\nEmail unvalid total: {unvalid_counter}\n")

    print("Pretty copy/paste:\n")
    for _, email in duplicates:
        print(f'{email}')

if __name__ == '__main__':
    parsers_run()
    grep_duplicates()
